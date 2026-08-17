#!/usr/bin/env python3
import argparse
import os
import random
import shutil
import signal
import socket
import struct
import subprocess
import threading
import time
from pathlib import Path

SERVER_PORT = 5001
NET_PKG_QUERY = 0
NET_PKG_REC_HEADER = 1
NET_PKG_REC_BODY = 2
NET_PKG_REC_END = 3
NET_PKG_ERROR = 4
NET_PKG_OK = 5
NET_PKG_RAW_STRING = 6

STRESS_ROWS = 1000
STRESS_CLIENTS = 8
STRESS_ROWS_PER_CLIENT = 100
RESTART_PRESSURE_ROUNDS = 3
CRASH_HELD_CLIENTS = []


def pack_query(sql):
    payload = sql.encode()
    return struct.pack("=IQ", NET_PKG_QUERY, len(payload)) + payload


def recv_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("server closed the connection")
        data.extend(chunk)
    return bytes(data)


def recv_pkg(sock, timeout=None):
    old_timeout = sock.gettimeout()
    sock.settimeout(timeout)
    try:
        header = recv_exact(sock, 12)
        pkg_type, length = struct.unpack("=IQ", header)
        payload = recv_exact(sock, length) if length else b""
        return pkg_type, payload.decode(errors="replace")
    finally:
        sock.settimeout(old_timeout)


class SqlClient:
    def __init__(self, name):
        self.name = name
        self.sock = socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=5)
        self.sock.settimeout(None)

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def execute_async(self, sql):
        self.sock.sendall(pack_query(sql))

    def fetch_response(self, timeout=5):
        rows = []
        raw = []
        while True:
            pkg_type, payload = recv_pkg(self.sock, timeout)
            if pkg_type == NET_PKG_OK:
                return {"ok": True, "rows": rows, "raw": raw}
            if pkg_type == NET_PKG_ERROR:
                return {"ok": False, "error": payload, "rows": rows, "raw": raw}
            if pkg_type in (NET_PKG_RAW_STRING, NET_PKG_REC_HEADER):
                raw.append(payload)
            elif pkg_type == NET_PKG_REC_BODY:
                for record in payload.split("\0"):
                    if record:
                        rows.append([field for field in record.split("\t") if field != ""])
            elif pkg_type == NET_PKG_REC_END:
                return {"ok": True, "rows": rows, "raw": raw}
            else:
                raise RuntimeError(f"{self.name}: unexpected package type {pkg_type}")

    def execute(self, sql, timeout=5):
        self.execute_async(sql)
        return self.fetch_response(timeout)

    def assert_ok(self, sql, timeout=5):
        result = self.execute(sql, timeout)
        if not result["ok"]:
            raise AssertionError(f"{self.name}: expected OK for {sql!r}, got {result}")
        return result


def assert_rows(result, expected, label):
    if result["rows"] != expected:
        raise AssertionError(f"{label}: expected rows {expected}, got {result['rows']}")


def assert_failed(result, label):
    if result["ok"]:
        raise AssertionError(f"{label}: expected failure, got {result}")


def wait_until_ready():
    deadline = time.time() + 8
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("njudb did not start")


def port_is_open():
    try:
        with socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=0.2):
            return True
    except OSError:
        return False


def shutdown_existing_server():
    if not port_is_open():
        return
    try:
        client = SqlClient("preflight-shutdown")
        try:
            client.execute("shutdown;", timeout=2)
        finally:
            client.close()
    except Exception:
        pass

    deadline = time.time() + 3
    while time.time() < deadline:
        if not port_is_open():
            return
        time.sleep(0.1)
    raise RuntimeError(f"port {SERVER_PORT} is still in use before Lab06 recovery test")


def launch_server(bin_dir, log_name, env_extra=None):
    log_path = Path(__file__).resolve().parent / "output" / log_name
    log_path.parent.mkdir(parents=True, exist_ok=True)
    server_log = open(log_path, "w")
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)
    proc = subprocess.Popen(
        [str(bin_dir / "njudb")],
        cwd=bin_dir,
        stdout=server_log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
        env=env,
    )
    proc._njudb_server_log = server_log
    return proc


def start_server(bin_dir, log_name, env_extra=None):
    proc = launch_server(bin_dir, log_name, env_extra)
    try:
        wait_until_ready()
        return proc
    except Exception:
        stop_server(proc, sig=signal.SIGKILL)
        raise


def start_server_expect_crash(bin_dir, log_name, env_extra):
    proc = launch_server(bin_dir, log_name, env_extra)
    deadline = time.time() + 10
    try:
        while time.time() < deadline:
            if proc.poll() is not None:
                return
            if port_is_open():
                raise AssertionError(f"{log_name}: server reached ready state instead of crashing")
            time.sleep(0.05)
        raise AssertionError(f"{log_name}: server did not crash before timeout")
    finally:
        stop_server(proc, sig=signal.SIGKILL)


def stop_server(proc, sig=signal.SIGTERM):
    server_log = getattr(proc, "_njudb_server_log", None)
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), sig)
            proc.wait(timeout=3)
        except Exception:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=3)
    if server_log is not None:
        server_log.close()


def crash_server(proc):
    stop_server(proc, sig=signal.SIGKILL)


def shutdown_server(proc):
    client = SqlClient("shutdown")
    try:
        try:
            client.execute("shutdown;", timeout=2)
        except RuntimeError as exc:
            if "server closed" not in str(exc):
                raise
    finally:
        client.close()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        stop_server(proc, sig=signal.SIGTERM)
    finally:
        server_log = getattr(proc, "_njudb_server_log", None)
        if server_log is not None:
            server_log.close()


def open_client(name, database):
    client = SqlClient(name)
    client.assert_ok(f"open database {database};")
    return client


def close_all(clients):
    for client in clients:
        client.close()


def hold_until_crash(client):
    CRASH_HELD_CLIENTS.append(client)


def close_crash_held_clients():
    close_all(CRASH_HELD_CLIENTS)
    CRASH_HELD_CLIENTS.clear()


def setup_clean_data(bin_dir):
    data_dir = bin_dir / "data"
    data_dir.mkdir(exist_ok=True)
    for entry in data_dir.iterdir():
        if entry.name == ".tmp":
            continue
        if entry.is_dir():
            shutil.rmtree(entry)
        else:
            entry.unlink()
    log_path = data_dir / "njudb.log"
    if log_path.exists():
        log_path.unlink()


def create_database(protocol, db_name):
    client = SqlClient(f"create-{db_name}")
    try:
        client.assert_ok(f"set concurrency_protocol = {protocol};")
        client.assert_ok(f"create database {db_name};")
    finally:
        client.close()


def create_accounts(db_name):
    client = open_client(f"schema-{db_name}", db_name)
    try:
        client.assert_ok("create table account (id int, balance int, tag char(8));")
        client.assert_ok("create index account_id_idx on account(id);")
        client.assert_ok("create index account_tag_idx on account(tag);")
        client.assert_ok("begin;")
        for row_id, balance, tag in [
            (1, 100, "a01"),
            (2, 200, "a02"),
            (3, 300, "a03"),
            (4, 400, "a04"),
            (5, 500, "a05"),
        ]:
            client.assert_ok(f"insert into account values ({row_id}, {balance}, '{tag}');")
        client.assert_ok("commit;")
    finally:
        client.close()


def create_stress_table(db_name):
    client = open_client(f"stress-schema-{db_name}", db_name)
    try:
        client.assert_ok("create table stress_account (id int, balance int, owner char(8));")
        client.assert_ok("create index stress_account_id_idx on stress_account(id);")
        client.assert_ok("create index stress_account_owner_idx on stress_account(owner);")
        client.assert_ok("begin;")
        for row_id in range(1, STRESS_ROWS + 1):
            client.assert_ok(
                f"insert into stress_account values ({row_id}, {row_id * 10}, 's{row_id:07d}');",
                timeout=10,
            )
        client.assert_ok("commit;", timeout=20)
    finally:
        client.close()


def checkpoint():
    client = SqlClient("checkpoint")
    try:
        result = client.assert_ok("create static_checkpoint;", timeout=10)
        if not any("Checkpoint created successfully" in raw for raw in result["raw"]):
            raise AssertionError(f"checkpoint did not report success: {result}")
    finally:
        client.close()


def setup_workload(include_mvcc):
    create_database("2PL", "lab06_2pl")
    create_accounts("lab06_2pl")
    if include_mvcc:
        create_database("MVCC", "lab06_mvcc")
        create_accounts("lab06_mvcc")
    create_stress_table("lab06_2pl")
    checkpoint()


def run_2pl_crash_workload():
    committed = open_client("2pl-committed", "lab06_2pl")
    loser = open_client("2pl-loser", "lab06_2pl")
    try:
        committed.assert_ok("begin;")
        committed.assert_ok("update account set balance = 111, tag = 'a01c' where id = 1;")
        committed.assert_ok("delete from account where id = 2;")
        committed.assert_ok("insert into account values (10, 1000, 'a10c');")
        committed.assert_ok("commit;", timeout=5)

        loser.assert_ok("begin;")
        loser.assert_ok("update account set balance = 3333, tag = 'a03x' where id = 3;")
        loser.assert_ok("delete from account where id = 4;")
        loser.assert_ok("insert into account values (11, 1100, 'a11x');")
        checkpoint()
    finally:
        committed.close()
        hold_until_crash(loser)


def run_mvcc_crash_workload():
    committed = open_client("mvcc-committed", "lab06_mvcc")
    loser = open_client("mvcc-loser", "lab06_mvcc")
    try:
        committed.assert_ok("begin;")
        committed.assert_ok("update account set balance = 121, tag = 'm01c' where id = 1;")
        committed.assert_ok("delete from account where id = 2;")
        committed.assert_ok("insert into account values (10, 1010, 'm10c');")
        committed.assert_ok("commit;", timeout=5)

        loser.assert_ok("begin;")
        loser.assert_ok("update account set balance = 3333, tag = 'm03x' where id = 3;")
        loser.assert_ok("delete from account where id = 4;")
        loser.assert_ok("insert into account values (11, 1110, 'm11x');")
        checkpoint()
    finally:
        committed.close()
        hold_until_crash(loser)


def stress_worker(db_name, client_id, errors):
    client = open_client(f"stress-{client_id}", db_name)
    try:
        start = 1 + client_id * STRESS_ROWS_PER_CLIENT
        end = start + STRESS_ROWS_PER_CLIENT
        client.assert_ok("begin;")
        for row_id in range(start, end):
            client.assert_ok(
                f"update stress_account set balance = {row_id * 10 + 7} where id = {row_id};",
                timeout=10,
            )
        client.assert_ok("commit;", timeout=20)
    except Exception as exc:
        errors.append(exc)
    finally:
        client.close()


def run_stress_crash_workload():
    errors = []
    threads = [
        threading.Thread(target=stress_worker, args=("lab06_2pl", client_id, errors))
        for client_id in range(STRESS_CLIENTS)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise AssertionError(f"stress workers failed: {errors}")

    loser = open_client("stress-loser", "lab06_2pl")
    try:
        loser.assert_ok("begin;")
        for row_id in range(901, 921):
            loser.assert_ok(f"update stress_account set balance = 999999 where id = {row_id};", timeout=10)
        for row_id in range(1001, 1011):
            loser.assert_ok(
                f"insert into stress_account values ({row_id}, {row_id * 10}, 's{row_id:07d}');",
                timeout=10,
            )
        checkpoint()
    finally:
        hold_until_crash(loser)


def verify_account_readonly_state(db_name, prefix):
    client = open_client(f"verify-{db_name}", db_name)
    try:
        expected_id1 = "111" if db_name == "lab06_2pl" else "121"
        expected_tag1 = "a01c" if db_name == "lab06_2pl" else "m01c"
        expected_tag10 = "a10c" if db_name == "lab06_2pl" else "m10c"
        assert_rows(
            client.assert_ok("select id, balance, tag from account where id = 1;", timeout=10),
            [["1", expected_id1, expected_tag1]],
            f"{prefix} {db_name} committed update by id index",
        )
        assert_rows(
            client.assert_ok(f"select id, balance, tag from account where tag = '{expected_tag10}';", timeout=10),
            [["10", "1000" if db_name == "lab06_2pl" else "1010", expected_tag10]],
            f"{prefix} {db_name} committed insert by tag index",
        )
        assert_rows(
            client.assert_ok("select id, balance, tag from account where id = 2;", timeout=10),
            [],
            f"{prefix} {db_name} committed delete",
        )
        assert_rows(
            client.assert_ok("select id, balance, tag from account where id = 3;", timeout=10),
            [["3", "300", "a03" if db_name == "lab06_2pl" else "a03"]],
            f"{prefix} {db_name} loser update undone",
        )
        assert_rows(
            client.assert_ok("select id, balance, tag from account where id = 4;", timeout=10),
            [["4", "400", "a04" if db_name == "lab06_2pl" else "a04"]],
            f"{prefix} {db_name} loser delete undone",
        )
        assert_rows(
            client.assert_ok("select id, balance, tag from account where id = 11;", timeout=10),
            [],
            f"{prefix} {db_name} loser insert undone",
        )
    finally:
        client.close()


def verify_account_unique_constraints(db_name, prefix):
    dup_client = open_client(f"verify-dup-{db_name}", db_name)
    try:
        assert_failed(
            dup_client.execute("insert into account values (10, 2222, 'dup10');", timeout=10),
            f"{prefix} {db_name} unique index still rejects duplicate",
        )
    finally:
        dup_client.close()


def verify_stress_readonly_state(prefix):
    client = open_client(f"verify-stress-{prefix}", "lab06_2pl")
    try:
        assert_rows(
            client.assert_ok("select count(*) from stress_account;", timeout=20),
            [[str(STRESS_ROWS)]],
            f"{prefix} stress row count",
        )
        for row_id in (1, 100, 101, 500, 800):
            expected = row_id * 10 + 7
            assert_rows(
                client.assert_ok(f"select id, balance from stress_account where id = {row_id};", timeout=10),
                [[str(row_id), str(expected)]],
                f"{prefix} committed stress row {row_id}",
            )
        for row_id in (901, 910, 920):
            assert_rows(
                client.assert_ok(f"select id, balance from stress_account where id = {row_id};", timeout=10),
                [[str(row_id), str(row_id * 10)]],
                f"{prefix} loser stress update undone {row_id}",
            )
        assert_rows(
            client.assert_ok("select id, balance from stress_account where id = 1001;", timeout=10),
            [],
            f"{prefix} loser stress insert undone",
        )
    finally:
        client.close()


def verify_stress_unique_constraints(prefix):
    dup_client = open_client(f"verify-stress-dup-{prefix}", "lab06_2pl")
    try:
        assert_failed(
            dup_client.execute("insert into stress_account values (800, 8000, 'dup00800');", timeout=10),
            f"{prefix} stress unique index rejects duplicate",
        )
    finally:
        dup_client.close()


def verify_key_reuse_after_recovery(include_mvcc):
    database_names = ["lab06_2pl"]
    if include_mvcc:
        database_names.append("lab06_mvcc")
    for db_name in database_names:
        client = open_client(f"reuse-{db_name}", db_name)
        try:
            client.assert_ok("insert into account values (11, 1111, 'retry11');", timeout=10)
            assert_rows(
                client.assert_ok("select id, balance, tag from account where id = 11;", timeout=10),
                [["11", "1111", "retry11"]],
                f"{db_name} can reuse loser insert key after recovery",
            )
        finally:
            client.close()

    client = open_client("reuse-stress", "lab06_2pl")
    try:
        client.assert_ok("insert into stress_account values (1001, 10010, 's0001001');", timeout=10)
        assert_rows(
            client.assert_ok("select id, balance from stress_account where id = 1001;", timeout=10),
            [["1001", "10010"]],
            "stress table can reuse loser insert key after recovery",
        )
    finally:
        client.close()


def verify_readonly_state(prefix, include_mvcc):
    verify_account_readonly_state("lab06_2pl", prefix)
    if include_mvcc:
        verify_account_readonly_state("lab06_mvcc", prefix)
    verify_stress_readonly_state(prefix)


def verify_negative_and_reuse(prefix, include_mvcc):
    verify_account_unique_constraints("lab06_2pl", prefix)
    if include_mvcc:
        verify_account_unique_constraints("lab06_mvcc", prefix)
    verify_stress_unique_constraints(prefix)
    verify_key_reuse_after_recovery(include_mvcc)


def verify_restart_after_recovery_checkpoint(bin_dir, include_mvcc):
    proc = start_server(bin_dir, "lab06_recovery_restart_after_checkpoint.log")
    verify_readonly_state("post-recovery-checkpoint", include_mvcc)
    shutdown_server(proc)
    print("Passed: restart after recovery checkpoint preserved table/index state")


def verify_recovery_phase_crash_windows(bin_dir):
    crashpoints = ["after_redo", "after_undo_record", "after_undo"]
    for point in crashpoints:
        start_server_expect_crash(
            bin_dir,
            f"lab06_recovery_phase_{point}.log",
            {"NJUDB_TEST_RECOVERY_CRASHPOINT": point},
        )
        print(f"Passed: recovery crashpoint {point} was retryable")


def verify_index_rebuild_crash_windows(bin_dir, include_mvcc):
    crashpoints = [
        "after_marker",
        "after_close_indexes",
        "after_temp_index",
        "after_rename",
        "after_open_indexes",
    ]
    for point in crashpoints:
        start_server_expect_crash(
            bin_dir,
            f"lab06_recovery_index_rebuild_{point}.log",
            {"NJUDB_TEST_INDEX_REBUILD_CRASHPOINT": point},
        )
        proc = start_server(bin_dir, f"lab06_recovery_index_rebuild_{point}_retry.log")
        verify_readonly_state(f"index-rebuild-{point}", include_mvcc)
        shutdown_server(proc)
        print(f"Passed: index rebuild crashpoint {point} was retryable")


def verify_restart_pressure_cycles(bin_dir, include_mvcc):
    rng = random.Random(20260619)
    for round_id in range(RESTART_PRESSURE_ROUNDS):
        proc = start_server(bin_dir, f"lab06_recovery_pressure_{round_id}.log")
        verify_readonly_state(f"pressure-{round_id}", include_mvcc)
        if rng.choice([True, False]):
            checkpoint()
        crash_server(proc)

        proc = start_server(bin_dir, f"lab06_recovery_pressure_{round_id}_retry.log")
        verify_readonly_state(f"pressure-{round_id}-retry", include_mvcc)
        shutdown_server(proc)
    print(f"Passed: {RESTART_PRESSURE_ROUNDS} deterministic crash/restart pressure rounds")


def run_recovery_scenario(bin_dir, include_mvcc):
    proc = start_server(bin_dir, "lab06_recovery_initial.log")
    setup_workload(include_mvcc)
    run_stress_crash_workload()
    run_2pl_crash_workload()
    if include_mvcc:
        run_mvcc_crash_workload()
    crash_server(proc)
    close_crash_held_clients()
    protocol_label = "2PL/MVCC" if include_mvcc else "2PL"
    print(f"Passed: killed server with committed and loser {protocol_label} transactions")

    verify_recovery_phase_crash_windows(bin_dir)

    proc = start_server(bin_dir, "lab06_recovery_restart_1.log")
    crash_server(proc)
    print("Passed: second crash immediately after recovery")

    proc = start_server(bin_dir, "lab06_recovery_restart_2.log")
    verify_readonly_state("restart-2", include_mvcc)
    checkpoint()
    shutdown_server(proc)
    print("Passed: nested restart remained idempotent")

    verify_restart_after_recovery_checkpoint(bin_dir, include_mvcc)
    verify_index_rebuild_crash_windows(bin_dir, include_mvcc)
    verify_restart_pressure_cycles(bin_dir, include_mvcc)

    proc = start_server(bin_dir, "lab06_recovery_final_reuse.log")
    verify_negative_and_reuse("final", include_mvcc)
    shutdown_server(proc)
    print("Passed: recovered indexes allowed key reuse after crash-window retries")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", required=True, type=Path)
    parser.add_argument("--sql-root", required=True, type=Path)
    parser.add_argument(
        "--include-mvcc",
        action="store_true",
        help="also score the MVCC bonus workload during every recovery phase",
    )
    args = parser.parse_args()

    shutdown_existing_server()
    setup_clean_data(args.bin_dir)
    run_recovery_scenario(args.bin_dir, args.include_mvcc)
    mode = "2PL/MVCC bonus" if args.include_mvcc else "2PL required"
    print(f"Passed [15/15] lab06 {mode} crash recovery SQL tests")


if __name__ == "__main__":
    main()
