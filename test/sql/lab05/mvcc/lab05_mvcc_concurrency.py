#!/usr/bin/env python3
import argparse
import os
import random
import shutil
import signal
import socket
import struct
import subprocess
import time
from pathlib import Path

SERVER_PORT = 5001
STRESS_ROWS = 1000
STRESS_CLIENTS = 8
# NetController uses a listen backlog of 10. Keep two slots free for setup,
# verification, and transient reconnects rather than treating that backlog as
# a safe active-client capacity.
MAX_SAFE_STRESS_CLIENTS = 8
NET_PKG_QUERY = 0
NET_PKG_REC_HEADER = 1
NET_PKG_REC_BODY = 2
NET_PKG_REC_END = 3
NET_PKG_ERROR = 4
NET_PKG_OK = 5
NET_PKG_RAW_STRING = 6


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
            if pkg_type == NET_PKG_RAW_STRING or pkg_type == NET_PKG_REC_HEADER:
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


def close_clients(clients):
    for client in clients:
        client.close()


def abort_if_needed(client):
    try:
        client.execute("abort;", timeout=2)
    except Exception:
        pass


def start_server(bin_dir):
    data_dir = bin_dir / "data"
    for name in ("lab05_mvcc", "lab05_cross_2pl"):
        target = data_dir / name
        if target.exists():
            shutil.rmtree(target)
    log_file = data_dir / "njudb.log"
    if log_file.exists():
        log_file.unlink()
    log_path = Path(__file__).resolve().parent / "output" / "lab05_mvcc_server.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    server_log = open(log_path, "w")
    proc = subprocess.Popen(
        [str(bin_dir / "njudb")],
        cwd=bin_dir,
        stdout=server_log,
        stderr=subprocess.STDOUT,
        preexec_fn=os.setsid,
    )
    proc._njudb_server_log = server_log
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", SERVER_PORT), timeout=0.2):
                return proc
        except OSError:
            time.sleep(0.05)
    stop_server(proc)
    raise RuntimeError("njudb did not start")


def stop_server(proc):
    server_log = getattr(proc, "_njudb_server_log", None)
    if proc.poll() is None:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=2)
        except Exception:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            proc.wait(timeout=2)
    if server_log is not None:
        server_log.close()


def run_file_client(bin_dir, sql_file):
    subprocess.run([str(bin_dir / "client"), "-i", str(sql_file)], cwd=bin_dir, check=True)


def open_client(name, database="lab05_mvcc"):
    client = SqlClient(name)
    client.assert_ok(f"open database {database};")
    return client


def setup_database(bin_dir, sql_root):
    run_file_client(bin_dir, sql_root / "mvcc" / "init.sql")
    run_file_client(bin_dir, sql_root / "mvcc" / "01_prepare.sql")
    run_file_client(bin_dir, sql_root / "mvcc" / "02_commit_abort.sql")
    run_file_client(bin_dir, sql_root / "mvcc" / "03_index_abort_consistency.sql")
    run_file_client(bin_dir, sql_root / "mvcc" / "04_repeated_writes.sql")

    client = open_client("stress-setup")
    try:
        client.assert_ok("create table account_stress (id int, balance int, owner char(8));")
        client.assert_ok("create index account_stress_id_idx on account_stress(id);")
        client.assert_ok("create index account_stress_owner_idx on account_stress(owner);")
        client.assert_ok("begin;")
        for row_id in range(1, STRESS_ROWS + 1):
            client.assert_ok(
                f"insert into account_stress values ({row_id}, {row_id * 10}, 'r{row_id:07d}');",
                timeout=10,
            )
        client.assert_ok("commit;", timeout=15)
    finally:
        client.close()


def test_uncommitted_update_reader_gets_old_value_without_blocking():
    writer = open_client("update-writer")
    reader = open_client("update-reader")
    try:
        writer.assert_ok("begin;")
        writer.assert_ok("update account set balance = 1001 where id = 1;")
        started = time.monotonic()
        assert_rows(reader.assert_ok("select id, balance from account where id = 1;", timeout=2),
                    [["1", "152"]], "reader sees old value")
        if time.monotonic() - started > 1:
            raise AssertionError("MVCC reader unexpectedly blocked behind writer")
        writer.assert_ok("abort;")
    finally:
        writer.close()
        reader.close()


def test_writer_reads_own_update():
    writer = open_client("own-write")
    try:
        writer.assert_ok("begin;")
        writer.assert_ok("update account set balance = 1002 where id = 1;")
        assert_rows(writer.assert_ok("select id, balance from account where id = 1;"),
                    [["1", "1002"]], "writer reads own update")
        writer.assert_ok("abort;")
    finally:
        writer.close()


def test_old_snapshot_stable_after_update_commit():
    old_reader = open_client("old-snapshot")
    writer = open_client("snapshot-writer")
    new_reader = None
    try:
        old_reader.assert_ok("begin;")
        assert_rows(old_reader.assert_ok("select id, balance from account where id = 1;"),
                    [["1", "152"]], "old snapshot initial")
        writer.assert_ok("begin;")
        writer.assert_ok("update account set balance = 1003 where id = 1;")
        writer.assert_ok("commit;")
        assert_rows(old_reader.assert_ok("select id, balance from account where id = 1;"),
                    [["1", "152"]], "old snapshot remains stable")
        new_reader = open_client("new-snapshot")
        assert_rows(new_reader.assert_ok("select id, balance from account where id = 1;"),
                    [["1", "1003"]], "new snapshot sees commit")
        old_reader.assert_ok("commit;")
    finally:
        old_reader.close()
        writer.close()
        if new_reader is not None:
            new_reader.close()


def test_uncommitted_insert_visibility_then_commit():
    old_reader = open_client("insert-old")
    writer = open_client("insert-writer")
    new_reader = None
    row_id = 1101
    try:
        old_reader.assert_ok("begin;")
        writer.assert_ok("begin;")
        writer.assert_ok(f"insert into account_stress values ({row_id}, 11010, 'n{row_id:07d}');")
        assert_rows(old_reader.assert_ok(f"select id, balance from account_stress where id = {row_id};"),
                    [], "uncommitted insert invisible")
        writer.assert_ok("commit;")
        assert_rows(old_reader.assert_ok(f"select id, balance from account_stress where id = {row_id};"),
                    [], "old snapshot does not gain committed insert")
        new_reader = open_client("insert-new")
        assert_rows(new_reader.assert_ok(f"select id, balance from account_stress where id = {row_id};"),
                    [[str(row_id), "11010"]], "new snapshot sees inserted row")
        old_reader.assert_ok("commit;")
    finally:
        old_reader.close()
        writer.close()
        if new_reader is not None:
            new_reader.close()


def test_delete_visibility_for_old_and_new_snapshots():
    old_reader = open_client("delete-old")
    writer = open_client("delete-writer")
    new_reader = None
    try:
        old_reader.assert_ok("begin;")
        writer.assert_ok("begin;")
        writer.assert_ok("delete from account_stress where id = 40;")
        assert_rows(old_reader.assert_ok("select id, balance from account_stress where id = 40;"),
                    [["40", "400"]], "old reader sees row during delete")
        writer.assert_ok("commit;")
        assert_rows(old_reader.assert_ok("select id, balance from account_stress where id = 40;"),
                    [["40", "400"]], "old reader sees deleted old version")
        new_reader = open_client("delete-new")
        assert_rows(new_reader.assert_ok("select id, balance from account_stress where id = 40;"),
                    [], "new reader does not see deleted row")
        old_reader.assert_ok("commit;")
    finally:
        old_reader.close()
        writer.close()
        if new_reader is not None:
            new_reader.close()


def test_same_rid_write_conflict_has_one_winner():
    first = open_client("conflict-first")
    second = open_client("conflict-second")
    check = None
    try:
        first.assert_ok("begin;")
        first.assert_ok("update account_stress set balance = 5100 where id = 51;")
        second.assert_ok("begin;")
        assert_failed(second.execute("update account_stress set balance = 5200 where id = 51;", timeout=2),
                      "second writer conflicts immediately")
        abort_if_needed(second)
        first.assert_ok("commit;")
        check = open_client("conflict-check")
        assert_rows(check.assert_ok("select id, balance from account_stress where id = 51;"),
                    [["51", "5100"]], "only winner is visible")
    finally:
        first.close()
        second.close()
        if check is not None:
            check.close()


def test_failed_conflict_does_not_change_indexes():
    first = open_client("index-conflict-first")
    second = open_client("index-conflict-second")
    check = None
    try:
        first.assert_ok("begin;")
        first.assert_ok("update account_stress set owner = 'winner52' where id = 52;")
        second.assert_ok("begin;")
        assert_failed(second.execute("update account_stress set owner = 'loser052' where id = 52;"),
                      "losing indexed update conflicts")
        abort_if_needed(second)
        first.assert_ok("commit;")
        check = open_client("index-conflict-check")
        assert_rows(check.assert_ok("select id, owner from account_stress where owner = 'winner52';"),
                    [["52", "winner52"]], "winner index exists")
        assert_rows(check.assert_ok("select id, owner from account_stress where owner = 'loser052';"),
                    [], "loser index absent")
    finally:
        first.close()
        second.close()
        if check is not None:
            check.close()


def test_repeated_update_commits_last_value():
    client = open_client("repeat-commit")
    check = None
    try:
        client.assert_ok("begin;")
        client.assert_ok("update account_stress set balance = 5301 where id = 53;")
        client.assert_ok("update account_stress set balance = 5302 where id = 53;")
        client.assert_ok("commit;")
        check = open_client("repeat-commit-check")
        assert_rows(check.assert_ok("select id, balance from account_stress where id = 53;"),
                    [["53", "5302"]], "repeated update commits final value")
    finally:
        client.close()
        if check is not None:
            check.close()


def test_repeated_update_delete_abort_restores_original():
    client = open_client("repeat-abort")
    check = None
    try:
        client.assert_ok("begin;")
        client.assert_ok("update account_stress set balance = 5499, owner = 'chg00054' where id = 54;")
        client.assert_ok("delete from account_stress where id = 54;")
        client.assert_ok("abort;")
        check = open_client("repeat-abort-check")
        assert_rows(check.assert_ok("select id, balance, owner from account_stress where id = 54;"),
                    [["54", "540", "r0000054"]], "abort restores original tuple")
        assert_rows(check.assert_ok("select id from account_stress where owner = 'chg00054';"),
                    [], "abort removes changed index key")
    finally:
        client.close()
        if check is not None:
            check.close()


def test_eight_client_disjoint_batch_updates():
    clients = [open_client(f"batch-{i}") for i in range(STRESS_CLIENTS)]
    check = None
    try:
        for client in clients:
            client.assert_ok("begin;")
        for round_no in range(20):
            for client_id, client in enumerate(clients):
                row_id = 101 + client_id * 20 + round_no
                client.execute_async(f"update account_stress set balance = {100000 + row_id} where id = {row_id};")
            for client in clients:
                if not client.fetch_response(timeout=5)["ok"]:
                    raise AssertionError("disjoint MVCC update failed")
        for client in clients:
            client.assert_ok("commit;")
        check = open_client("batch-check")
        assert_rows(check.assert_ok("select count(*) from account_stress;"),
                    [[str(STRESS_ROWS)]], "batch row count")
        assert_rows(check.assert_ok("select id, balance from account_stress where id = 101;"),
                    [["101", "100101"]], "batch sample")
    finally:
        close_clients(clients)
        if check is not None:
            check.close()


def test_five_writer_hotspot_rejects_losers():
    clients = [open_client(f"hotspot-{i}") for i in range(5)]
    check = None
    try:
        for client in clients:
            client.assert_ok("begin;")
        clients[0].assert_ok("update account_stress set balance = 6000 where id = 60;")
        for index, client in enumerate(clients[1:], start=1):
            assert_failed(client.execute(f"update account_stress set balance = {6000 + index} where id = 60;"),
                          f"hotspot loser {index}")
            abort_if_needed(client)
        clients[0].assert_ok("commit;")
        check = open_client("hotspot-check")
        assert_rows(check.assert_ok("select id, balance from account_stress where id = 60;"),
                    [["60", "6000"]], "hotspot winner")
    finally:
        close_clients(clients)
        if check is not None:
            check.close()


def test_seven_client_mixed_disjoint_dml():
    clients = [open_client(f"mixed-{i}") for i in range(7)]
    check = None
    try:
        for client in clients:
            client.assert_ok("begin;")
        statements = [
            "insert into account_stress values (1201, 12010, 'm0001201');",
            "insert into account_stress values (1202, 12020, 'm0001202');",
            "insert into account_stress values (1203, 12030, 'm0001203');",
            "update account_stress set balance = 7100 where id = 71;",
            "update account_stress set owner = 'mixed072' where id = 72;",
            "delete from account_stress where id = 73;",
            "delete from account_stress where id = 74;",
        ]
        for client, sql in zip(clients, statements):
            client.execute_async(sql)
        for client in clients:
            if not client.fetch_response(timeout=5)["ok"]:
                raise AssertionError("mixed disjoint DML failed")
        for client in clients:
            client.assert_ok("commit;")
        check = open_client("mixed-check")
        assert_rows(check.assert_ok("select count(*) from account_stress;"),
                    [[str(STRESS_ROWS + 3 - 2)]], "mixed DML row count")
        assert_rows(check.assert_ok("select id from account_stress where owner = 'mixed072';"),
                    [["72"]], "mixed indexed update")
    finally:
        close_clients(clients)
        if check is not None:
            check.close()


def test_unique_key_abort_then_retry():
    holder = open_client("unique-holder")
    retry = open_client("unique-retry")
    check = None
    row_id = 1301
    try:
        holder.assert_ok("begin;")
        holder.assert_ok(f"insert into account_stress values ({row_id}, 13010, 'q0001301');")
        holder.assert_ok("abort;")
        retry.assert_ok("begin;")
        retry.assert_ok(f"insert into account_stress values ({row_id}, 13020, 's0001301');")
        retry.assert_ok("commit;")
        check = open_client("unique-check")
        assert_rows(check.assert_ok(f"select id, balance, owner from account_stress where id = {row_id};"),
                    [[str(row_id), "13020", "s0001301"]], "unique key reusable after abort")
        assert_rows(check.assert_ok("select id from account_stress where owner = 'q0001301';"),
                    [], "aborted unique index key absent")
    finally:
        holder.close()
        retry.close()
        if check is not None:
            check.close()


def test_eight_client_insert_update_delete_workflow():
    clients = [open_client(f"workflow-{i}") for i in range(STRESS_CLIENTS)]
    check = None
    per_client = 10
    base = 2001
    try:
        for client_id, client in enumerate(clients):
            client.assert_ok("begin;")
            for offset in range(per_client):
                row_id = base + client_id * per_client + offset
                client.assert_ok(f"insert into account_stress values ({row_id}, {row_id}, 'i{row_id:07d}');")
            client.assert_ok("commit;")

        for client_id, client in enumerate(clients):
            client.assert_ok("begin;")
            for offset in range(per_client):
                row_id = base + client_id * per_client + offset
                client.assert_ok(
                    f"update account_stress set balance = {200000 + row_id}, owner = 'u{row_id:07d}' where id = {row_id};"
                )
            for offset in range(per_client // 2):
                row_id = base + client_id * per_client + offset
                client.assert_ok(f"delete from account_stress where id = {row_id};")
            client.assert_ok("commit;")

        check = open_client("workflow-check")
        # Earlier scenarios leave one mixed-DML net insert and one committed
        # unique-key retry, then this workflow retains half of its 80 inserts.
        expected = STRESS_ROWS + 2 + STRESS_CLIENTS * (per_client // 2)
        assert_rows(check.assert_ok("select count(*) from account_stress;"),
                    [[str(expected)]], "workflow row count")
        assert_rows(check.assert_ok(f"select id from account_stress where id = {base};"),
                    [], "workflow deleted half absent")
        retained = base + per_client // 2
        assert_rows(check.assert_ok(f"select id, balance, owner from account_stress where id = {retained};"),
                    [[str(retained), str(200000 + retained), f"u{retained:07d}"]], "workflow retained half")
    finally:
        close_clients(clients)
        if check is not None:
            check.close()


def test_mvcc_and_2pl_databases_coexist():
    creator = SqlClient("cross-creator")
    mvcc = None
    two_pl = None
    try:
        creator.assert_ok("set concurrency_protocol = 2PL;")
        creator.assert_ok("create database lab05_cross_2pl;")
        two_pl = open_client("cross-2pl", "lab05_cross_2pl")
        two_pl.assert_ok("create table protocol_probe (id int, value int);")
        two_pl.assert_ok("insert into protocol_probe values (1, 20);")

        mvcc = open_client("cross-mvcc")
        mvcc.assert_ok("insert into account values (9, 900, 'ivy');")
        assert_rows(mvcc.assert_ok("select id, balance from account where id = 9;"),
                    [["9", "900"]], "MVCC database remains usable")
        assert_rows(two_pl.assert_ok("select id, value from protocol_probe where id = 1;"),
                    [["1", "20"]], "2PL database remains usable")
    finally:
        creator.close()
        if mvcc is not None:
            mvcc.close()
        if two_pl is not None:
            two_pl.close()


def run_optional_randomized_stress(client_count, rounds, seed):
    rng = random.Random(seed)
    clients = [open_client(f"optional-stress-{i}") for i in range(client_count)]
    rows_per_client = 16
    base_row = 301
    expected = {}

    for client_id in range(client_count):
        for offset in range(rows_per_client):
            row_id = base_row + client_id * rows_per_client + offset
            expected[row_id] = (row_id * 10, f"r{row_id:07d}")

    try:
        for round_no in range(rounds):
            operations = []
            for client_id, client in enumerate(clients):
                offset = rng.randrange(rows_per_client)
                row_id = base_row + client_id * rows_per_client + offset
                balance = 900000 + round_no * client_count + client_id
                owner = f"p{client_id}{round_no:06d}"
                commit = rng.random() < 0.75

                client.assert_ok("begin;")
                client.execute_async(
                    f"update account_stress set balance = {balance}, owner = '{owner}' where id = {row_id};"
                )
                operations.append((client, row_id, balance, owner, commit))

            for client, _, _, _, _ in operations:
                result = client.fetch_response(timeout=10)
                if not result["ok"]:
                    raise AssertionError(f"optional stress update failed: {result}")

            for client, row_id, balance, owner, commit in operations:
                client.assert_ok("commit;" if commit else "abort;", timeout=10)
                if commit:
                    expected[row_id] = (balance, owner)
    finally:
        close_clients(clients)

    check = open_client("optional-stress-check")
    try:
        for row_id, (balance, owner) in sorted(expected.items()):
            assert_rows(
                check.assert_ok(
                    f"select id, balance, owner from account_stress where id = {row_id};",
                    timeout=10,
                ),
                [[str(row_id), str(balance), owner]],
                f"optional stress row {row_id}",
            )
            assert_rows(
                check.assert_ok(f"select id from account_stress where owner = '{owner}';", timeout=10),
                [[str(row_id)]],
                f"optional stress owner index {owner}",
            )
    finally:
        check.close()


TESTS = [
    ("uncommitted update reader gets old value without blocking", test_uncommitted_update_reader_gets_old_value_without_blocking),
    ("writer reads own update", test_writer_reads_own_update),
    ("old snapshot stable after update commit", test_old_snapshot_stable_after_update_commit),
    ("uncommitted insert visibility then commit", test_uncommitted_insert_visibility_then_commit),
    ("delete visibility for old and new snapshots", test_delete_visibility_for_old_and_new_snapshots),
    ("same RID write conflict has one winner", test_same_rid_write_conflict_has_one_winner),
    ("failed conflict does not change indexes", test_failed_conflict_does_not_change_indexes),
    ("repeated update commits last value", test_repeated_update_commits_last_value),
    ("repeated update/delete abort restores original", test_repeated_update_delete_abort_restores_original),
    ("8-client disjoint batch updates", test_eight_client_disjoint_batch_updates),
    ("5-writer hotspot rejects losers", test_five_writer_hotspot_rejects_losers),
    ("7-client mixed disjoint DML", test_seven_client_mixed_disjoint_dml),
    ("unique key abort then retry", test_unique_key_abort_then_retry),
    ("8-client insert-update-delete workflow", test_eight_client_insert_update_delete_workflow),
    ("MVCC and 2PL databases coexist", test_mvcc_and_2pl_databases_coexist),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", default="build/bin")
    parser.add_argument("--sql-root", default="test/sql/lab05")
    parser.add_argument("--reuse-server", action="store_true")
    parser.add_argument("--stress", action="store_true", help="append the optional randomized MVCC stress test")
    parser.add_argument("--stress-clients", type=int, default=8)
    parser.add_argument("--stress-rounds", type=int, default=100)
    parser.add_argument("--stress-seed", type=int, default=20260612)
    args = parser.parse_args()

    if not 2 <= args.stress_clients <= MAX_SAFE_STRESS_CLIENTS:
        parser.error(f"--stress-clients must be between 2 and {MAX_SAFE_STRESS_CLIENTS}")
    if not 1 <= args.stress_rounds <= 1000:
        parser.error("--stress-rounds must be between 1 and 1000")

    bin_dir = Path(args.bin_dir).resolve()
    sql_root = Path(args.sql_root).resolve()
    proc = None
    if not args.reuse_server:
        proc = start_server(bin_dir)
    try:
        setup_database(bin_dir, sql_root)
        for index, (name, test) in enumerate(TESTS, start=1):
            test()
            print(f"Passed: {name}")
        print(f"Passed [{len(TESTS)}/{len(TESTS)}] lab05 MVCC concurrency tests")
        if args.stress:
            run_optional_randomized_stress(args.stress_clients, args.stress_rounds, args.stress_seed)
            print(
                "Passed: optional randomized MVCC stress "
                f"(clients={args.stress_clients}, rounds={args.stress_rounds}, seed={args.stress_seed})"
            )
    finally:
        if proc is not None:
            stop_server(proc)


if __name__ == "__main__":
    main()
