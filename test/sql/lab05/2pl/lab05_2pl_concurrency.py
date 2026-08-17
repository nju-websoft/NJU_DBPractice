#!/usr/bin/env python3
import argparse
import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
from pathlib import Path

SERVER_PORT = 5001
STRESS_ROWS = 1000
STRESS_CLIENTS = 8
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
            if pkg_type == NET_PKG_RAW_STRING:
                raw.append(payload)
            elif pkg_type == NET_PKG_REC_HEADER:
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
    got = result["rows"]
    if got != expected:
        raise AssertionError(f"{label}: expected rows {expected}, got {got}")


def assert_blocked(client, label, wait=0.35):
    try:
        result = client.fetch_response(timeout=wait)
    except socket.timeout:
        return
    raise AssertionError(f"{label}: expected statement to block, but got {result}")


def close_clients(clients):
    for client in clients:
        client.close()


def rollback_if_needed(client):
    try:
        client.execute("commit;", timeout=2)
    except Exception:
        pass


def start_server(bin_dir):
    data_dir = bin_dir / "data"
    target = data_dir / "lab05_2pl"
    if target.exists():
        shutil.rmtree(target)
    log_file = data_dir / "njudb.log"
    if log_file.exists():
        log_file.unlink()
    log_path = Path(__file__).resolve().parent / "output" / "lab05_2pl_server.log"
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
    if proc.poll() is not None:
        if server_log is not None:
            server_log.close()
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        proc.wait(timeout=2)
    except Exception:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=2)
    finally:
        if server_log is not None:
            server_log.close()


def run_file_client(bin_dir, sql_file, output_file=None):
    cmd = [str(bin_dir / "client"), "-i", str(sql_file)]
    if output_file is not None:
        cmd.extend(["-o", str(output_file)])
    subprocess.run(cmd, cwd=bin_dir, check=True)


def setup_database(bin_dir, sql_root):
    run_file_client(bin_dir, sql_root / "init.sql")
    run_file_client(bin_dir, sql_root / "2pl" / "01_prepare.sql")
    run_file_client(bin_dir, sql_root / "2pl" / "02_commit_abort.sql")
    run_file_client(bin_dir, sql_root / "2pl" / "03_index_abort_consistency.sql")
    setup_stress_table()


def setup_stress_table():
    client = open_client("stress-setup")
    try:
        client.assert_ok("create table account_stress (id int, balance int, owner char(8));", timeout=5)
        client.assert_ok("create index account_stress_id_idx on account_stress(id);", timeout=5)
        client.assert_ok("create index account_stress_owner_idx on account_stress(owner);", timeout=5)
        client.assert_ok("begin;")
        for row_id in range(1, STRESS_ROWS + 1):
            owner = f"r{row_id:07d}"
            client.assert_ok(
                f"insert into account_stress values ({row_id}, {row_id * 10}, '{owner}');",
                timeout=5,
            )
        client.assert_ok("commit;", timeout=10)
        assert_rows(
            client.assert_ok("select count(*) from account_stress;", timeout=10),
            [[str(STRESS_ROWS)]],
            "stress setup row count",
        )
    finally:
        client.close()


def assert_stress_balance(row_id, expected_balance, label):
    check = open_client(f"{label}-check")
    try:
        assert_rows(
            check.assert_ok(f"select id, balance from account_stress where id = {row_id};", timeout=5),
            [[str(row_id), str(expected_balance)]],
            label,
        )
    finally:
        check.close()


def open_client(name):
    client = SqlClient(name)
    client.assert_ok("open database lab05_2pl;")
    return client


def test_read_read_compatible():
    c1 = open_client("rr-c1")
    c2 = open_client("rr-c2")
    try:
        c1.assert_ok("begin;")
        c2.assert_ok("begin;")
        assert_rows(c1.assert_ok("select id, balance from account where id = 1;"), [["1", "150"]], "read/read c1")
        assert_rows(c2.assert_ok("select id, balance from account where id = 1;"), [["1", "150"]], "read/read c2")
        c1.assert_ok("commit;")
        c2.assert_ok("commit;")
    finally:
        c1.close()
        c2.close()


def test_write_blocks_read_until_commit():
    writer = open_client("wr-writer")
    reader = open_client("wr-reader")
    try:
        writer.assert_ok("begin;")
        writer.assert_ok("update account set balance = 151 where id = 1;")
        reader.execute_async("select id, balance from account where id = 1;")
        assert_blocked(reader, "reader behind X lock")
        writer.assert_ok("commit;")
        assert_rows(reader.fetch_response(timeout=3), [["1", "151"]], "reader sees committed value")
    finally:
        writer.close()
        reader.close()


def test_write_write_conflict_serializes():
    c1 = open_client("ww-c1")
    c2 = open_client("ww-c2")
    check = open_client("ww-check")
    try:
        c1.assert_ok("begin;")
        c1.assert_ok("update account set balance = 152 where id = 1;")
        c2.assert_ok("begin;")
        c2.execute_async("update account set balance = 153 where id = 1;")
        assert_blocked(c2, "writer behind X lock")
        c1.assert_ok("commit;")
        if not c2.fetch_response(timeout=3)["ok"]:
            raise AssertionError("second writer should continue after first writer commits")
        c2.assert_ok("commit;")
        assert_rows(check.assert_ok("select id, balance from account where id = 1;"), [["1", "153"]], "write/write final")
    finally:
        c1.close()
        c2.close()
        check.close()


def test_abort_wakes_waiter():
    c1 = open_client("abort-c1")
    c2 = open_client("abort-c2")
    check = open_client("abort-check")
    try:
        c1.assert_ok("begin;")
        c1.assert_ok("update account set balance = 210 where id = 2;")
        c2.assert_ok("begin;")
        c2.execute_async("update account set balance = 220 where id = 2;")
        assert_blocked(c2, "waiter behind aborted txn")
        c1.assert_ok("abort;")
        if not c2.fetch_response(timeout=3)["ok"]:
            raise AssertionError("waiter should continue after holder aborts")
        c2.assert_ok("commit;")
        assert_rows(check.assert_ok("select id, balance from account where id = 2;"), [["2", "220"]], "abort wakes waiter final")
    finally:
        c1.close()
        c2.close()
        check.close()


def test_different_rids_can_update_concurrently():
    c1 = open_client("rid-c1")
    c2 = open_client("rid-c2")
    check = open_client("rid-check")
    try:
        c1.assert_ok("begin;")
        c1.assert_ok("update account set balance = 301 where id = 3;")
        c2.assert_ok("begin;")
        c2.assert_ok("update account set balance = 661 where id = 6;")
        c1.assert_ok("commit;")
        c2.assert_ok("commit;")
        assert_rows(
            check.assert_ok("select id, balance from account where id > 2 order by id;"),
            [["3", "301"], ["4", "400"], ["6", "661"]],
            "different RID final",
        )
    finally:
        c1.close()
        c2.close()
        check.close()


def test_two_transaction_cycle_breaks():
    c1 = open_client("cycle-c1")
    c2 = open_client("cycle-c2")
    check = open_client("cycle-check")
    try:
        c1.assert_ok("begin;")
        c2.assert_ok("begin;")
        c1.assert_ok("update account set balance = 1001 where id = 1;")
        c2.assert_ok("update account set balance = 2002 where id = 2;")

        c1.execute_async("update account set balance = 1002 where id = 2;")
        assert_blocked(c1, "first edge in cycle")

        second = c2.execute("update account set balance = 2001 where id = 1;", timeout=3)
        if second["ok"]:
            first = c1.fetch_response(timeout=3)
        else:
            first = c1.fetch_response(timeout=3)

        c1_end = c1.execute("commit;", timeout=3)
        c2_end = c2.execute("commit;", timeout=3)
        if c1_end["ok"] and c2_end["ok"]:
            raise AssertionError("cycle should not allow both transactions to commit")

        rows = check.assert_ok("select id, balance from account where id < 3 order by id;")["rows"]
        allowed = [
            [["1", "1001"], ["2", "1002"]],
            [["1", "2001"], ["2", "2002"]],
            [["1", "153"], ["2", "220"]],
        ]
        if rows not in allowed:
            raise AssertionError(f"cycle final state should be one complete winner or original rows, got {rows}; first={first}, second={second}")
    finally:
        c1.close()
        c2.close()
        check.close()


def test_eight_client_disjoint_rid_batch_updates():
    clients = [open_client(f"batch-c{i}") for i in range(STRESS_CLIENTS)]
    check = open_client("batch-check")
    try:
        for client in clients:
            client.assert_ok("begin;")

        for round_no in range(20):
            for client_id, client in enumerate(clients):
                row_id = 101 + client_id * 20 + round_no
                new_balance = 100000 + client_id * 1000 + round_no
                client.execute_async(
                    f"update account_stress set balance = {new_balance} where id = {row_id};"
                )
            for client in clients:
                result = client.fetch_response(timeout=5)
                if not result["ok"]:
                    raise AssertionError(f"disjoint RID update failed: {result}")

        for client in clients:
            client.assert_ok("commit;", timeout=5)

        assert_rows(
            check.assert_ok("select count(*) from account_stress;", timeout=10),
            [[str(STRESS_ROWS)]],
            "disjoint RID row count",
        )
        for client_id in range(STRESS_CLIENTS):
            row_id = 101 + client_id * 20
            expected_balance = 100000 + client_id * 1000
            assert_rows(
                check.assert_ok(f"select id, balance from account_stress where id = {row_id};", timeout=5),
                [[str(row_id), str(expected_balance)]],
                f"disjoint RID sample {client_id}",
            )
    finally:
        close_clients(clients)
        check.close()


def test_one_writer_blocks_seven_readers():
    writer = open_client("fanout-writer")
    readers = [open_client(f"fanout-reader-{i}") for i in range(7)]
    try:
        writer.assert_ok("begin;")
        writer.assert_ok("update account_stress set balance = 88888 where id = 20;")
        for reader in readers:
            reader.execute_async("select id, balance from account_stress where id = 20;")
        for reader in readers:
            assert_blocked(reader, "reader fanout behind X lock", wait=0.25)
        writer.assert_ok("commit;")
        for reader in readers:
            assert_rows(reader.fetch_response(timeout=5), [["20", "88888"]], "fanout reader sees committed value")
    finally:
        writer.close()
        close_clients(readers)


def test_five_writer_hotspot_queue_drains():
    clients = [open_client(f"hotspot-c{i}") for i in range(5)]
    check = open_client("hotspot-check")
    pending = {}
    try:
        for client in clients:
            client.assert_ok("begin;")
        clients[0].assert_ok("update account_stress set balance = 91000 where id = 30;")
        for index, client in enumerate(clients[1:], start=1):
            client.execute_async(f"update account_stress set balance = {91000 + index} where id = 30;")
            pending[client] = 91000 + index
        for client in pending:
            assert_blocked(client, "hotspot writer queue", wait=0.2)

        clients[0].assert_ok("commit;")
        committed_values = {91000}
        deadline = time.time() + 10
        while pending and time.time() < deadline:
            progressed = False
            for client, value in list(pending.items()):
                try:
                    result = client.fetch_response(timeout=0.2)
                except socket.timeout:
                    continue
                if not result["ok"]:
                    raise AssertionError(f"hotspot queued writer failed: {result}")
                client.assert_ok("commit;", timeout=5)
                committed_values.add(value)
                pending.pop(client)
                progressed = True
            if not progressed:
                time.sleep(0.05)
        if pending:
            raise AssertionError("hotspot writer queue did not drain")

        final_rows = check.assert_ok("select id, balance from account_stress where id = 30;", timeout=5)["rows"]
        if len(final_rows) != 1 or final_rows[0][1] not in {str(v) for v in committed_values}:
            raise AssertionError(f"hotspot final value should be one committed writer value, got {final_rows}")
    finally:
        for client in pending:
            rollback_if_needed(client)
        close_clients(clients)
        check.close()


def test_four_transaction_wait_graph_breaks():
    clients = [open_client(f"graph-c{i}") for i in range(4)]
    check = open_client("graph-check")
    pending = {}
    committed = 0
    failed = 0
    try:
        for client in clients:
            client.assert_ok("begin;")

        clients[0].assert_ok("update account_stress set balance = 41001 where id = 41;")
        clients[1].assert_ok("update account_stress set balance = 42002 where id = 42;")
        clients[2].assert_ok("update account_stress set balance = 43003 where id = 43;")
        clients[3].assert_ok("update account_stress set balance = 44004 where id = 44;")

        waits = [
            (clients[0], 42, 41002),
            (clients[1], 43, 42003),
            (clients[2], 44, 43004),
        ]
        for client, row_id, value in waits:
            client.execute_async(f"update account_stress set balance = {value} where id = {row_id};")
            pending[client] = value
        for client in pending:
            assert_blocked(client, "multi-edge wait graph", wait=0.2)

        last = clients[3].execute("update account_stress set balance = 44001 where id = 41;", timeout=5)
        if last["ok"]:
            clients[3].assert_ok("commit;", timeout=5)
            committed += 1
        else:
            failed += 1
            rollback_if_needed(clients[3])

        deadline = time.time() + 10
        while pending and time.time() < deadline:
            progressed = False
            for client in list(pending):
                try:
                    result = client.fetch_response(timeout=0.2)
                except socket.timeout:
                    continue
                pending.pop(client)
                progressed = True
                if result["ok"]:
                    end = client.execute("commit;", timeout=5)
                    if end["ok"]:
                        committed += 1
                    else:
                        failed += 1
                else:
                    failed += 1
                    rollback_if_needed(client)
            if not progressed:
                time.sleep(0.05)

        if pending:
            raise AssertionError("multi-transaction wait graph did not resolve")
        if failed == 0:
            raise AssertionError("multi-transaction wait graph should abort at least one transaction")
        if committed == 0:
            raise AssertionError("multi-transaction wait graph should allow at least one transaction to commit")

        assert_rows(
            check.assert_ok("select count(*) from account_stress;", timeout=10),
            [[str(STRESS_ROWS)]],
            "wait graph row count",
        )
    finally:
        for client in pending:
            rollback_if_needed(client)
        close_clients(clients)
        check.close()


def test_read_committed_reader_releases_before_commit():
    reader = open_client("rw-reader")
    writer = open_client("rw-writer")
    check = open_client("rw-check")
    try:
        reader.assert_ok("begin;")
        assert_rows(
            reader.assert_ok("select id, balance from account_stress where id = 70;"),
            [["70", "700"]],
            "reader holds S lock on row 70",
        )

        writer.assert_ok("begin;")
        writer.assert_ok("update account_stress set balance = 7070 where id = 70;", timeout=5)
        writer.assert_ok("commit;", timeout=5)

        assert_rows(
            reader.assert_ok("select id, balance from account_stress where id = 70;"),
            [["70", "7070"]],
            "READ COMMITTED reader sees later committed value",
        )
        reader.assert_ok("commit;")
        assert_rows(
            check.assert_ok("select id, balance from account_stress where id = 70;"),
            [["70", "7070"]],
            "reader/writer final value",
        )
    finally:
        reader.close()
        writer.close()
        check.close()


def test_multi_row_business_transaction_serializes():
    first = open_client("transfer-first")
    second = open_client("transfer-second")
    check = open_client("transfer-check")
    try:
        first.assert_ok("begin;")
        first.assert_ok("update account_stress set balance = 5000 where id = 501;")
        first.assert_ok("update account_stress set balance = 5030 where id = 502;")

        second.assert_ok("begin;")
        second.execute_async("update account_stress set balance = 4990 where id = 501;")
        assert_blocked(second, "second transfer waits on first row")

        first.assert_ok("commit;", timeout=5)
        if not second.fetch_response(timeout=5)["ok"]:
            raise AssertionError("second transfer should continue after first transfer commits")
        second.assert_ok("update account_stress set balance = 5040 where id = 502;", timeout=5)
        second.assert_ok("commit;", timeout=5)

        assert_rows(
            check.assert_ok("select id, balance from account_stress where id >= 501 and id <= 502 order by id;"),
            [["501", "4990"], ["502", "5040"]],
            "multi-row transaction final state",
        )
        assert_rows(
            check.assert_ok("select sum(balance) from account_stress where id >= 501 and id <= 502;"),
            [["10030"]],
            "multi-row transaction invariant",
        )
    finally:
        first.close()
        second.close()
        check.close()


def test_seven_client_mixed_dml_same_table():
    clients = [open_client(f"mixed-c{i}") for i in range(STRESS_CLIENTS)]
    check = open_client("mixed-check")
    rounds = 10

    try:
        for offset in range(rounds):
            for client in clients[:7]:
                client.assert_ok("begin;")

            statements = []
            for client_id in range(3):
                row_id = 3001 + client_id * rounds + offset
                statements.append(
                    (clients[client_id], f"insert into account_stress values ({row_id}, {700000 + row_id}, 'm{row_id:07d}');")
                )
            for client_id in range(3, 5):
                row_id = 901 + (client_id - 3) * rounds + offset
                statements.append(
                    (clients[client_id], f"update account_stress set balance = {710000 + row_id}, owner = 'v{row_id:07d}' where id = {row_id};")
                )
            for client_id in range(5, 7):
                row_id = 951 + (client_id - 5) * rounds + offset
                statements.append((clients[client_id], f"delete from account_stress where id = {row_id};"))
            for client, sql in statements:
                client.execute_async(sql)
            pending = {client: sql for client, sql in statements}
            deadline = time.time() + 30
            while pending and time.time() < deadline:
                progressed = False
                for client, sql in list(pending.items()):
                    try:
                        result = client.fetch_response(timeout=0.2)
                    except socket.timeout:
                        continue
                    if not result["ok"]:
                        raise AssertionError(f"mixed DML failed for {sql!r}: {result}")
                    client.assert_ok("commit;", timeout=10)
                    pending.pop(client)
                    progressed = True
                if not progressed:
                    time.sleep(0.05)
            if pending:
                blocked = ", ".join(f"{client.name}: {sql!r}" for client, sql in pending.items())
                raise AssertionError(f"mixed DML did not finish: {blocked}")

        inserted_id = 3001
        updated_id = 901
        deleted_id = 951
        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where id = {inserted_id};"),
            [[str(inserted_id), str(700000 + inserted_id), f"m{inserted_id:07d}"]],
            "mixed DML inserted row",
        )
        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where id = {updated_id};"),
            [[str(updated_id), str(710000 + updated_id), f"v{updated_id:07d}"]],
            "mixed DML updated row",
        )
        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where id = {deleted_id};"),
            [],
            "mixed DML deleted row absent by id",
        )
        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where owner = 'r{deleted_id:07d}';"),
            [],
            "mixed DML deleted row absent by owner index",
        )
    finally:
        close_clients(clients)
        check.close()


def test_unique_index_conflict_abort_then_retry():
    holder = open_client("uniq-holder")
    conflict = open_client("uniq-conflict")
    retry = open_client("uniq-retry")
    check = open_client("uniq-check")
    row_id = 3901
    try:
        holder.assert_ok("begin;")
        holder.assert_ok(f"insert into account_stress values ({row_id}, 390100, 'q{row_id:07d}');")

        conflict.assert_ok("begin;")
        duplicate = conflict.execute(f"insert into account_stress values ({row_id}, 390101, 'd{row_id:07d}');", timeout=5)
        if duplicate["ok"]:
            raise AssertionError("duplicate unique index insert should fail while conflicting key exists")
        rollback_if_needed(conflict)

        holder.assert_ok("abort;", timeout=5)
        retry.assert_ok("begin;")
        retry.assert_ok(f"insert into account_stress values ({row_id}, 390102, 's{row_id:07d}');", timeout=5)
        retry.assert_ok("commit;", timeout=5)

        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where id = {row_id};"),
            [[str(row_id), "390102", f"s{row_id:07d}"]],
            "unique conflict retry by id",
        )
        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where owner = 'q{row_id:07d}';"),
            [],
            "aborted unique conflict old owner absent",
        )
    finally:
        holder.close()
        conflict.close()
        retry.close()
        check.close()


def test_eight_client_insert_update_delete_workflow():
    clients = [open_client(f"iud-c{i}") for i in range(STRESS_CLIENTS)]
    check = open_client("iud-check")
    inserted_per_client = 20
    deleted_per_client = 10

    def row_id_for(client_id, offset):
        return 2001 + client_id * inserted_per_client + offset

    try:
        for client_id, client in enumerate(clients):
            client.assert_ok("begin;")
            for offset in range(inserted_per_client):
                row_id = row_id_for(client_id, offset)
                client.assert_ok(
                    f"insert into account_stress values ({row_id}, {500000 + row_id}, 'i{row_id:07d}');",
                    timeout=15,
                )
            client.assert_ok("commit;", timeout=5)

        for client in clients:
            client.assert_ok("begin;")
        for offset in range(inserted_per_client):
            for client_id, client in enumerate(clients):
                row_id = row_id_for(client_id, offset)
                client.execute_async(
                    f"update account_stress set balance = {600000 + row_id}, owner = 'u{row_id:07d}' "
                    f"where id = {row_id};"
                )
            for client in clients:
                result = client.fetch_response(timeout=5)
                if not result["ok"]:
                    raise AssertionError(f"IUD update failed: {result}")
        for client in clients:
            client.assert_ok("commit;", timeout=5)

        for client in clients:
            client.assert_ok("begin;")
        for offset in range(deleted_per_client):
            for client_id, client in enumerate(clients):
                row_id = row_id_for(client_id, offset)
                client.execute_async(f"delete from account_stress where id = {row_id};")
            for client in clients:
                result = client.fetch_response(timeout=5)
                if not result["ok"]:
                    raise AssertionError(f"IUD delete failed: {result}")
        for client in clients:
            client.assert_ok("commit;", timeout=5)

        deleted_id = row_id_for(0, 0)
        retained_id = row_id_for(0, deleted_per_client)
        last_retained_id = row_id_for(len(clients) - 1, inserted_per_client - 1)

        assert_rows(
            check.assert_ok(f"select id, balance, owner from account_stress where id = {deleted_id};", timeout=5),
            [],
            "IUD deleted row absent by id index",
        )
        assert_rows(
            check.assert_ok(
                f"select id, balance, owner from account_stress where owner = 'u{deleted_id:07d}';",
                timeout=5,
            ),
            [],
            "IUD deleted row absent by owner index",
        )
        assert_rows(
            check.assert_ok(
                f"select id, balance, owner from account_stress where id = {retained_id};",
                timeout=5,
            ),
            [[str(retained_id), str(600000 + retained_id), f"u{retained_id:07d}"]],
            "IUD retained row by id index",
        )
        assert_rows(
            check.assert_ok(
                f"select id, balance, owner from account_stress where owner = 'u{retained_id:07d}';",
                timeout=5,
            ),
            [[str(retained_id), str(600000 + retained_id), f"u{retained_id:07d}"]],
            "IUD retained row by owner index",
        )
        assert_rows(
            check.assert_ok(
                f"select id, balance, owner from account_stress where owner = 'i{retained_id:07d}';",
                timeout=5,
            ),
            [],
            "IUD old owner index key removed after update",
        )
        assert_rows(
            check.assert_ok(
                f"select id, balance, owner from account_stress where id = {last_retained_id};",
                timeout=5,
            ),
            [[str(last_retained_id), str(600000 + last_retained_id), f"u{last_retained_id:07d}"]],
            "IUD last retained row",
        )
    finally:
        close_clients(clients)
        check.close()


TESTS = [
    ("read/read compatible", test_read_read_compatible),
    ("write blocks read until commit", test_write_blocks_read_until_commit),
    ("write/write conflict serializes", test_write_write_conflict_serializes),
    ("abort wakes waiter", test_abort_wakes_waiter),
    ("different RIDs update concurrently", test_different_rids_can_update_concurrently),
    ("two-transaction cycle breaks", test_two_transaction_cycle_breaks),
    ("8-client disjoint RID batch updates", test_eight_client_disjoint_rid_batch_updates),
    ("1 writer blocks 7 readers", test_one_writer_blocks_seven_readers),
    ("5-writer hotspot queue drains", test_five_writer_hotspot_queue_drains),
    ("4-transaction wait graph breaks", test_four_transaction_wait_graph_breaks),
    ("READ COMMITTED reader releases before commit", test_read_committed_reader_releases_before_commit),
    ("multi-row business transaction serializes", test_multi_row_business_transaction_serializes),
    ("7-client mixed DML same table", test_seven_client_mixed_dml_same_table),
    ("unique index conflict abort then retry", test_unique_index_conflict_abort_then_retry),
    ("8-client insert-update-delete workflow", test_eight_client_insert_update_delete_workflow),
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bin-dir", default="build/bin")
    parser.add_argument("--sql-root", default="test/sql/lab05")
    parser.add_argument("--reuse-server", action="store_true")
    args = parser.parse_args()

    bin_dir = Path(args.bin_dir).resolve()
    sql_root = Path(args.sql_root).resolve()

    proc = None
    if not args.reuse_server:
        proc = start_server(bin_dir)
    try:
        setup_database(bin_dir, sql_root)
        passed = 0
        for name, test in TESTS:
            test()
            passed += 1
            print(f"Passed: {name}")
        print(f"Passed [{passed}/{len(TESTS)}] lab05 2PL concurrency tests")
    finally:
        if proc is not None:
            stop_server(proc)


if __name__ == "__main__":
    main()
