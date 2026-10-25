use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Barrier};
use std::time::Duration;

struct ServerProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    address: SocketAddr,
    instance_id: String,
}

impl ServerProcess {
    fn start() -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_sparkless-server"))
            .args(["control", "--host", "127.0.0.1", "--port", "0"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("start native HTTP server");
        let stdin = child.stdin.take();
        let stdout = BufReader::new(child.stdout.take().expect("piped server stdout"));
        let mut process = Self {
            stdin,
            stdout,
            child,
            address: "127.0.0.1:0".parse().unwrap(),
            instance_id: String::new(),
        };

        let mut line = String::new();
        process
            .stdout
            .read_line(&mut line)
            .expect("read server readiness");
        assert!(!line.is_empty(), "server exited before readiness");
        let ready: Value = serde_json::from_str(&line).expect("valid readiness JSON");
        assert_eq!(ready["type"], "ready");
        process.instance_id = ready["server_instance_id"]
            .as_str()
            .expect("server identity")
            .to_string();
        process.address = ready["http_url"]
            .as_str()
            .expect("HTTP URL")
            .strip_prefix("http://")
            .expect("HTTP scheme")
            .parse()
            .expect("bound HTTP address");
        process
    }

    fn stop(&mut self) {
        if let Some(mut stdin) = self.stdin.take() {
            stdin
                .write_all(b"{\"control_protocol_version\":1,\"command\":\"shutdown\"}\n")
                .expect("send graceful shutdown");
            stdin.flush().expect("flush graceful shutdown");
        }
        let status = self.child.wait().expect("wait for server shutdown");
        assert!(status.success(), "server process exited with {status}");
        let mut stopped_line = String::new();
        self.stdout
            .read_line(&mut stopped_line)
            .expect("read server stopped record");
        let stopped: Value = serde_json::from_str(&stopped_line).expect("valid stopped JSON");
        assert_eq!(stopped["type"], "stopped");
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn post(server: &ServerProcess, path: &str, body: &Value) -> (u16, Value) {
    post_to(server.address, path, body)
}

fn post_to(address: SocketAddr, path: &str, body: &Value) -> (u16, Value) {
    let encoded = serde_json::to_vec(body).expect("encode request JSON");
    let mut stream = TcpStream::connect(address).expect("connect to server listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set HTTP read timeout");
    write!(
        stream,
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        encoded.len()
    )
    .expect("write HTTP headers");
    stream.write_all(&encoded).expect("write HTTP body");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .expect("read HTTP response");
    let response = String::from_utf8(response).expect("HTTP response is UTF-8");
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("complete HTTP response");
    let status = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .expect("HTTP status code");
    (
        status,
        serde_json::from_str(body).expect("JSON response body"),
    )
}

fn statement(
    server: &ServerProcess,
    session: &str,
    sql: &str,
    operation_id: Option<&str>,
) -> (u16, Value) {
    let mut body = json!({"session":session,"kind":"sql","code":sql});
    if let Some(operation_id) = operation_id {
        body["server_instance_id"] = json!(server.instance_id);
        body["operation_id"] = json!(operation_id);
    }
    post(server, "/statements", &body)
}

#[test]
fn phase1_http_shared_tables_mutations_and_restart_contract() {
    let mut server = ServerProcess::start();
    let instance_id = server.instance_id.clone();

    let (status, created_db) = statement(
        &server,
        "owner",
        "CREATE DATABASE phase_http",
        Some("create-database"),
    );
    assert_eq!(status, 200, "{created_db}");
    assert_eq!(created_db["status"], "ok");

    let (status, created_table) = statement(
        &server,
        "owner",
        "CREATE TABLE phase_http.items (id BIGINT, note STRING)",
        Some("create-table"),
    );
    assert_eq!(status, 200, "{created_table}");
    assert_eq!(created_table["status"], "ok");

    let (status, existing_table_noop) = statement(
        &server,
        "owner",
        "CREATE TABLE IF NOT EXISTS phase_http.items (id BIGINT, note STRING)",
        Some("create-existing-table-noop"),
    );
    assert_eq!(status, 200, "{existing_table_noop}");
    assert_eq!(existing_table_noop["status"], "ok");
    assert_eq!(
        existing_table_noop["operation"]["effect_outcome"],
        "committed"
    );
    assert_eq!(
        existing_table_noop["receipt"], created_table["receipt"],
        "a successful idempotent create returns the existing table receipt"
    );
    let (status, no_op_status) = post(
        &server,
        "/operations/status",
        &json!({
            "server_instance_id":instance_id,
            "operation_id":"create-existing-table-noop"
        }),
    );
    assert_eq!(status, 200, "{no_op_status}");
    assert_eq!(no_op_status["execution_state"], "succeeded");
    assert_eq!(no_op_status["effect_outcome"], "committed");
    assert_eq!(no_op_status["receipt"], created_table["receipt"]);

    let (status, created_default_items) = statement(
        &server,
        "owner",
        "CREATE TABLE default.items (id BIGINT, note STRING)",
        Some("create-default-items"),
    );
    assert_eq!(status, 200, "{created_default_items}");
    assert_eq!(created_default_items["status"], "ok");
    let (status, inserted_default_item) = statement(
        &server,
        "owner",
        "INSERT INTO default.items VALUES (99, 'default')",
        Some("insert-default-item"),
    );
    assert_eq!(status, 200, "{inserted_default_item}");
    assert_eq!(inserted_default_item["status"], "ok");

    let (status, created_integer_table) = statement(
        &server,
        "owner",
        "CREATE TABLE phase_http.integer_items (id INT)",
        Some("create-integer-table"),
    );
    assert_eq!(status, 200, "{created_integer_table}");
    assert_eq!(created_integer_table["status"], "ok");
    let (status, empty_integer_table) = statement(
        &server,
        "reader",
        "SELECT id FROM phase_http.integer_items",
        None,
    );
    assert_eq!(status, 200, "{empty_integer_table}");
    assert_eq!(
        empty_integer_table["data"]["application/json"]["schema"]["fields"][0]["type"],
        "integer"
    );
    let (status, rejected_integer_overflow) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.integer_items VALUES (2147483648)",
        Some("reject-integer-overflow"),
    );
    assert_eq!(status, 200, "{rejected_integer_overflow}");
    assert_eq!(rejected_integer_overflow["status"], "error");
    assert_eq!(
        rejected_integer_overflow["operation"]["effect_outcome"],
        "failed"
    );
    let (status, accepted_integer_boundary) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.integer_items VALUES (2147483647)",
        Some("accept-integer-boundary"),
    );
    assert_eq!(status, 200, "{accepted_integer_boundary}");
    assert_eq!(
        accepted_integer_boundary["operation"]["effect_outcome"],
        "committed"
    );
    let (status, integer_boundary_read) = statement(
        &server,
        "reader",
        "SELECT id FROM phase_http.integer_items",
        None,
    );
    assert_eq!(status, 200, "{integer_boundary_read}");
    assert_eq!(
        integer_boundary_read["data"]["application/json"]["data"],
        json!([[2147483647]])
    );

    let (status, created_temporal_table) = statement(
        &server,
        "owner",
        "CREATE TABLE phase_http.temporal_items (event_ts TIMESTAMP, event_day DATE, small SMALLINT)",
        Some("create-temporal-table"),
    );
    assert_eq!(status, 200, "{created_temporal_table}");
    assert_eq!(created_temporal_table["status"], "ok");
    let (status, empty_temporal_table) = statement(
        &server,
        "reader",
        "SELECT * FROM phase_http.temporal_items",
        None,
    );
    assert_eq!(status, 200, "{empty_temporal_table}");
    assert_eq!(
        empty_temporal_table["data"]["application/json"]["schema"]["fields"][0]["type"],
        "timestamp"
    );
    assert_eq!(
        empty_temporal_table["data"]["application/json"]["schema"]["fields"][1]["type"],
        "date"
    );
    assert_eq!(
        empty_temporal_table["data"]["application/json"]["schema"]["fields"][2]["type"],
        "short"
    );
    let (status, rejected_date) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.temporal_items VALUES ('2025-01-02T03:04:05', 'not-a-date', 32767)",
        Some("reject-invalid-date"),
    );
    assert_eq!(status, 200, "{rejected_date}");
    assert_eq!(rejected_date["status"], "error");
    assert_eq!(rejected_date["operation"]["effect_outcome"], "failed");
    let (status, rejected_smallint_overflow) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.temporal_items VALUES ('2025-01-02T03:04:05', '2025-01-02', 32768)",
        Some("reject-smallint-overflow"),
    );
    assert_eq!(status, 200, "{rejected_smallint_overflow}");
    assert_eq!(rejected_smallint_overflow["status"], "error");
    assert_eq!(
        rejected_smallint_overflow["operation"]["effect_outcome"],
        "failed"
    );
    let (status, accepted_temporal_boundary) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.temporal_items VALUES ('2025-01-02T03:04:05', '2025-01-02', 32767)",
        Some("accept-temporal-boundary"),
    );
    assert_eq!(status, 200, "{accepted_temporal_boundary}");
    assert_eq!(
        accepted_temporal_boundary["operation"]["effect_outcome"],
        "committed"
    );
    let (status, temporal_read) = statement(
        &server,
        "reader",
        "SELECT * FROM phase_http.temporal_items",
        None,
    );
    assert_eq!(status, 200, "{temporal_read}");
    assert_eq!(
        temporal_read["data"]["application/json"]["data"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let (status, scalar_table) = statement(
        &server,
        "owner",
        "CREATE TABLE phase_http.scalar_items (exact_id BIGINT, amount DECIMAL(38,4), event_time TIMESTAMP, event_day DATE)",
        Some("create-scalar-items"),
    );
    assert_eq!(status, 200, "{scalar_table}");
    assert_eq!(scalar_table["status"], "ok");
    let (status, scalar_insert) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.scalar_items VALUES (9007199254740993, '1234567890123456789012345678901234.5678', '2025-01-02T03:04:05.123456', '2025-01-02')",
        Some("insert-scalar-items"),
    );
    assert_eq!(status, 200, "{scalar_insert}");
    assert_eq!(scalar_insert["operation"]["effect_outcome"], "committed");
    let (status, scalar_view) = statement(
        &server,
        "owner",
        "CREATE TEMP VIEW scalar_snapshot AS SELECT exact_id, amount, event_time, event_day FROM phase_http.scalar_items WHERE exact_id > 0",
        Some("create-scalar-snapshot"),
    );
    assert_eq!(status, 200, "{scalar_view}");
    assert_eq!(scalar_view["status"], "ok");
    let (status, scalar_projection) = statement(
        &server,
        "owner",
        "SELECT event_day, amount, exact_id, event_time FROM scalar_snapshot",
        None,
    );
    assert_eq!(status, 200, "{scalar_projection}");
    assert_eq!(
        scalar_projection["data"]["application/json"]["schema"]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field["type"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["date", "decimal(38,4)", "long", "timestamp"]
    );
    assert_eq!(
        scalar_projection["data"]["application/json"]["data"],
        json!([[
            "2025-01-02",
            "1234567890123456789012345678901234.5678",
            9007199254740993_i64,
            "2025-01-02T03:04:05.123456Z"
        ]])
    );
    let (status, scalar_count) = statement(
        &server,
        "reader",
        "SELECT COUNT(*) AS n FROM phase_http.scalar_items",
        None,
    );
    assert_eq!(status, 200, "{scalar_count}");
    assert_eq!(
        scalar_count["data"]["application/json"]["data"],
        json!([[1]])
    );
    assert_eq!(
        scalar_count["data"]["application/json"]["schema"]["fields"][0]["type"],
        "long"
    );
    assert_eq!(
        scalar_count["data"]["application/json"]["schema"]["fields"][0]["nullable"],
        false
    );

    let insert_sql = "INSERT INTO phase_http.items (id, note) VALUES (7, 'seven')";
    let submit_barrier = Arc::new(Barrier::new(2));
    let simultaneous = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&submit_barrier);
            let address = server.address;
            let instance_id = server.instance_id.clone();
            let sql = insert_sql.to_string();
            std::thread::spawn(move || {
                let request = json!({
                    "session":"owner",
                    "kind":"sql",
                    "code":sql,
                    "server_instance_id":instance_id,
                    "operation_id":"append-once"
                });
                barrier.wait();
                post_to(address, "/statements", &request)
            })
        })
        .collect::<Vec<_>>();
    let submissions = simultaneous
        .into_iter()
        .map(|submission| submission.join().expect("concurrent HTTP client"))
        .collect::<Vec<_>>();
    for (status, response) in &submissions {
        assert_eq!(*status, 200, "{response}");
    }
    assert!(submissions.iter().any(|(_, response)| {
        response["status"] == "ok" && response["operation"]["effect_outcome"] == "committed"
    }));
    assert!(submissions.iter().all(|(_, response)| {
        response["status"] == "ok" || response["ename"] == "OperationInProgress"
    }));

    let (status, selected_database) = statement(
        &server,
        "owner",
        "USE phase_http",
        Some("use-phase-http-owner"),
    );
    assert_eq!(status, 200, "{selected_database}");
    assert_eq!(selected_database["status"], "ok");
    let (status, owner_unqualified_read) =
        statement(&server, "owner", "SELECT id, note FROM items", None);
    assert_eq!(status, 200, "{owner_unqualified_read}");
    assert_eq!(
        owner_unqualified_read["data"]["application/json"]["data"],
        json!([[7, "seven"]])
    );
    let (status, reader_unqualified_read) =
        statement(&server, "reader", "SELECT id, note FROM items", None);
    assert_eq!(status, 200, "{reader_unqualified_read}");
    assert_eq!(
        reader_unqualified_read["data"]["application/json"]["data"],
        json!([[99, "default"]])
    );

    let (status, view) = statement(
        &server,
        "owner",
        "CREATE TEMP VIEW local_only AS SELECT 99 AS id",
        Some("create-local-view"),
    );
    assert_eq!(status, 200, "{view}");
    assert_eq!(view["status"], "ok");

    let (status, visible_to_peer) = statement(
        &server,
        "reader",
        "SELECT id, note FROM phase_http.items ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{visible_to_peer}");
    assert_eq!(
        visible_to_peer["data"]["application/json"]["data"],
        json!([[7, "seven"]])
    );

    let (status, hidden_view) = statement(&server, "reader", "SELECT id FROM local_only", None);
    assert_eq!(status, 200, "{hidden_view}");
    assert_eq!(hidden_view["status"], "error");

    let (status, saved_snapshot) = statement(
        &server,
        "owner",
        "CREATE TEMP VIEW saved_snapshot AS SELECT id FROM phase_http.items",
        Some("create-saved-snapshot"),
    );
    assert_eq!(status, 200, "{saved_snapshot}");
    assert_eq!(saved_snapshot["status"], "ok");

    let (status, appended_after_snapshot) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.items (id, note) VALUES (8, 'eight')",
        Some("append-after-snapshot"),
    );
    assert_eq!(status, 200, "{appended_after_snapshot}");
    assert_eq!(
        appended_after_snapshot["operation"]["effect_outcome"],
        "committed"
    );

    let (status, pinned_snapshot_read) = statement(
        &server,
        "owner",
        "SELECT id FROM saved_snapshot ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{pinned_snapshot_read}");
    assert_eq!(
        pinned_snapshot_read["data"]["application/json"]["data"],
        json!([[7]])
    );

    let (status, latest_table_read) = statement(
        &server,
        "reader",
        "SELECT id, note FROM phase_http.items ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{latest_table_read}");
    assert_eq!(
        latest_table_read["data"]["application/json"]["data"],
        json!([[7, "seven"], [8, "eight"]])
    );

    let (status, created_peer_table) = statement(
        &server,
        "owner",
        "CREATE TABLE phase_http.peer_items (peer_id BIGINT, item_id BIGINT)",
        Some("create-peer-table"),
    );
    assert_eq!(status, 200, "{created_peer_table}");
    assert_eq!(created_peer_table["status"], "ok");
    let (status, inserted_peer_row) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.peer_items VALUES (9007199254740993, 7)",
        Some("insert-peer-row"),
    );
    assert_eq!(status, 200, "{inserted_peer_row}");
    assert_eq!(
        inserted_peer_row["operation"]["effect_outcome"],
        "committed"
    );

    let (status, joined_tables) = statement(
        &server,
        "reader",
        "SELECT i.id AS item_id, p.peer_id AS peer_id FROM phase_http.items i CROSS JOIN phase_http.peer_items p ORDER BY item_id",
        None,
    );
    assert_eq!(status, 200, "{joined_tables}");
    assert_eq!(joined_tables["status"], "ok", "{joined_tables}");
    assert_eq!(
        joined_tables["data"]["application/json"]["schema"]["fields"],
        json!([
            {"name":"item_id","type":"long","nullable":true,"metadata":{}},
            {"name":"peer_id","type":"long","nullable":true,"metadata":{}}
        ])
    );
    assert_eq!(
        joined_tables["data"]["application/json"]["data"],
        json!([[7, 9007199254740993_i64], [8, 9007199254740993_i64]])
    );
    let (status, inner_joined_tables) = statement(
        &server,
        "reader",
        "SELECT i.id AS item_id, p.peer_id AS peer_id FROM phase_http.items i INNER JOIN phase_http.peer_items p ON i.id = p.item_id ORDER BY item_id",
        None,
    );
    assert_eq!(status, 200, "{inner_joined_tables}");
    assert_eq!(inner_joined_tables["status"], "ok", "{inner_joined_tables}");
    assert_eq!(
        inner_joined_tables["data"]["application/json"]["data"],
        json!([[7, 9007199254740993_i64]])
    );
    let (status, rejected_outer_join) = statement(
        &server,
        "reader",
        "SELECT i.id FROM phase_http.items i LEFT JOIN phase_http.peer_items p ON i.id = p.peer_id",
        None,
    );
    assert_eq!(status, 200, "{rejected_outer_join}");
    assert_eq!(rejected_outer_join["status"], "error");
    assert_eq!(rejected_outer_join["ename"], "UnsupportedSql");

    let (status, rejected_append) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.items (id, note) VALUES ('not-an-integer', 'invalid')",
        Some("rejected-cast-append"),
    );
    assert_eq!(status, 200, "{rejected_append}");
    assert_eq!(rejected_append["status"], "error");
    assert_eq!(rejected_append["operation"]["effect_outcome"], "failed");

    let (status, failed_operation) = post(
        &server,
        "/operations/status",
        &json!({"server_instance_id":instance_id,"operation_id":"rejected-cast-append"}),
    );
    assert_eq!(status, 200, "{failed_operation}");
    assert_eq!(failed_operation["execution_state"], "failed");
    assert_eq!(failed_operation["effect_outcome"], "failed");
    assert_eq!(failed_operation["error"]["ename"], rejected_append["ename"]);

    let (status, replayed_failure) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.items (id, note) VALUES ('not-an-integer', 'invalid')",
        Some("rejected-cast-append"),
    );
    assert_eq!(status, 200, "{replayed_failure}");
    assert_eq!(replayed_failure["ename"], rejected_append["ename"]);
    assert_eq!(replayed_failure["operation"]["effect_outcome"], "failed");
    assert_eq!(
        replayed_failure["operation"]["error"],
        failed_operation["error"]
    );

    let (status, rejected_select_cast) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.items (id, note) SELECT note, note FROM phase_http.items",
        Some("rejected-select-cast"),
    );
    assert_eq!(status, 200, "{rejected_select_cast}");
    assert_eq!(rejected_select_cast["status"], "error");
    assert_eq!(
        rejected_select_cast["operation"]["effect_outcome"],
        "failed"
    );

    let (status, rejected_update) = statement(
        &server,
        "owner",
        "UPDATE phase_http.items SET note = 'changed' WHERE id = 7",
        Some("unsupported-update"),
    );
    assert_eq!(status, 200, "{rejected_update}");
    assert_eq!(rejected_update["status"], "error");
    assert_eq!(rejected_update["ename"], "UnsupportedSql");

    let (status, after_rejections) = statement(
        &server,
        "reader",
        "SELECT id, note FROM phase_http.items ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{after_rejections}");
    assert_eq!(
        after_rejections["data"]["application/json"]["data"],
        json!([[7, "seven"], [8, "eight"]])
    );

    let (status, closed) = post(&server, "/close", &json!({"session":"owner"}));
    assert_eq!(status, 200, "{closed}");
    assert_eq!(closed["status"], "ok");

    let (status, replayed) = statement(&server, "owner", insert_sql, Some("append-once"));
    assert_eq!(status, 200, "{replayed}");
    assert_eq!(replayed["operation"]["effect_outcome"], "committed");

    let (status, operation) = post(
        &server,
        "/operations/status",
        &json!({"server_instance_id":instance_id,"operation_id":"append-once"}),
    );
    assert_eq!(status, 200, "{operation}");
    assert_eq!(operation["effect_outcome"], "committed");

    let conflicting_sql = "INSERT INTO phase_http.items (id, note) VALUES (8, 'eight')";
    let (status, conflict) = statement(&server, "owner", conflicting_sql, Some("append-once"));
    assert_eq!(status, 200, "{conflict}");
    assert_eq!(conflict["ename"], "OperationIdentityConflict");

    let whitespace_variant = "INSERT INTO  phase_http.items (id, note) VALUES (7, 'seven')";
    let (status, exact_bytes_conflict) =
        statement(&server, "owner", whitespace_variant, Some("append-once"));
    assert_eq!(status, 200, "{exact_bytes_conflict}");
    assert_eq!(exact_bytes_conflict["ename"], "OperationIdentityConflict");

    let (status, cross_session_retry) =
        statement(&server, "reader", insert_sql, Some("append-once"));
    assert_eq!(status, 200, "{cross_session_retry}");
    assert_eq!(cross_session_retry["ename"], "OperationIdentityConflict");

    let (status, reopened_session) = statement(&server, "owner", "SELECT 1 AS value", None);
    assert_eq!(status, 200, "{reopened_session}");
    assert_eq!(reopened_session["status"], "ok");

    let (status, stale_generation_retry) =
        statement(&server, "owner", insert_sql, Some("append-once"));
    assert_eq!(status, 200, "{stale_generation_retry}");
    assert_eq!(stale_generation_retry["ename"], "OperationIdentityConflict");

    let (status, after_replay) = statement(
        &server,
        "reader",
        "SELECT id, note FROM phase_http.items ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{after_replay}");
    assert_eq!(
        after_replay["data"]["application/json"]["data"],
        json!([[7, "seven"], [8, "eight"]])
    );

    let (status, retained_view) = statement(
        &server,
        "owner",
        "CREATE TEMP VIEW before_recreate AS SELECT id, note FROM phase_http.items",
        Some("create-before-recreate-view"),
    );
    assert_eq!(status, 200, "{retained_view}");
    assert_eq!(retained_view["status"], "ok");

    let (status, dropped_table) = statement(
        &server,
        "owner",
        "DROP TABLE phase_http.items",
        Some("drop-recreated-table"),
    );
    assert_eq!(status, 200, "{dropped_table}");
    assert_eq!(dropped_table["operation"]["effect_outcome"], "committed");

    let (status, recreated_table) = statement(
        &server,
        "owner",
        "CREATE TABLE phase_http.items (id BIGINT, note STRING)",
        Some("recreate-table-name"),
    );
    assert_eq!(status, 200, "{recreated_table}");
    assert_eq!(recreated_table["operation"]["effect_outcome"], "committed");

    let (status, inserted_recreated_row) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.items VALUES (70, 'recreated')",
        Some("insert-recreated-row"),
    );
    assert_eq!(status, 200, "{inserted_recreated_row}");
    assert_eq!(
        inserted_recreated_row["operation"]["effect_outcome"],
        "committed"
    );

    let (status, retained_view_read) = statement(
        &server,
        "owner",
        "SELECT id, note FROM before_recreate ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{retained_view_read}");
    assert_eq!(
        retained_view_read["data"]["application/json"]["data"],
        json!([[7, "seven"], [8, "eight"]])
    );

    let (status, recreated_binding_read) = statement(
        &server,
        "reader",
        "SELECT id, note FROM phase_http.items ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{recreated_binding_read}");
    assert_eq!(
        recreated_binding_read["data"]["application/json"]["data"],
        json!([[70, "recreated"]])
    );

    server.stop();
    let restarted = ServerProcess::start();
    assert_ne!(restarted.instance_id, instance_id);
    let (status, fresh_catalog) = statement(
        &restarted,
        "reader",
        "SELECT id, note FROM phase_http.items ORDER BY id",
        None,
    );
    assert_eq!(status, 200, "{fresh_catalog}");
    assert_eq!(fresh_catalog["status"], "error");

    let (status, old_instance_status) = post(
        &restarted,
        "/operations/status",
        &json!({"server_instance_id":instance_id,"operation_id":"append-once"}),
    );
    assert_eq!(status, 409, "{old_instance_status}");
    assert_eq!(old_instance_status["ename"], "ServerInstanceMismatch");

    let stale_mutation = json!({
        "session":"reader",
        "kind":"sql",
        "code":"CREATE DATABASE stale_identity",
        "server_instance_id":instance_id,
        "operation_id":"old-instance-create"
    });
    let (status, stale_mutation) = post(&restarted, "/statements", &stale_mutation);
    assert_eq!(status, 409, "{stale_mutation}");
    assert_eq!(stale_mutation["ename"], "ServerInstanceMismatch");

    let (status, new_instance_create) = statement(
        &restarted,
        "reader",
        "CREATE DATABASE stale_identity",
        Some("new-instance-create"),
    );
    assert_eq!(status, 200, "{new_instance_create}");
    assert_eq!(new_instance_create["status"], "ok");
}
