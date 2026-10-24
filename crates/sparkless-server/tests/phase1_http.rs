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

    let (status, rejected_append) = statement(
        &server,
        "owner",
        "INSERT INTO phase_http.items (id, note) VALUES ('not-an-integer', 'invalid')",
        Some("rejected-cast-append"),
    );
    assert_eq!(status, 200, "{rejected_append}");
    assert_eq!(rejected_append["status"], "error");
    assert_eq!(rejected_append["operation"]["effect_outcome"], "failed");

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
