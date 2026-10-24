use serde_json::Value;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

#[test]
fn control_mode_reports_bound_listener_and_stops_on_versioned_command() {
    let child = Command::new(env!("CARGO_BIN_EXE_sparkless-server"))
        .args(["control", "--host", "127.0.0.1", "--port", "0"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("start owned server process");
    let mut child = ChildGuard(child);
    let mut stdout = BufReader::new(child.0.stdout.take().expect("piped child stdout"));

    let mut ready_line = String::new();
    stdout
        .read_line(&mut ready_line)
        .expect("read readiness record");
    let ready: Value = serde_json::from_str(&ready_line).expect("valid JSON readiness record");
    assert_eq!(ready["type"], "ready");
    assert_eq!(ready["control_protocol_version"], 1);
    assert!(
        ready["server_instance_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(ready["capabilities"]["transport"], "http");
    assert_eq!(ready["agent_protocol_version"], "1");
    assert_eq!(ready["capabilities"]["agent_protocol_version"], "1");
    assert_eq!(ready["capabilities"]["mutation_protocol_version"], "1");
    assert_eq!(
        ready["capabilities"]["limits"]["session_idle_ttl_ms"],
        15 * 60 * 1000
    );
    assert_eq!(
        ready["capabilities"]["limits"]["session_ttl_exemptions"][0],
        "sql-warehouse"
    );
    assert_eq!(
        ready["capabilities"]["server_instance_id"],
        ready["server_instance_id"]
    );
    assert!(ready_line.len() <= 16 * 1024);

    let address: SocketAddr = ready["http_url"]
        .as_str()
        .expect("HTTP URL")
        .strip_prefix("http://")
        .expect("HTTP scheme")
        .parse()
        .expect("bound socket address");
    let mut stream = TcpStream::connect(address).expect("connect to readiness address");
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("set response timeout");
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("send health request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read health response");
    assert!(response.starts_with("HTTP/1.1 200"), "{response}");
    assert!(response.contains("\"status\":\"ready\""), "{response}");

    let stdin = child.0.stdin.as_mut().expect("piped child stdin");
    stdin
        .write_all(&vec![b'x'; 20 * 1024])
        .expect("send oversized control frame");
    stdin.write_all(b"\n").expect("terminate oversized frame");
    stdin
        .write_all(&vec![b'x'; 16 * 1024 - 1])
        .expect("send control frame at the size limit");
    stdin
        .write_all(b"\n")
        .expect("terminate control frame at the size limit");
    stdin
        .write_all(&vec![b'x'; 16 * 1024])
        .expect("send control frame above the size limit");
    stdin
        .write_all(b"\n")
        .expect("terminate control frame above the size limit");
    stdin
        .write_all(b"{\"control_protocol_version\":2,\"command\":\"shutdown\"}\n")
        .expect("send unsupported protocol command");
    stdin
        .write_all(b"{\"control_protocol_version\":1,\"command\":\"stop\"}\n")
        .expect("send unsupported command");
    stdin
        .write_all(b"{\"control_protocol_version\":1,\"command\":\"shutdown\"}\n")
        .expect("send shutdown command");
    stdin.flush().expect("flush shutdown command");
    drop(child.0.stdin.take());

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.0.try_wait().expect("poll child status").is_some() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "server did not stop after shutdown"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let mut stopped_line = String::new();
    stdout
        .read_line(&mut stopped_line)
        .expect("read stopped record");
    let stopped: Value = serde_json::from_str(&stopped_line).expect("valid stopped record");
    assert_eq!(stopped["type"], "stopped");
    assert_eq!(stopped["control_protocol_version"], 1);
    assert!(stopped_line.len() <= 16 * 1024);
}

#[test]
fn version_flag_reports_the_packaged_server_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_sparkless-server"))
        .arg("--version")
        .output()
        .expect("run server version command");
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout)
            .expect("version output is UTF-8")
            .trim(),
        format!("sparkless-server {}", env!("CARGO_PKG_VERSION"))
    );
}

#[cfg(unix)]
#[test]
fn serve_mode_drains_on_sigterm() {
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port");
    let address = reservation.local_addr().expect("read reserved address");
    drop(reservation);

    let child = Command::new(env!("CARGO_BIN_EXE_sparkless-server"))
        .args([
            "serve",
            "--host",
            "127.0.0.1",
            "--port",
            &address.port().to_string(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start server process");
    let mut child = ChildGuard(child);

    // This process starts a full native engine, and the control-mode test in this
    // integration binary also starts an engine concurrently. Give slower CI runners
    // time to initialize before treating a missing listener as a signal regression.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.0.try_wait().expect("poll server status") {
            panic!("server exited before becoming ready: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(100)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("set health response timeout");
            stream
                .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .expect("send health request");
            let mut response = String::new();
            stream
                .read_to_string(&mut response)
                .expect("read health response");
            assert!(response.starts_with("HTTP/1.1 200"), "{response}");
            break;
        }
        assert!(Instant::now() < deadline, "server did not become ready");
        thread::sleep(Duration::from_millis(20));
    }

    let signal = Command::new("kill")
        .args(["-TERM", &child.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(signal.success(), "SIGTERM command failed: {signal}");
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.0.try_wait().expect("poll stopped server") {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "server did not drain after SIGTERM"
        );
        thread::sleep(Duration::from_millis(20));
    };
    assert!(status.success(), "server exited unsuccessfully: {status}");
}
