fn main() {
    if sparkless_server::run_cli_for("sparkless-server").is_err() {
        std::process::exit(2);
    }
}
