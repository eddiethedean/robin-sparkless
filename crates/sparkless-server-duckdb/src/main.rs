fn main() {
    if sparkless_server_duckdb::run().is_err() {
        std::process::exit(2);
    }
}
