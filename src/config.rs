pub const DEFAULT_ADDR: &str = "127.0.0.1:27631";

pub enum Mode {
    Server,
    Client,
    Status,
}

pub fn parse_mode() -> Mode {
    let mut args = std::env::args().skip(1);

    match args.next().as_deref() {
        None | Some("server") => Mode::Server,
        Some("client") => Mode::Client,
        Some("status") => Mode::Status,
        Some(_) => Mode::Server,
    }
}
