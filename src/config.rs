pub const DEFAULT_ADDR: &str = "127.0.0.1:27631";

pub enum Mode {
    Server,
    Client { addr: String },
}

pub fn parse_mode() -> Mode {
    let mut args = std::env::args().skip(1);

    match args.next().as_deref() {
        None | Some("server") => Mode::Server,
        Some("client") => {
            let addr = args.next().unwrap_or_else(|| DEFAULT_ADDR.to_string());
            Mode::Client { addr }
        }
        Some(_) => Mode::Server,
    }
}
