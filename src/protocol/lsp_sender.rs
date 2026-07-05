use tokio::sync::mpsc::Sender;

use super::lsp::LspFrame;
use super::rad::RadMessage;

#[derive(Clone)]
pub struct LspSender {
    tx: Sender<RadMessage>,
}

impl LspSender {
    pub fn new(tx: Sender<RadMessage>) -> Self {
        Self { tx }
    }

    pub async fn send(
        &self,
        frame: LspFrame,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<RadMessage>> {
        self.tx.send(RadMessage::lsp(frame)).await
    }
}
