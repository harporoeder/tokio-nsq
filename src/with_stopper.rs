use log::*;

pub async fn with_stopper(
    shutdown_rx: tokio::sync::oneshot::Receiver<()>,
    operation:   impl std::future::Future
) {
    tokio::select! {
        _ = shutdown_rx => {
            info!("stopper operation stopped via oneshot");
        },
        _ = operation => {
            info!("stopper operation finished naturally");
        }
    }
}
