use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};

/// Bounded TCP transport with a timeout on stalled writes. Time spent computing
/// a response is not a write timeout; embedded routers retain their host policy.
pub struct Listener {
    tcp: TcpListener,
    connections: Arc<Semaphore>,
    timeout: Duration,
}
impl Listener {
    pub fn new(tcp: TcpListener, options: &crate::Options) -> Self {
        Self {
            tcp,
            connections: Arc::new(Semaphore::new(options.max_http_connections)),
            timeout: Duration::from_millis(options.http_write_timeout_ms),
        }
    }
}
impl axum::serve::Listener for Listener {
    type Io = Connection;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Connection, SocketAddr) {
        let permit = self.connections.clone().acquire_owned().await.unwrap();
        let (tcp, address) = axum::serve::Listener::accept(&mut self.tcp).await;
        (
            Connection {
                tcp,
                _permit: permit,
                timeout: self.timeout,
                write: None,
            },
            address,
        )
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.tcp.local_addr()
    }
}
pub struct Connection {
    tcp: TcpStream,
    _permit: OwnedSemaphorePermit,
    timeout: Duration,
    write: Option<Pin<Box<Sleep>>>,
}
fn deadline<T>(
    result: Poll<io::Result<T>>,
    timer: &mut Option<Pin<Box<Sleep>>>,
    timeout: Duration,
    cx: &mut Context<'_>,
) -> Poll<io::Result<T>> {
    match result {
        Poll::Ready(value) => {
            *timer = None;
            Poll::Ready(value)
        }
        Poll::Pending => {
            let sleep = timer.get_or_insert_with(|| {
                Box::pin(tokio::time::sleep_until(Instant::now() + timeout))
            });
            if sleep.as_mut().poll(cx).is_ready() {
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP socket idle timeout",
                )))
            } else {
                Poll::Pending
            }
        }
    }
}
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tcp).poll_read(cx, buf)
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.tcp).poll_write(cx, bytes);
        let timeout = self.timeout;
        deadline(result, &mut self.write, timeout, cx)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // TcpStream is unbuffered: a successful no-op flush is not socket write
        // progress and must not restart a pending write's idle deadline.
        Pin::new(&mut self.tcp).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.tcp).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::serve::Listener as _;
    #[tokio::test]
    async fn slow_handler_is_not_a_socket_write_timeout() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let options = crate::Options {
            http_write_timeout_ms: 20,
            ..Default::default()
        };
        let router = axum::Router::new().route(
            "/slow",
            axum::routing::get(|| async {
                tokio::time::sleep(Duration::from_millis(80)).await;
                "completed"
            }),
        );
        let server = tokio::spawn(axum::serve(Listener::new(tcp, &options), router).into_future());
        let response = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .unwrap()
            .get(format!("http://{address}/slow"))
            .send()
            .await;
        server.abort();
        let response = response.unwrap();
        assert!(response.status().is_success());
        assert_eq!(response.text().await.unwrap(), "completed");
    }
    #[tokio::test]
    async fn connections_are_bounded_and_stalled_writes_expire() {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = tcp.local_addr().unwrap();
        let options = crate::Options {
            max_http_connections: 1,
            http_write_timeout_ms: 20,
            ..Default::default()
        };
        let mut listener = Listener::new(tcp, &options);
        let _first_client = TcpStream::connect(address).await.unwrap();
        let (first, _) = listener.accept().await;
        let _second_client = TcpStream::connect(address).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
        drop(first);
        let (mut second, _) = tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .unwrap();
        let bytes = vec![0; 16 << 20];
        let stalled = async {
            let mut offset = 0;
            while offset < bytes.len() {
                offset += std::future::poll_fn(|cx| {
                    let result = Pin::new(&mut second).poll_write(cx, &bytes[offset..]);
                    if result.is_pending() {
                        let _ = Pin::new(&mut second).poll_flush(cx);
                    }
                    result
                })
                .await?;
            }
            Ok::<_, io::Error>(())
        };
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(200), stalled)
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
    }
}
