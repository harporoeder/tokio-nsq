extern crate async_compression;

use std::pin::Pin;
use crate::tokio::io::AsyncWrite;
use core::task::Context;
use core::task::Poll;
use tokio::io::Result;
use async_compression::tokio_02::write::DeflateEncoder;

pub struct NSQInflateCompress<S> {
    encoder: DeflateEncoder<S>,
    written: usize
}

impl<S: AsyncWrite + Unpin> NSQInflateCompress<S> {
    pub fn new(inner: S, level: u8) -> Self {
        Self {
            written: 0,
            encoder: DeflateEncoder::with_quality(
                inner, async_compression::Level::Precise(level as u32)
            ),
        }
    }
}
impl<S> AsyncWrite for NSQInflateCompress<S>
    where S: AsyncWrite + Unpin
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
        buf:      &[u8]
    ) -> Poll<Result<usize>>
    {
        loop {
            if self.written == buf.len() {
                match AsyncWrite::poll_flush(
                    Pin::new(&mut self.encoder),
                    cx
                ) {
                    Poll::Ready(Ok(())) => {
                        self.written = 0;
                        return Poll::Ready(Ok(buf.len()));
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    },
                }
            }

            match AsyncWrite::poll_write(
                Pin::new(&mut self.encoder),
                cx,
                buf
            ) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Ok(n)) => {
                    self.written += n;

                    continue;
                },
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Err(err));
                },
                Poll::Pending => {
                    return Poll::Pending;
                },
            }
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
    ) -> Poll<Result<()>>
    {
        AsyncWrite::poll_flush(Pin::new(&mut self.encoder), cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
    ) -> Poll<Result<()>>
    {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.encoder), cx)
    }
}
