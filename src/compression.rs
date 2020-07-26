use super::*;

use miniz_oxide::inflate;
use core::task::Context;
use core::task::Poll;
use tokio::io::Result;
use std::pin::Pin;
use failure::Fail;
use std::fmt;
use std::io::{Error, ErrorKind};

#[derive(Debug, Fail)]
struct DecompressError;

impl fmt::Display for DecompressError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self)
    }
}

pub struct NSQInflate<S> {
    inner:         S,
    inflate:       inflate::stream::InflateState,
    input_buffer:  Vec<u8>,
    output_buffer: Vec<u8>,
    output_start:  usize,
    output_end:    usize,
    input_end:     usize,
}

impl<S> NSQInflate<S> {
    pub fn new(inner: S) -> Self {
        NSQInflate {
            inner:         inner,
            inflate:       inflate::stream::InflateState::new(miniz_oxide::DataFormat::Raw),
            input_buffer:  vec![0; 512],
            output_buffer: vec![0; 1024],
            output_start:  0,
            output_end:    0,
            input_end:     0,
        }
    }
}

impl<S> AsyncRead for NSQInflate<S>
    where S: AsyncRead + Unpin
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
        buf:      &mut [u8]
    ) -> Poll<Result<usize>>
    {
        let this = &mut *self;

        loop {
            if this.output_start != this.output_end {
                let count = std::cmp::min(buf.len(), this.output_end - this.output_start);

                buf.clone_from_slice(
                    &this.output_buffer[this.output_start..this.output_start + count]
                );

                this.output_start = this.output_start + count;

                info!("write count {}", count);

                return Poll::Ready(Ok(count));
            }

            this.output_start = 0;
            this.output_end   = 0;

            match AsyncRead::poll_read(Pin::new(&mut this.inner), cx, &mut this.input_buffer) {
                Poll::Ready(Ok(0)) => {
                    info!("ready 0");
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Ok(n)) => {
                    info!("ready {}", n);
                    this.input_end = n;
                },
                Poll::Ready(Err(err)) => {
                    info!("ready error {}", err);
                    return Poll::Ready(Err(err));
                },
                Poll::Pending => {
                    info!("ready pending");
                    return Poll::Pending;
                },
            }

            let result = miniz_oxide::inflate::stream::inflate(
                &mut this.inflate,
                &this.input_buffer[..this.input_end],
                &mut this.output_buffer,
                miniz_oxide::MZFlush::Sync
            );

            info!("got status {} {}", result.bytes_consumed, result.bytes_written);

            this.output_end += result.bytes_written;

            match result.status {
                Ok(_) => {
                    info!("status ok");
                },
                Err(err) => {
                    info!("status error {:?}", err);

                    return Poll::Ready(Err(Error::new(ErrorKind::Other, "decompress")));
                }
            }
        }
    }
}
