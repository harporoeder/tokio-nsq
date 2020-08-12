extern crate snap;

use core::task::Context;
use core::task::Poll;
use std::pin::Pin;
use crate::tokio::io::AsyncRead;
use crate::tokio::io::AsyncWrite;
use tokio::io::Result;

// start section copied from https://github.com/BurntSushi/rust-snappy

pub fn read_u24_le(slice: &[u8]) -> u32 {
    slice[0] as u32 | (slice[1] as u32) << 8 | (slice[2] as u32) << 16
}

const MAX_COMPRESS_BLOCK_SIZE: usize = 76490;
const MAX_BLOCK_SIZE: usize          = 1 << 16;

// end section

pub struct NSQSnappyInflate<S> {
    inner:         S,
    input_buffer:  Vec<u8>,
    output_buffer: Vec<u8>,
    input_end:     usize,
    output_start:  usize,
    output_end:    usize,
    decoder:       snap::read::FrameDecoder<std::io::Cursor<Vec<u8>>>,
}

impl<S> NSQSnappyInflate<S> {
    pub fn new(inner: S) -> Self {
        let output_buffer: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(output_buffer);

        NSQSnappyInflate {
            input_buffer:  vec![0; MAX_COMPRESS_BLOCK_SIZE],
            output_buffer: vec![0; MAX_BLOCK_SIZE],
            decoder:       snap::read::FrameDecoder::new(cursor),
            input_end:     0,
            output_start:  0,
            output_end:    0,
            inner,
        }
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}

impl<S> AsyncRead for NSQSnappyInflate<S>
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

                this.output_start += count;

                return Poll::Ready(Ok(count));
            }
            
            this.output_start = 0;
            this.output_end   = 0;

            if this.input_end < 4 {
                match AsyncRead::poll_read(
                    Pin::new(&mut this.inner), cx, &mut this.input_buffer[this.input_end..4]
                ) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Ok(n)) => {
                        this.input_end += n;
                    },
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    },
                }
                
                continue;
            }
            
            let len: usize = read_u24_le(&this.input_buffer[1..]) as usize;
            
            if this.input_end < len + 4 {
                match AsyncRead::poll_read(
                    Pin::new(&mut this.inner), cx, &mut this.input_buffer[this.input_end..len + 4]
                ) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Ok(n)) => {
                        this.input_end += n;
                    },
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    },
                }
                
                continue;
            }

            this.decoder.get_mut().set_position(0);
            
            std::io::Write::write(
                &mut this.decoder.get_mut(),
                &this.input_buffer[..len + 4]
            )?;
            
            this.decoder.get_mut().set_position(0);
            
            this.input_end    = 0;
            this.output_start = 0;
            this.output_end   = 0;
            
            loop {
                let decoded = std::io::Read::read(
                    &mut this.decoder,
                    &mut this.output_buffer[this.output_end..],
                )?;
                
                if decoded == 0 {
                    break;
                }
                
                this.output_end += decoded;
            }
        }
    }
}

pub struct NSQSnappyDeflate<S> {
    inner:         S,
    initial:       bool,
    output_buffer: Vec<u8>,
    output_start:  usize,
    output_end:    usize,
    encoder:       snap::write::FrameEncoder<std::io::Cursor<Vec<u8>>>
}

impl<S> NSQSnappyDeflate<S> {
    pub fn new(inner: S) -> Self {
        let output_buffer: Vec<u8> = Vec::new();
        let cursor = std::io::Cursor::new(output_buffer);

        NSQSnappyDeflate {
            initial:       true,
            output_buffer: vec![0; MAX_COMPRESS_BLOCK_SIZE],
            output_start:  0,
            output_end:    0,
            encoder:       snap::write::FrameEncoder::new(cursor),
            inner,
        }
    }
}

impl<S> AsyncWrite for NSQSnappyDeflate<S>
    where S: AsyncWrite + Unpin
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
        buf:      &[u8]
    ) -> Poll<Result<usize>>
    {
        let this = &mut *self;

        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        loop {
            if this.output_start != this.output_end {
                match AsyncWrite::poll_write(
                    Pin::new(&mut this.inner),
                    cx,
                    &this.encoder.get_mut().get_mut()[this.output_start..this.output_end]
                ) {
                    Poll::Ready(Ok(0)) => {
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Ok(n)) => {
                        this.output_start += n;

                        if this.output_start != this.output_end {
                            return Poll::Pending;
                        } else {
                            return Poll::Ready(Ok(buf.len()));
                        }
                    },
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        return Poll::Pending;
                    },
                }
            }
            
            this.encoder.get_mut().set_position(0);
            
            this.output_start = 0;
            this.output_end   = 0;
            
            std::io::Write::write(&mut this.encoder, buf)?;

            if this.encoder.get_ref().position() == 0 {
                std::io::Write::flush(&mut this.encoder)?;
            }
            
            this.output_end = this.encoder.get_ref().position() as usize;
        }
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        _cx:  &mut Context,
    ) -> Poll<Result<()>>
    {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
    ) -> Poll<Result<()>>
    {
        AsyncWrite::poll_shutdown(Pin::new(&mut self.inner), cx)
    }
}
