extern crate snap;

use log::*;
use core::task::Context;
use core::task::Poll;
use std::pin::Pin;
use crate::tokio::io::AsyncRead;
use crate::tokio::io::AsyncWrite;
use crate::tokio::io::AsyncReadExt;
use tokio::io::Result;
use std::io::Cursor;

// start section copied from https://github.com/BurntSushi/rust-snappy

pub fn read_u24_le(slice: &[u8]) -> u32 {
    slice[0] as u32 | (slice[1] as u32) << 8 | (slice[2] as u32) << 16
}

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
            input_buffer:  vec![0; 1024],
            output_buffer: vec![0; 1024],
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
            while this.output_start != this.output_end {
                // println!("output_loop start");
                
                let count = std::cmp::min(buf.len(), this.output_end - this.output_start);

                buf.clone_from_slice(
                    &this.output_buffer[this.output_start..this.output_start + count]
                );

                this.output_start += count;

                return Poll::Ready(Ok(count));
            }
            
            this.output_start = 0;
            this.output_end   = 0;

            // println!("read_loop");

            if this.input_end < 4 {
                // println!("not enough data for kind and size");
                
                // println!("async_read");

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
            
            
            let written = std::io::Write::write(
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
            output_buffer: Vec::new(),
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
            
            &this.encoder.get_mut().set_position(0);
            
            this.output_start = 0;
            this.output_end   = 0;
            
            let wrote = std::io::Write::write(&mut this.encoder, buf)?;

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

struct AsyncMock {
    buffer:   Vec<u8>,
    position: usize,
}

impl AsyncRead for AsyncMock {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx:       &mut Context,
        buf:      &mut [u8]
    ) -> Poll<Result<usize>>
    {
        println!("async mock read {}", buf.len());

        if self.buffer.len() == self.position {
            return Poll::Ready(Ok(0));
        }

        let count = std::cmp::min(buf.len(), self.buffer.len() - self.position);
        
        buf.clone_from_slice(
            &self.buffer[self.position..self.position + count]
        );
        
        self.position += count;

        Poll::Ready(Ok(count))
    }
}

#[tokio::test]
async fn test_snappy_async_decompress() {
    let mut snappy_rx = NSQSnappyInflate::new(
        AsyncMock{
            buffer:   Vec::new(),
            position: 0
        }
    );

    let cursor: Cursor<Vec<u8>> = std::io::Cursor::new(vec![0; 1024]);
    let mut snappy_tx = snap::write::FrameEncoder::new(cursor);
    
    std::io::Write::write(&mut snappy_tx, b"12345").unwrap();
    std::io::Write::flush(&mut snappy_tx).unwrap();
    
    let end = snappy_tx.get_ref().position() as usize;
    snappy_rx.get_mut().buffer = snappy_tx.get_ref().get_ref()[0..end].to_vec();
    let mut actual = [0; 5];
    snappy_rx.read_exact(&mut actual).await.unwrap();
    assert_eq!(&actual, b"12345");
    
    snappy_tx.get_mut().set_position(0);
    snappy_rx.get_mut().buffer = Vec::new();
    snappy_rx.get_mut().position = 0;
    
    std::io::Write::write(&mut snappy_tx, b"hello").unwrap();
    std::io::Write::flush(&mut snappy_tx).unwrap();
    
    let end = snappy_tx.get_ref().position() as usize;
    snappy_rx.get_mut().buffer = snappy_tx.get_ref().get_ref()[0..end].to_vec();
    let mut actual = [0; 5];
    snappy_rx.read_exact(&mut actual).await.unwrap();
    assert_eq!(&actual, b"hello");
}
