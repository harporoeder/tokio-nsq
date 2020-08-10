extern crate snap;

use log::*;
use core::task::Context;
use core::task::Poll;
use std::pin::Pin;
use crate::tokio::io::AsyncRead;
use crate::tokio::io::AsyncWrite;
use tokio::io::Result;

// start section copied from https://github.com/BurntSushi/rust-snappy

const STREAM_IDENTIFIER: &'static [u8] = b"\xFF\x06\x00\x00sNaPpY";

pub fn read_u24_le(slice: &[u8]) -> u32 {
    slice[0] as u32 | (slice[1] as u32) << 8 | (slice[2] as u32) << 16
}

// end section

pub struct NSQSnappyInflate<S> {
    inner:         S,
    initial:       bool,
    input_buffer:  Vec<u8>,
    output_buffer: Vec<u8>,
    input_end:     usize,
    input_start:   usize,
    output_start:  usize,
}

impl<S> NSQSnappyInflate<S> {
    pub fn new(inner: S) -> Self {
        NSQSnappyInflate {
            initial:       true,
            input_buffer:  vec![0; 1024],
            output_buffer: Vec::new(),
            input_end:     0,
            input_start:   0,
            output_start:  0,
            inner,
        }
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
        
        println!("poll_read");
        
        loop {
            println!("main_loop");
            
            while this.output_start != this.output_buffer.len() {
                println!("output_loop start");
                
                let count = std::cmp::min(buf.len(), this.output_buffer.len() - this.output_start);

                buf.clone_from_slice(
                    &this.output_buffer[this.output_start..this.output_start + count]
                );

                this.output_start += count;

                println!("output_loop end {}", count);

                return Poll::Ready(Ok(count));
            }
            
            this.output_start = 0;
            this.output_buffer.resize(0, 0);

            println!("read_loop");

            if this.input_end < 4 {
                println!("not enough data for kind and size");
                
                println!("async_read");

                match AsyncRead::poll_read(
                    Pin::new(&mut this.inner), cx, &mut this.input_buffer[this.input_end..]
                ) {
                    Poll::Ready(Ok(0)) => {
                        println!("ok 0");

                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Ok(n)) => {
                        this.input_end += n;
                    },
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        println!("pending 0");

                        return Poll::Pending;
                    },
                }
                
                println!("input_end {}", this.input_end);
                
                continue;
            }
            
            let len: usize = read_u24_le(&this.input_buffer[1..]) as usize;
            
            if this.input_end < len + 4 {
                println!("not enough data for frame body");

                println!("async_read");

                match AsyncRead::poll_read(
                    Pin::new(&mut this.inner), cx, &mut this.input_buffer[this.input_end..]
                ) {
                    Poll::Ready(Ok(0)) => {
                        println!("ok 1");
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Ok(n)) => {
                        this.input_end += n;
                    },
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        println!("pending 1");
                        return Poll::Pending;
                    },
                }
                
                println!("input_end {}", this.input_end);
                
                continue;
            }
            
            println!("len is {}", len);
            
            println!("process_loop");
            
            match this.input_buffer[0] {
                0xFF => {
                    println!("frame kind header");
                    
                    let identifier: Vec<u8> = vec![
                        0x73, 0x4e, 0x61, 0x50, 0x70, 0x59
                    ];
                    
                    if &this.input_buffer[4..10] != identifier.as_slice() {
                        println!("does not match");
                        
                        return Poll::Pending;
                    }
                    
                    println!("does match");
                },
                0x00 => {
                    println!("frame kind compressed");
                },
                0x01 => {
                    println!("frame kind uncompressed {}", len);
                    
                    println!("x1 = {:?}", this.input_buffer[4..len + 4].to_vec());
                    
                    if len < 4 {
                        println!("frame kind uncompressed size violation");
                    }
                    
                    let x = this.input_buffer[8..len + 4].to_vec();
                    println!("x2 = {:?}", x);
                    
                    // let y = std::str::from_utf8(&x).unwrap();
                    // println!("y = {}", y);
                    
                    this.output_buffer = x;
                },
                0xFE => {
                    println!("frame kind padding");
                },
                _ => {
                    println!("frame kind unknown");
                }
            }
            
            println!("start {:?}", this.input_buffer[0..this.input_end].to_vec());
            
            this.input_buffer = this.input_buffer[4 + len..this.input_end].to_vec();
            this.input_buffer.resize(1024, 0);
            this.input_end -= 4 + len;
            
            println!("end {:?}", this.input_buffer[0..this.input_end].to_vec());
            
            println!("input_end post {}", this.input_end);
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
            println!("main_write_loop");

            if this.output_start != this.output_end {
                match AsyncWrite::poll_write(
                    Pin::new(&mut this.inner),
                    cx,
                    &this.encoder.get_mut().get_mut()[this.output_start..this.output_end]
                ) {
                    Poll::Ready(Ok(0)) => {
                        println!("write ok0");
                        return Poll::Ready(Ok(0));
                    }
                    Poll::Ready(Ok(n)) => {
                        println!("write ready");
                        this.output_start += n;

                        if this.output_start != this.output_end {
                            return Poll::Pending;
                        } else {
                            return Poll::Ready(Ok(buf.len()));
                        }
                    },
                    Poll::Ready(Err(err)) => {
                        println!("write error");
                        error!("write ready error {}", err);
                        return Poll::Ready(Err(err));
                    },
                    Poll::Pending => {
                        println!("write pending");
                        return Poll::Pending;
                    },
                }
            }
            
            &this.encoder.get_mut().set_position(0);
            
            println!("snappy_write post write {}", buf.len());
            
            this.output_start = 0;
            this.output_end   = 0;
            
            std::io::Write::write(&mut this.encoder, buf)?;

            if this.encoder.get_ref().position() == 0 {
                std::io::Write::flush(&mut this.encoder)?;
            }
            
            this.output_end = this.encoder.get_ref().position() as usize;

            println!("compressed length {}", this.output_end);
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