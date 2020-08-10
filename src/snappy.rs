extern crate snap;

use log::*;
use core::task::Context;
use core::task::Poll;
use std::pin::Pin;
use crate::tokio::io::AsyncRead;
use tokio::io::Result;

pub fn read_u24_le(slice: &[u8]) -> u32 {
    slice[0] as u32 | (slice[1] as u32) << 8 | (slice[2] as u32) << 16
}

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

            println!("read_loop");

            if this.input_end < 4 {
                println!("not enough data for kind and size");
                
                println!("async_read");

                match AsyncRead::poll_read(Pin::new(&mut this.inner), cx, &mut this.input_buffer) {
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

                match AsyncRead::poll_read(Pin::new(&mut this.inner), cx, &mut this.input_buffer) {
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
                    
                    let y = std::str::from_utf8(&x).unwrap();
                    println!("y = {}", y);
                    
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