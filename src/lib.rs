#![allow(dead_code)]

extern crate hyper;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate byteorder;
extern crate log;
extern crate tokio_rustls;
extern crate rustls;
extern crate regex;
#[macro_use]
extern crate lazy_static;
extern crate backoff;
extern crate miniz_oxide;

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use log::*;
use crate::tokio::io::AsyncWrite;
use crate::tokio::io::AsyncRead;
use crate::tokio::io::AsyncWriteExt;
use crate::tokio::io::AsyncReadExt;
use std::convert::TryFrom;
use failure::{Error};

mod connection;
mod producer;
mod consumer;
mod compression;
mod connection_config;

pub use connection::*;
pub use producer::*;
pub use consumer::*;
pub use connection_config::*;
