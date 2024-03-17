use crate::frame::{self, Frame};

use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Send and receive 'Frame' from a remote peer
/// 
/// we just need to read and write the frames on the underlying 'TcpStream'
/// 
/// to read frame, the 'Conneciton' uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. How to check? 
/// the protocal has pattern, just need to decode it.
/// Once we can create a full frame, Connection create and return it to the caller  
/// 
/// when sending a frame, the frame is first encoded into the write buffer
/// and then it will write to the socket


#[derive(Debug)]
pub struct Connection{
    /// here we use bufwriter to decorate our tcpstream, because it
    /// provides write level buffering, and it is tokio's, so good
    stream: BufWriter<TcpStream>,

    /// the buffer is for reading the frame. for now, we do it manually
    /// we can use tokio_util::codec later
    buffer: BytesMut,
}

impl Connection{
    pub fn new(socket: TcpStream) -> Connection{
        Connection { 
            stream: BufWriter::new(socket), 
            buffer: BytesMut::with_capacity(4096), 
        }
    }

    /// Read a single 'Frame' value from the underlying stream.
    /// 
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept for the next call  'read_frame'
    
    pub async fn read_frame(&mut self)->crate::Result<Option<Frame>>{
        // loop until a frame is constructed
        loop {
            // attempt to parse a frame
            if let Some(frame) = self.parse_frame()?{
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await?{
                if self.buffer.is_empty(){
                    return Ok(None);
                }else{
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> crate::Result<Option<Frame>>{
        use frame::Error::Incomplete;
        
        let mut buf = Cursor::new(&self.buffer[..]);
        
        // The first step is to check if enough data has been buffered to parse
        // a single frame.
        match Frame::check(&mut buf){
            Ok(_) => {
                let len = buf.position() as usize;

                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                self.buffer.advance(len);
                Ok(Some(frame))
            },
            Err(Incomplete) => Ok(None),
            Err(e) => Err(e.into())
        }
    }
    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()>{
        match frame{
            Frame::Array(val)=>{
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;

                for entry in val{
                    self.write_value(entry).await?;
                }
            }
            _ => self.write_value(frame).await?,
        }

        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await
    }

    async fn write_value(&mut self, frame: &Frame) -> io::Result<()>{
        match frame{
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unreachable!(),
        }
        Ok(())
    }

    async fn write_decimal(&mut self, val : u64) -> io::Result<()>{
        use std::io::Write;

        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;
        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}