
use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::Cursor;
use std::num::TryFromIntError;
use std::string::FromUtf8Error;



#[derive(Clone, Debug)]
pub enum Frame{
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(bytes::Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error{
    Incomplete,
    Other(crate::Error),
}

impl Frame{
    /// return an empty array
    pub(crate) fn array() -> Frame{
        Frame::Array(vec![])
    }
    /*
        push bulk of frame into Array
        # panic if self is not Array
     */
    pub(crate) fn push_bulk(&mut self, b: Bytes){
        match self{
            Frame::Array(vec)=>{
                vec.push(Frame::Bulk(b));
            }
            _ => panic!("not an array frame"),
        }
    }

    /*
    push integer frame into array
    #self must tbe an array frame
     */
    pub(crate) fn push_int(&mut self, value: u64){
        match self{
            Frame::Array(vec)=>{
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    ///check if an entire message can be decoded from 'src'
    pub fn check(src: &mut Cursor<&[u8]>)->Result<(), Error>{
        match get_u8(src)?{
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            b'$' => {
                if b'-' == peek_u8(src)?{
                    // Skip 4 bytes '-1\r\n'
                    skip(src, 4)
                }else{
                    let len : usize = get_decimal(src)?.try_into()?;
                    // 2 is '\r\n'
                    skip(src, len + 2)
                }
            }
            b'*'  => {
                let len = get_decimal(src)?;

                for _ in 0..len{
                    Frame::check(src)?;
                }

                Ok(())
            }
            actual => Err(format!("protocol error; invalid frame type byte `{}`", actual).into()),
        }
    }

    

}

impl PartialEq<&str> for Frame{
    /// if you were to compare a Frame object directly with an &str, 
    /// it would require converting the &str into a 
    /// String in order to perform the comparison.
    fn eq(&self, other: &&str) -> bool{
        match self{
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}

fn get_u8(src: &mut Cursor<&[u8]>)->Result<u8, Error>{
    if !src.has_remaining() { 
        return Err(Error::Incomplete);
    }
    Ok(src.get_u8())
}

// returning the first bytes 
fn peek_u8(src: &mut Cursor<&[u8]>)->Result<u8, Error>{
    if !src.has_remaining(){
        return Err(Error::Incomplete);
    }
    Ok(src.bytes()[0])
}

// advance the cursor with n bytes
fn skip(src : &mut Cursor<&[u8]>, n: usize)->Result<(), Error>{
    if src.remaining() < n {
        return Err(Error::Incomplete);
    }
    src.advance(n);
    Ok(())
}

// read a new-line terminated decimal
fn get_decimal(src: &mut Cursor<&[u8]>)->Result<u64, Error>{
    use atoi::atoi;
    let line = get_line(src)?;

    atoi::<u64>(line).ok_or_else(|| "protocal error; invalid frame format".into())
}

/// Find a line
/// 呢到一定要加life time，complier唔知道return 翻去的reference 會誤會變成 dangling referece
fn get_line<'a>(src: &mut Cursor<&'a [u8]>)->Result<&'a [u8], Error>{
    let start = src.position() as usize;

    // get the length of the bytes array that the cursor pointing at
    let end = src.get_ref().len() - 1;

    for i in start..end {
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n'{
            //we found a terminator, update the position to be *after* the \n
            src.set_position((i + 2) as u64);

            // return the line
            return Ok(&src.get_ref()[start..i]);
        }
    }
    Err(Error::Incomplete)
}