
use bytes::{Buf, Bytes};
use std::convert::TryInto;
use std::fmt;
use std::io::{Cursor};
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
            actual => Err(format!("protocol error; invalid frame type byte {}", actual).into()),
        }
    }

    pub fn parse(src: &mut Cursor<&[u8]>)->Result<Frame, Error>{
        //get the first byte and parse the line
        match get_u8(src)?{
            b'+' => {
                let line = get_line(src)?.to_vec();
                
                let string = String::from_utf8(line)?;

                Ok(Frame::Simple(string))
            }
            b'-' =>{
                let line = get_line(src)?.to_vec();

                let string  = String::from_utf8(line)?;

                Ok(Frame::Error(string))
            }
            b':' =>{
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            b'$' =>{
                if b'-' == peek_u8(src)?{
                    let line = get_line(src)?;

                    if line != b"-1"{
                        return Err("protocal error; invalid frame fomat".into());
                    }
                    Ok(Frame::Null)
                }else{
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;
                    if src.remaining() < n{
                        return Err(Error::Incomplete);
                    }

                    let data = Bytes::copy_from_slice(&src.bytes()[..len]);

                    skip(src, n)?;
                    Ok(Frame::Bulk(data))
                }
            }
            b'*' =>{
                let len = get_decimal(src)?.try_into()?;
                
                let mut out = Vec::with_capacity(len);
                // parse 
                for _ in 0..len{
                    out.push(Frame::parse(src)?);
                }

                Ok(Frame::Array(out))
            }
            _ => unimplemented!(),
        }
    }
    pub(crate) fn to_error(&self)->crate::Error{
        format!("unexpected frame: {}", self).into()
    }

}

impl fmt::Display for Frame{
    fn fmt(&self, fmt: & mut fmt::Formatter)->fmt::Result{
        use std::str;
        // formatter 係 format的形式， "asdad {}" 而我地要將 self中不同的類型拆出黎交比 佢地中的fmt function去做
        match self{
            Frame::Simple(response) => response.fmt(fmt),
            Frame::Error(msg) => write!(fmt, "error: {}", msg),
            Frame::Integer(num) => num.fmt(fmt),
            Frame::Bulk(msg)=> match str::from_utf8(msg){
                Ok(string) => string.fmt(fmt),
                Err(_) => write!(fmt, "{:?}", msg)
            },
            Frame::Null => "(nil)".fmt(fmt),
            Frame::Array(parts) =>{
                for (i, part) in parts.iter().enumerate(){
                    if i > 0 {
                        write!(fmt, " ")?;
                        part.fmt(fmt)?;
                    }
                }
                Ok(())
            }

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

impl From<String> for Error{
    fn from(src: String) -> Error{
        Error::Other(src.into())
    }
}

impl From<&str> for Error{
    fn from(src: &str)->Error{
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for Error{
    fn from(_src : FromUtf8Error)->Error{
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error{
    fn from(_src: TryFromIntError)->Error{
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for Error {}

impl fmt::Display for Error{
    fn fmt(&self, fmt: &mut fmt::Formatter)->fmt::Result{
        match self{
            Error::Incomplete => "stream ended early".fmt(fmt),
            Error::Other(err) => err.fmt(fmt),
        }
    }
}