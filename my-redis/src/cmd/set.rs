use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};


#[derive(Debug)]
pub struct Set{
    key: String,
    value: Bytes,

    expire: Option<Duration>
}

impl Set{
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>)->Set{
        Set { key: key.to_string(), value, expire }
    }

    pub fn key(&self)->&str{
        &self.key
    }

    pub fn value(&self) -> &Bytes{
        &self.value
    }

    pub fn expire(&self) -> Option<Duration>{
        self.expire
    }

    pub(crate) fn parse_frames(parse: &mut Parse)->crate::Result<Set>{
        use ParseError::EndOfStream;

        let key = parse.next_string()?;

        let value = parse.next_bytes()?;

        let mut expire = None;

        match parse.next_string(){
            Ok(s) if s.to_uppercase() == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // An expiration is specified in milliseconds. The next value is
                // an integer.
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            // Currently, mini-redis does not support any of the other SET
            // options. An error here results in the connection being
            // terminated. Other connections will continue to operate normally.
            Ok(_) => return Err("currently `SET` only supports the expiration option".into()),
            // The `EndOfStream` error indicates there is no further data to
            // parse. In this case, it is a normal run time situation and
            // indicates there are no specified `SET` options.
            Err(EndOfStream) => {}
            // All other errors are bubbled up, resulting in the connection
            // being terminated.
            Err(err) => return Err(err.into()),
        }

        Ok(Set{key, value, expire})
    }

    #[instrument(skip(self, db, dst))]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value, self.expire);
        
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }


    /// Converts the command into an equivalent `Frame`.
    ///
    /// This is called by the client when encoding a `Set` command to send to
    /// the server.
    pub(crate) fn into_frame(self) -> Frame{
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        if let Some(ms) = self.expire {
            // Expirations in Redis procotol can be specified in two ways
            // 1. SET key value EX seconds
            // 2. SET key value PX milliseconds
            // We the second option because it allows greater precision and
            // src/bin/cli.rs parses the expiration argument as milliseconds
            // in duration_from_ms_str()
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }
}