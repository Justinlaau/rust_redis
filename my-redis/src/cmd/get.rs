use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
use tracing::{debug, instrument};

#[derive(Debug)]
pub struct Get {
    /// Name of the key to get
    key: String,
}

impl Get{
    pub fn new(key: impl ToString)->Get{
        Get{
            key: key.to_string(),
        }
    }

    pub fn key(&self)->&str{
        &self.key
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get>{
        let key = parse.next_string()?;

        Ok(Get{key})
    }

    #[instrument(self, db, dst)]
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection)->crate::Result<()>{
        let response = if let Some(value) = db.get(&self.key){
            Frame::Bulk(value)
        }else{
            Frame::Null
        };

        debug!(?response);

        dst.write_frame(&response).await?;

        Ok(())
    }

    /// convert the command into an equivalent 'Frame'
    /// This is called by the client when encoding a 'Get' command
    /// to send to the server
    pub(crate) fn into_frame(self)->Frame{
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}
