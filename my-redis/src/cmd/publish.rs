use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;


#[derive(Debug)]
pub struct Publish {
    /// Name of the channel on which the message should be published.
    channel: String,

    /// The message to publish.
    message: Bytes,
}

impl Publish {
    /// Create a new `Publish` command which sends `message` on `channel`.
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel: channel.to_string(),
            message,
        }
    }
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        // The `PUBLISH` string has already been consumed. Extract the `channel`
        // and `message` values from the frame.
        //
        // The `channel` must be a valid string.
        let channel = parse.next_string()?;

        // The `message` is arbitrary bytes.
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // The shared state contains the `tokio::sync::broadcast::Sender` for
        // all active channels. Calling `db.publish` dispatches the message into
        // the appropriate channel.
        //
        // The number of subscribers currently listening on the channel is
        // returned. This does not mean that `num_subscriber` channels will
        // receive the message. Subscribers may drop before receiving the
        // message. Given this, `num_subscribers` should only be used as a
        // "hint".
        let num_subscribers = db.publish(&self.channel, self.message);

        // The number of subscribers is returned as the response to the publish
        // request.
        let response = Frame::Integer(num_subscribers as u64);

        // Write the frame to the client.
        dst.write_frame(&response).await?;

        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}