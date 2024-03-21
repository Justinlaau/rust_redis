use crate::{Command, Connection, Db, DbDropGuard, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};

/// Server listener state. Created in the `run` call. It includes a `run` method
/// which performs the TCP listening and initialization of per-connection state.
#[derive(Debug)]
struct Listener{
    db_holder: DbDropGuard,

    /// TCP listener supplied by the 'run' caller
    listener: TcpListener,

    /// Limit the max number of connections
    /// 
    /// A 'Semaphore' is used to limit the max number of connections.
    /// Before attempting to accept a new connection, a permit is acquired from the 
    /// semaphore. If none are available, the listener waits for one.
    /// 
    /// when handlers complete processing a connection, the permit is returned
    /// to the semaphore
    limit_connections: Arc<Semaphore>,
    /// Broadcasts a shutdown signal to all active connections.
    ///
    /// The initial `shutdown` trigger is provided by the `run` caller. The
    /// server is responsible for gracefully shutting down active connections.
    /// When a connection task is spawned, it is passed a broadcast receiver
    /// handle. When a graceful shutdown is initiated, a `()` value is sent via
    /// the broadcast::Sender. Each active connection receives it, reaches a
    /// safe terminal state, and completes the task.
    notify_shutdown : broadcast::Sender<()>,
    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing.
    ///
    /// Tokio channels are closed once all `Sender` handles go out of scope.
    /// When a channel is closed, the receiver receives `None`. This is
    /// leveraged to detect all connection handlers completing. When a
    /// connection handler is initialized, it is assigned a clone of
    /// `shutdown_complete_tx`. When the listener shuts down, it drops the
    /// sender held by this `shutdown_complete_tx` field. Once all handler tasks
    /// complete, all clones of the `Sender` are also dropped. This results in
    /// `shutdown_complete_rx.recv()` completing with `None`. At this point, it
    /// is safe to exit the server process.
    shutdown_complete_tx: mpsc::Sender<()>,
}

/// per-connection handler. read requests from 'connection' and applies the commands to 'db'
#[derive(Debug)]
struct Handler{
    db: Db,
        /// The TCP connection decorated with the redis protocol encoder / decoder
    /// implemented using a buffered `TcpStream`.
    ///
    /// When `Listener` receives an inbound connection, the `TcpStream` is
    /// passed to `Connection::new`, which initializes the associated buffers.
    /// `Connection` allows the handler to operate at the "frame" level and keep
    /// the byte level protocol parsing details encapsulated in `Connection`.
    connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

const MAX_CONNECTIONS: usize = 250;

pub async fn run(listener: TcpListener, shutdown: impl Future){
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::channel(1);

    let mut server = Listener{
        listener,
        db_holder : DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
    };

    tokio::select!{
        res = server.run() => {
            if let Err(err) = res{
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down");
        }
    }

    // Extract the `shutdown_complete` receiver and transmitter
    // explicitly drop `shutdown_transmitter`. This is important, as the
    // `.await` below would otherwise never complete.
    // 這個是解構pattern，..表示其他變量不重要，但是記住，server是沒有被drop的，其他
    // 變量也沒有被drop，但是你不能再access了
    let Listener {
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;
    //由於server要離開scope才會被drop，所以在當前這個scope永遠會有一個sender，我們要把他拿出來drop掉
    drop(notify_shutdown);
    drop(shutdown_complete_tx);

    let _ = shutdown_complete_rx.recv().await;
}

impl Listener{
    /// Run the server
    ///
    /// Listen for inbound connections. For each inbound connection, spawn a
    /// task to process that connection.
    /// 有點像reactor， event driven，讓handle處理
    ///
    /// # Error
    /// 
    /// Returns 'Err' if accepting returns an error
    /// possible reason, exceeds os's max number of sockets
    
    async fn run(&mut self) -> crate::Result<()>{
        info!("accepting inbound connections");

        loop {
            // `acquire_owned` returns a permit that is bound to the semaphore.
            // When the permit value is dropped, it is automatically returned
            // to the semaphore.
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();
            
            let socket = self.accept().await?;

            let mut handler = Handler {
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                shutdown : Shutdown::new(self.notify_shutdown.subscribe()),

                _shutdown_complete: self.shutdown_complete_tx.clone()
            };

            tokio::spawn(async move{
                if let Err(err) = handler.run().await{
                    error!(cause = ?err, "connection error");
                }

                drop(permit);
            });
        }
    }

    async fn accept(&mut self)->crate::Result<TcpStream>{
        let mut backoff = 1;

        loop{
            match self.listener.accept().await{
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64{
                        return Err(err.into());
                    }
                }
            }
        }
        time::sleep(Duration::from_secs(backoff)).await;

        backoff *= 2;
    }
}

impl Handler {
    #[instrument(skip(self))]
    async fn run(&mut self)->crate::Result<()>{
        while !self.shutdown.is_shutdown(){
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let cmd = Command::from_frame(frame)?;
            debug!(?cmd);

            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;


        }
        Ok(())
    }
}