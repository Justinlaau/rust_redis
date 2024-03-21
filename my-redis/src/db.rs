use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use tracing::debug;

/// A wrapper around a "Db" instance. It allow us to orderly clean up of the db by signalling the background purge task 
/// to shut down when this struct is dropped

#[derive(Debug)]
pub(crate) struct DbDropGuard{
    /// The Db instance will be shut down ehn this 'Db holder' got dropped  
    db: Db,
}

/// Server state shared across all connections
/// 
/// 'Db' contains a 'HashMap' storing the key/value data and all
/// 'broadcast::Sender' values for active pub/sub channels.
/// 
/// 'Db' instance only handle to shared state, cloning db only incurs an arc increment
/// 
/// When db value is created, a background task is spawned. This task is used to expire values
/// this is used to expire values after the duration has elapsed. 
/// The task runs until all instances of "Db" are dropped
#[derive(Debug, Clone)]
pub(crate) struct Db{
    /// handle to shared state.
    /// background task will also have one
    shared: Arc<Shared>
}


#[derive(Debug)]
struct Shared{
    /// The shared state is guarded by a mutex. This is a `std::sync::Mutex` and
    /// not a Tokio mutex. This is because there are no asynchronous operations
    /// being performed while holding the mutex. Additionally, the critical
    /// sections are very small.
    ///
    /// A Tokio mutex is mostly intended to be used when locks need to be held
    /// across `.await` yield points. All other cases are **usually** best
    /// served by a std mutex. If the critical section does not include any
    /// async operations but is long (CPU intensive or performing blocking
    /// operations), then the entire operation, including waiting for the mutex,
    /// is considered a "blocking" operation and `tokio::task::spawn_blocking`
    /// should be used.
    state: Mutex<State>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,
}

#[derive(Debug)]
struct State{
    entries : HashMap<String, Entry>,
    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `mini-redis` handles this by using a separate `HashMap`.
    pub_sub : HashMap<String, broadcast::Sender<Bytes>>,

    /// tracks key TTLS (time to live)
    /// 
    /// A 'BTreeSet' is used to maintain expirations sorted by when they expire
    /// This allows the background task to iterate this map to find the value 
    /// expriring next.
    /// 
    /// while highly unlikeyly, it is possible for more than one expiration to be
    /// created for the same instant. Because of this, the "Instant" is
    /// not enough for the key. String is used to break these ties
    expirations: BTreeSet<(Instant, String)>,

    shutdown: bool
}



/// Entry in the key-value store
#[derive(Debug)]
struct Entry{
    data: Bytes,
    
    /// Instant at which the entry expires and should be removed from the database
    expires_at: Option<Instant>
}

impl DbDropGuard{
    pub(crate) fn new() -> DbDropGuard{
        DbDropGuard{db : Db::new()}
    }
    pub(crate) fn db(&self) -> Db{
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    fn drop(&mut self){
        self.db.shutdown_purge_task();
    }
}

impl Db{
    pub(crate) fn new() ->Db {
        let shared = Arc::new(Shared{
            state: Mutex::new(State { 
                entries: HashMap::new(), 
                pub_sub: HashMap::new(), 
                expirations: BTreeSet::new(), 
                shutdown: false,
            }),
            background_task : Notify::new()
        });

        tokio::spawn(purge_expired_tasks(shared.clone()));
        Db{shared}
    }

    pub(crate) fn get(&self, key: &str)->Option<Bytes>{
        // Acquire the lock, get the entry and clone the value
        
        //the clone is shallow clone
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    // if a value is already associated with a key, remove it
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>){
        let mut state = self.shared.state.lock().unwrap();

        let mut notify = false; 
        
        let expires_at = expire.map(|duration|{
            //Instant at which the key expires
            let when = Instant::now() + duration; 

            // if this 'set' becomes the key that expires **next**, the background
            // task needs to be notified so it can update its state
            //
            // Whether or not the task needs to be notified is computed during the 
            // 'set' routine
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            when
        });

        // Insert the entry into the 'HashMap'
        let prev_key_pair = state.entries.insert(
            key.clone(),
            Entry { data: value, expires_at: expires_at }
        );

        //remove if the same key exist  
        if let Some(prev) = prev_key_pair{
            if let Some(when) = prev.expires_at{
                state.expirations.remove(&(when, key.clone()));
            }
        }
        // release the mutex before notifying, because help to reduce contention
        // dropping needs to acquire a mutex, if we dont drop it, it will cause busy
        // loop
        drop(state);

        if notify{
            self.shared.background_task.notify_one();
        }
    }

    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes>{
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().unwrap();

        // if there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key){
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // A message would stored in the channel, until all subscribers 
                // have seen it.
                // This means that a slow subscriber could result in messages being 
                // held indefinitely
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize{
        let state = self.shared.state.lock().unwrap();

        state.pub_sub
            .get(key)
            // on a successful message send on the broadcast channel
            // the number of subscribers is returned.
            // Error means there are no receivers
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }

    /// signals the purge background task to shut down
    fn shutdown_purge_task(&self){
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;
        drop(state);
        self.shared.background_task.notify_one()
    }
}

impl Shared{
    /// purge all expired keys and return the "Instant" at which the
    /// next key will expire. the background task will sleep until this
    /// instant
    fn purge_expired_keys(&self) -> Option<Instant>{
        let mut state = self.state.lock().unwrap();

        if state.shutdown{
            // the database is shutting down.
            // All handles to the share state have dropped
            return None;
        }
        // This is needed to make the borrow checker happy. In short, `lock()`
        // returns a `MutexGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the mutex guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        let now  = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next(){
            if when > now{
                return Some(when);
            }
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }
        None
    }

    fn is_shutdown(&self) -> bool{
        self.state.lock().unwrap().shutdown
    }
}


impl State{
    fn next_expiration(&self)-> Option<Instant>{
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

async fn purge_expired_tasks(shared: Arc<Shared>){
    while !shared.is_shutdown(){
        if let Some(when) = shared.purge_expired_keys(){
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select!{
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        }else{

        }
    }
    debug!("Purge background task shut down")
}