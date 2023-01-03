use std::collections::{BTreeMap, HashMap};

use jsonwebtoken::{DecodingKey, EncodingKey};
use tokio::{
    net::TcpListener,
    sync::broadcast::{self, Sender},
    time::{self, Duration},
};
use xline::{client::Client, server::XlineServer};

/// Cluster
pub struct Cluster {
    /// listeners of members
    listeners: BTreeMap<usize, TcpListener>,
    /// address of members
    all_members: HashMap<String, String>,
    /// Client of cluster
    client: Option<Client>,
    /// Stop sender
    stop_tx: Option<Sender<()>>,
    /// Cluster size
    size: usize,
}

impl Cluster {
    /// New `Cluster`
    pub(crate) async fn new(size: usize) -> Self {
        let mut listeners = BTreeMap::new();
        for i in 0..size {
            listeners.insert(i, TcpListener::bind("0.0.0.0:0").await.unwrap());
        }
        let all_members: HashMap<String, String> = listeners
            .iter()
            .map(|(i, l)| (format!("server{}", i), l.local_addr().unwrap().to_string()))
            .collect();

        Self {
            listeners,
            all_members,
            client: None,
            stop_tx: None,
            size,
        }
    }

    /// Start `Cluster`
    pub(crate) async fn start(&mut self) {
        let (stop_tx, _) = broadcast::channel(1);

        for i in 0..self.size {
            let name = format!("server{}", i);
            let is_leader = i == 0;
            let mut rx = stop_tx.subscribe();
            let listener = self.listeners.remove(&i).unwrap();
            let all_members = self.all_members.clone();
            tokio::spawn(async move {
                let server =
                    XlineServer::new(name, all_members, is_leader, Self::test_key_pair()).await;
                let signal = async {
                    let _ = rx.recv().await;
                };
                let result = server.start_from_listener_shoutdown(listener, signal).await;
                if let Err(e) = result {
                    panic!("Server start error: {e}");
                }
            });
        }
        self.stop_tx = Some(stop_tx);
        // Sleep 30ms, wait for the server to start
        time::sleep(Duration::from_millis(30)).await;
    }

    /// Create or get the client with the specified index
    pub(crate) async fn client(&mut self) -> &mut Client {
        if self.client.is_none() {
            let client = Client::new(self.all_members.clone(), true)
                .await
                .unwrap_or_else(|e| {
                    panic!("Client connect error: {:?}", e);
                });
            self.client = Some(client);
        }
        self.client.as_mut().unwrap()
    }

    #[allow(dead_code)] // used in tests but get warning
    pub fn addrs(&self) -> &HashMap<String, String> {
        &self.all_members
    }

    fn test_key_pair() -> Option<(EncodingKey, DecodingKey)> {
        let private_key = include_bytes!("../private.pem");
        let public_key = include_bytes!("../public.pem");
        let encoding_key = EncodingKey::from_rsa_pem(private_key).unwrap();
        let decoding_key = DecodingKey::from_rsa_pem(public_key).unwrap();
        Some((encoding_key, decoding_key))
    }
}
