use std::collections::HashMap;

/// Define server command args
#[derive(clap::Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct ServerArgs {
    /// Node identify use for ocenaraft node.
    #[arg(long)]
    pub node_id: u64,

    /// Server listend network address.
    #[arg(long)]
    pub addr: String,

    /// Server should know other nodes.
    #[arg(long)]
    pub nodes: String,
}

impl ServerArgs {
    pub fn validate(&self) -> Result<(), String> {
        if self.node_id == 0 {
            return Err("node_id must be more than 0".to_string());
        }

        if self.addr.is_empty() {
            return Err("addr must be not empty".to_string());
        }

        if let Err(_) = self.addr.parse::<std::net::SocketAddr>() {
            return Err(format!("{} is not valid network addr", self.addr));
        }

        if let Err(err) = self.parse_nodes() {
            return Err(err.to_string());
        }

        Ok(())
    }

    pub fn parse_nodes(
        &self,
    ) -> Result<HashMap<u64, String>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let iter = self.nodes.split(',');
        let mut nodes = HashMap::new();
        for s in iter {
            if let Some(pos) = s.find('=') {
                let (k, v) = (s[..pos].parse()?, s[pos + 1..].parse()?);
                nodes.insert(k, v);
            }
        }

        Ok(nodes)
    }
}
