use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::Error;

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq, Eq)]
pub struct Endpoint {
    address: String,
    port: u32,
    redis_port: u32,
}

impl Endpoint {
    pub fn new(addr: impl ToString, redis_port: u32, port: u32) -> Self {
        Self {
            address: addr.to_string(),
            redis_port,
            port,
        }
    }

    #[allow(clippy::inherent_to_string_shadow_display)]
    pub fn redis_addr(&self) -> String {
        format!("{}:{}", self.address, self.redis_port)
    }
    #[allow(clippy::inherent_to_string_shadow_display)]
    pub fn raft_addr(&self) -> String {
        format!("{}:{}", self.address, self.port)
    }

    pub fn addr(&self) -> &str {
        &self.address
    }

    pub fn port(&self) -> u32 {
        self.port
    }
    pub fn redis_port(&self) -> u32 {
        self.redis_port
    }

    /// Parse `1.2.3.4:5555` into `Endpoint`.
    pub fn parse(address: &str, redis_port: u32) -> Result<Self, Error> {
        let x = address.splitn(2, ':').collect::<Vec<_>>();
        if x.len() != 2 {
            return Err(Error::config(format!(
                "Failed to parse address: {}",
                address
            )));
        }
        let port = x[1].parse::<u32>().map_err(|e| {
            Error::config(format!("Failed to parse port: {}; address: {}", e, address))
        })?;
        Ok(Self::new(x[0], redis_port, port))
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.address, self.port)
    }
}
