use crate::error::TlsError;
use crate::node::parsed_config::ParsedConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig, version};
use std::io::Cursor;
use std::sync::Arc;
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn parse_tls_versions(
    protocol: &Option<String>,
) -> Result<Vec<&'static rustls::SupportedProtocolVersion>, TlsError> {
    let protocol = protocol.as_deref().unwrap_or("TLSv1.2 TLSv1.3");

    let mut versions = Vec::new();

    for p in protocol.split_whitespace() {
        match p {
            "TLSv1.2" => versions.push(&version::TLS12),
            "TLSv1.3" => versions.push(&version::TLS13),
            _ => {
                return Err(TlsError::InvalidConfig(format!(
                    "unsupported TLS protocol version '{}'",
                    p
                )));
            }
        }
    }

    if versions.is_empty() {
        return Err(TlsError::InvalidConfig(
            "no TLS protocol versions specified".into(),
        ));
    }

    Ok(versions)
}

fn load_cert_chain(path: &str) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let data = std::fs::read(path).map_err(|e| {
        TlsError::CertificateLoad(format!("failed to read certificate '{}': {}", path, e))
    })?;

    let mut reader = Cursor::new(data);

    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CertificateLoad(format!("failed to parse certificate: {}", e)))?;

    if certs.is_empty() {
        return Err(TlsError::CertificateLoad(
            "no valid certificate found".into(),
        ));
    }

    Ok(certs)
}

fn load_private_key(path: &str) -> Result<PrivateKeyDer<'static>, TlsError> {
    let data = std::fs::read(path).map_err(|e| {
        TlsError::PrivateKeyLoad(format!("failed to read private key '{}': {}", path, e))
    })?;

    // PKCS#8
    {
        let mut reader = Cursor::new(data.clone());

        if let Some(key) = rustls_pemfile::pkcs8_private_keys(&mut reader).next() {
            return Ok(PrivateKeyDer::from(
                key.map_err(|e| TlsError::PrivateKeyLoad(format!("{}", e)))?,
            ));
        }
    }

    // RSA PKCS#1
    {
        let mut reader = Cursor::new(data.clone());

        if let Some(key) = rustls_pemfile::rsa_private_keys(&mut reader).next() {
            return Ok(PrivateKeyDer::from(
                key.map_err(|e| TlsError::PrivateKeyLoad(format!("{}", e)))?,
            ));
        }
    }

    // EC SEC1
    {
        let mut reader = Cursor::new(data);

        if let Some(key) = rustls_pemfile::ec_private_keys(&mut reader).next() {
            return Ok(PrivateKeyDer::from(
                key.map_err(|e| TlsError::PrivateKeyLoad(format!("{}", e)))?,
            ));
        }
    }

    Err(TlsError::PrivateKeyLoad(
        "no supported private key found".into(),
    ))
}

fn load_root_store(path: &str) -> Result<RootCertStore, TlsError> {
    let certs = load_cert_chain(path).map_err(|e| TlsError::CaCertificateLoad(e.to_string()))?;

    let mut roots = RootCertStore::empty();

    for cert in certs {
        roots.add(cert).map_err(|e| {
            TlsError::CaCertificateLoad(format!("failed to add CA certificate: {}", e))
        })?;
    }

    Ok(roots)
}

fn load_identity(
    cert_file: &str,
    key_file: &str,
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError> {
    Ok((load_cert_chain(cert_file)?, load_private_key(key_file)?))
}

fn load_versions(
    config: &ParsedConfig,
) -> Result<Vec<&'static rustls::SupportedProtocolVersion>, TlsError> {
    parse_tls_versions(&config.tls_protocols)
}

pub fn load_tls_config(
    cert_file: &str,
    key_file: &str,
    config: &ParsedConfig,
) -> Result<Arc<ServerConfig>, TlsError> {
    let versions = load_versions(config)?;
    let (cert_chain, private_key) = load_identity(cert_file, key_file)?;

    let server = if config.tls_auth_clients {
        let ca = config.tls_ca_cert_file.as_ref().ok_or_else(|| {
            TlsError::InvalidConfig(
                "client authentication enabled but CA certificate missing".into(),
            )
        })?;

        let verifier = rustls::server::WebPkiClientVerifier::builder(load_root_store(ca)?.into())
            .build()
            .map_err(|e| TlsError::InvalidConfig(format!("{}", e)))?;

        ServerConfig::builder_with_protocol_versions(&versions)
            .with_client_cert_verifier(verifier)
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| TlsError::InvalidConfig(format!("{}", e)))?
    } else {
        ServerConfig::builder_with_protocol_versions(&versions)
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|e| TlsError::InvalidConfig(format!("{}", e)))?
    };

    Ok(Arc::new(server))
}

pub fn load_client_tls_config(
    cert_file: &str,
    key_file: &str,
    config: &ParsedConfig,
) -> Result<Arc<ClientConfig>, TlsError> {
    let versions = load_versions(config)?;
    let (cert_chain, private_key) = load_identity(cert_file, key_file)?;

    let ca = config
        .tls_ca_cert_file
        .as_ref()
        .ok_or_else(|| TlsError::InvalidConfig("tls-ca-cert-file is required".into()))?;

    let client = ClientConfig::builder_with_protocol_versions(&versions)
        .with_root_certificates(load_root_store(ca)?)
        .with_client_auth_cert(cert_chain, private_key)
        .map_err(|e| TlsError::InvalidConfig(format!("{}", e)))?;

    Ok(Arc::new(client))
}

pub struct TlsContext {
    server: Option<Arc<ServerConfig>>,
    client: Option<Arc<ClientConfig>>,
    //server-server
    tls_replication: bool,
}

impl TlsContext {
    pub fn load(config: &ParsedConfig) -> Result<Self, TlsError> {
        let (cert_file, key_file) = match (
            config.tls_cert_file.as_deref(),
            config.tls_key_file.as_deref(),
        ) {
            (Some(cert), Some(key)) => (cert, key),

            (None, None) => {
                if config.tls_port.is_some() || config.tls_replication {
                    return Err(TlsError::InvalidConfig(
                        "TLS is enabled but tls-cert-file and tls-key-file are missing".into(),
                    ));
                }
                return Ok(Self {
                    server: None,
                    client: None,
                    tls_replication: config.tls_replication,
                });
            }

            _ => {
                return Err(TlsError::InvalidConfig(
                    "tls-cert-file and tls-key-file must both be specified".into(),
                ));
            }
        };

        let server = load_tls_config(cert_file, key_file, config)?;
        // A client configuration is only needed for Raft replication.  Redis
        // client TLS uses the server configuration above; requiring a CA and
        // client certificate in that mode would reject valid one-way TLS
        // configurations before the server can start.
        let client = if config.tls_replication {
            Some(load_client_tls_config(cert_file, key_file, config)?)
        } else {
            None
        };

        Ok(Self {
            server: Some(server),
            client,
            tls_replication: config.tls_replication,
        })
    }

    pub fn is_enabled(&self) -> bool {
        // Redis TLS only needs a server configuration.  The client
        // configuration is optional and is built only for Raft replication.
        self.server.is_some()
    }

    pub fn server_config(&self) -> Option<Arc<ServerConfig>> {
        self.server.clone()
    }

    pub fn client_config(&self) -> Option<Arc<ClientConfig>> {
        self.client.clone()
    }

    pub fn acceptor_for_client(&self) -> Option<TlsAcceptor> {
        self.server
            .as_ref()
            .map(|cfg| TlsAcceptor::from(Arc::clone(cfg)))
    }

    pub fn acceptor_for_cluster(&self) -> Option<TlsAcceptor> {
        if !self.tls_replication {
            return None;
        }
        self.server
            .as_ref()
            .map(|cfg| TlsAcceptor::from(Arc::clone(cfg)))
    }

    pub fn connector_for_cluster(&self) -> Option<TlsConnector> {
        if !self.tls_replication {
            return None;
        }
        self.client
            .as_ref()
            .map(|cfg| TlsConnector::from(Arc::clone(cfg)))
    }
}

#[cfg(test)]
mod tests {
    use super::TlsContext;
    use crate::cfg::config::Config;
    use crate::node::parsed_config::ParsedConfig;

    fn parsed_config(tls_port: Option<u32>, tls_replication: bool) -> ParsedConfig {
        let mut config = Config::default();
        config.redis.tls_port = tls_port;
        config.tls.tls_replication = Some(tls_replication);
        ParsedConfig::from(&config).expect("default configuration should parse")
    }

    #[test]
    fn redis_tls_requires_server_identity() {
        let config = parsed_config(Some(6479), false);
        let error = match TlsContext::load(&config) {
            Ok(_) => panic!("TLS listener without cert must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("tls-cert-file and tls-key-file"));
    }

    #[test]
    fn replication_tls_requires_server_identity() {
        let config = parsed_config(None, true);
        let error = match TlsContext::load(&config) {
            Ok(_) => panic!("TLS replication without cert must fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("tls-cert-file and tls-key-file"));
    }

    #[test]
    fn tls_disabled_without_identity_is_allowed() {
        let config = parsed_config(None, false);
        let context = TlsContext::load(&config).expect("TLS disabled should not need certs");

        assert!(!context.is_enabled());
        assert!(context.acceptor_for_client().is_none());
        assert!(context.client_config().is_none());
        assert!(context.connector_for_cluster().is_none());
    }
}
