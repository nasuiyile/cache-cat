use crate::error::TlsError;
use crate::node::parsed_config::ParsedConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{RootCertStore, ServerConfig, version};
use std::sync::Arc;

fn parse_tls_versions(
    protocol: Option<&str>,
) -> Result<Vec<&'static rustls::SupportedProtocolVersion>, TlsError> {
    // Redis 默认支持 TLSv1.2 TLSv1.3
    let protocol = protocol.unwrap_or("TLSv1.2 TLSv1.3");

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
            "no TLS protocol versions specified".to_string(),
        ));
    }

    Ok(versions)
}

pub fn load_tls_config(
    cert_file: &str,
    key_file: &str,
    config: &ParsedConfig,
    protocol: Option<&str>,
) -> Result<Arc<ServerConfig>, TlsError> {
    let versions = parse_tls_versions(protocol)?;
    // 读取证书
    let certs_data = std::fs::read(cert_file).map_err(|e| {
        TlsError::CertificateLoad(format!(
            "failed to read certificate file '{}': {}",
            cert_file, e
        ))
        .into()
    })?;

    let mut cert_reader = std::io::Cursor::new(certs_data);
    let cert_chain: Vec<CertificateDer> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError::CertificateLoad(format!("failed to parse certificate: {}", e)))?
        .into_iter()
        .map(CertificateDer::from)
        .collect();

    if cert_chain.is_empty() {
        return Err(
            TlsError::CertificateLoad("no valid certificate found in file".to_string()).into(),
        );
    }

    // 读取私钥
    let key_data = std::fs::read(key_file).map_err(|e| {
        TlsError::PrivateKeyLoad(format!(
            "failed to read private key file '{}': {}",
            key_file, e
        ))
    })?;

    let mut key_reader = std::io::Cursor::new(key_data);

    // 尝试读取 PKCS#8 格式的私钥
    let private_key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .next()
        .ok_or_else(|| {
            TlsError::PrivateKeyLoad("no private key found in PKCS#8 format".to_string())
        })?
        .map_err(|e| TlsError::PrivateKeyLoad(format!("failed to parse private key: {}", e)))?;

    let key = PrivateKeyDer::from(private_key);

    // 根据是否要求客户端证书选择不同的构建方式
    let tls_config = if config.tls_auth_clients {
        if let Some(ca_cert) = &config.tls_ca_cert_file {
            let ca_data = std::fs::read(ca_cert).map_err(|e| {
                TlsError::CaCertificateLoad(format!(
                    "failed to read CA certificate file '{}': {}",
                    ca_cert, e
                ))
            })?;

            let mut ca_reader = std::io::Cursor::new(ca_data);
            let ca_certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut ca_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    TlsError::CaCertificateLoad(format!("failed to parse CA certificate: {}", e))
                })?
                .into_iter()
                .map(CertificateDer::from)
                .collect();

            if ca_certs.is_empty() {
                return Err(TlsError::CaCertificateLoad(
                    "no valid CA certificate found in file".to_string(),
                )
                .into());
            }
            // 创建 RootCertStore 并添加证书
            let mut root_store = RootCertStore::empty();
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    TlsError::CaCertificateLoad(format!("failed to add CA certificate: {}", e))
                })?;
            }

            let client_verifier = rustls::server::WebPkiClientVerifier::builder(root_store.into())
                .build()
                .map_err(|e| {
                    TlsError::InvalidConfig(format!("failed to build client verifier: {}", e))
                })?;

            ServerConfig::builder_with_protocol_versions(&versions)
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(cert_chain, key)
                .map_err(|e| {
                    TlsError::InvalidConfig(format!(
                        "failed to configure TLS with client auth: {}",
                        e
                    ))
                })?
        } else {
            // 要求客户端证书但没有提供 CA 证书
            return Err(TlsError::InvalidConfig(
                "client authentication enabled but CA certificate not provided".to_string(),
            )
            .into());
        }
    } else {
        // 不需要客户端证书
        ServerConfig::builder_with_protocol_versions(&versions)
            .with_no_client_auth()
            .with_single_cert(cert_chain, key)
            .map_err(|e| TlsError::InvalidConfig(format!("failed to configure TLS: {}", e)))?
    };

    Ok(Arc::new(tls_config))
}
