//! Mutual TLS: certificates, configurations, and the identity check.
//!
//! Every member node has a certificate from the cluster's own CA, with the
//! DNS name `node-<id>`. A node accepts a connection only with a client
//! certificate from that CA, and a peer message only if the certificate
//! names the node that sends it. Other CA certificates (for example
//! `admin`) can read the status and run commands.

use std::sync::Arc;

use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair, KeyUsagePurpose,
};
use rustls::RootCertStore;
use rustls::client::ClientConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ServerConfig, WebPkiClientVerifier};
use synod_core::NodeId;

use crate::NetError;

/// PEM files of one node or client.
#[derive(Clone, Debug)]
pub struct Tls {
    /// The cluster CA certificate.
    pub ca_pem: Vec<u8>,
    /// This node's certificate.
    pub cert_pem: Vec<u8>,
    /// This node's private key.
    pub key_pem: Vec<u8>,
}

/// Certificates made by [`make_certs`].
pub struct Certs {
    /// CA certificate.
    pub ca_pem: String,
    /// CA private key: keep it offline; it makes new node certificates.
    pub ca_key_pem: String,
    /// (name, certificate, key) for each node and client.
    pub issued: Vec<(String, String, String)>,
}

/// The DNS name in the certificate of a node.
pub fn node_name(id: NodeId) -> String {
    format!("node-{}", id.0)
}

/// Makes a cluster CA, and a certificate for each node (`node-<id>`) and
/// for each extra client name (for example `admin`).
pub fn make_certs(nodes: &[NodeId], clients: &[&str]) -> Result<Certs, NetError> {
    let tls = |e: rcgen::Error| NetError::Tls(e.to_string());
    let ca_key = KeyPair::generate().map_err(tls)?;
    let mut ca = CertificateParams::new(Vec::<String>::new()).map_err(tls)?;
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.distinguished_name.push(DnType::CommonName, "SynodSQL cluster CA");
    ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign, KeyUsagePurpose::DigitalSignature];
    let ca_cert = ca.self_signed(&ca_key).map_err(tls)?;
    let issuer = Issuer::new(ca, &ca_key);
    let names = nodes.iter().map(|n| node_name(*n)).chain(clients.iter().map(|c| c.to_string()));
    let mut issued = Vec::new();
    for name in names {
        let key = KeyPair::generate().map_err(tls)?;
        let mut p = CertificateParams::new(vec![name.clone()]).map_err(tls)?;
        p.distinguished_name.push(DnType::CommonName, name.clone());
        p.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
        p.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let cert = p.signed_by(&key, &issuer).map_err(tls)?;
        issued.push((name, cert.pem(), key.serialize_pem()));
    }
    Ok(Certs { ca_pem: ca_cert.pem(), ca_key_pem: ca_key.serialize_pem(), issued })
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, NetError> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| NetError::Tls(format!("certificate: {e}")))?;
    if certs.is_empty() {
        return Err(NetError::Tls("no certificate in the PEM file".into()));
    }
    Ok(certs)
}

fn key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, NetError> {
    PrivateKeyDer::from_pem_slice(pem).map_err(|e| NetError::Tls(format!("private key: {e}")))
}

fn roots(ca_pem: &[u8]) -> Result<Arc<RootCertStore>, NetError> {
    let mut roots = RootCertStore::empty();
    for c in certs(ca_pem)? {
        roots.add(c).map_err(|e| NetError::Tls(format!("CA certificate: {e}")))?;
    }
    Ok(Arc::new(roots))
}

/// The server side: HTTP/2 only, a client certificate from the CA required.
pub fn server_config(tls: &Tls) -> Result<Arc<ServerConfig>, NetError> {
    let t = |e: rustls::Error| NetError::Tls(e.to_string());
    let verifier = WebPkiClientVerifier::builder_with_provider(roots(&tls.ca_pem)?, provider())
        .build()
        .map_err(|e| NetError::Tls(e.to_string()))?;
    let mut config = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(t)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs(&tls.cert_pem)?, key(&tls.key_pem)?)
        .map_err(t)?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// The client side: HTTP/2 only, with this node's certificate.
pub fn client_config(tls: &Tls) -> Result<Arc<ClientConfig>, NetError> {
    let t = |e: rustls::Error| NetError::Tls(e.to_string());
    let mut config = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(t)?
        .with_root_certificates(roots(&tls.ca_pem)?)
        .with_client_auth_cert(certs(&tls.cert_pem)?, key(&tls.key_pem)?)
        .map_err(t)?;
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(Arc::new(config))
}

/// True if a (CA-verified) certificate names `name`.
pub fn cert_names(cert: &CertificateDer<'_>, name: &str) -> bool {
    let Ok(server_name) = ServerName::try_from(name) else { return false };
    webpki::EndEntityCert::try_from(cert).is_ok_and(|ee| ee.verify_is_valid_for_subject_name(&server_name).is_ok())
}
