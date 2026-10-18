// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A self-signed certificate for loopback TLS fixtures.
pub struct LoopbackCertificate {
    pub cert_der: Vec<u8>,
    pub cert_pem: String,
    pub key_pkcs8_der: Vec<u8>,
}

// Valid for 127.0.0.1 and localhost.
pub fn loopback_certificate() -> LoopbackCertificate {
    let cert =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    LoopbackCertificate {
        cert_der: cert.cert.der().to_vec(),
        cert_pem: cert.cert.pem(),
        key_pkcs8_der: cert.signing_key.serialize_der(),
    }
}
