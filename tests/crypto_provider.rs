//! `Client::new` must install a rustls crypto provider.
//!
//! `reqwest` is configured with `rustls-no-provider` and `tokio-tungstenite`
//! selects no backend, so nothing installs one on the crate's behalf, and
//! rustls 0.23 panics rather than errors on the first TLS use without one.
//!
//! Its own integration binary, and the only test in it, on purpose: the
//! provider is process-global and several paths install it, so the same
//! assertion inside the crate's unit tests passes whether or not
//! `Client::new` does anything at all. A clean process is what makes this a
//! test of `Client` rather than of whatever ran before it.

#[test]
fn constructing_a_client_installs_the_crypto_provider() {
    assert!(
        rustls::crypto::CryptoProvider::get_default().is_none(),
        "nothing may install the provider before the client does, or this proves nothing"
    );

    let _client = soniox::Client::new("sk-test");

    assert!(
        rustls::crypto::CryptoProvider::get_default().is_some(),
        "Client::new must install a rustls crypto provider before any TLS use"
    );
}
