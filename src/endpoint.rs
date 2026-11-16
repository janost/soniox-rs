use std::str::FromStr;

use serde::{Deserialize, Serialize};
use url::Url;

use crate::error::{Error, Result};

/// A Soniox data-residency region. Each region has its own host family, and
/// audio never leaves the chosen region's jurisdiction. A key issued in one
/// region is rejected with a 401 by every other region's hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Region {
    #[default]
    Us,
    Eu,
    Jp,
    In,
}

impl Region {
    /// The base domain every host of the region hangs off. US has no
    /// regional subdomain.
    fn base_domain(self) -> &'static str {
        match self {
            Region::Us => "soniox.com",
            Region::Eu => "eu.soniox.com",
            Region::Jp => "jp.soniox.com",
            Region::In => "in.soniox.com",
        }
    }

    /// Websocket endpoint for realtime speech-to-text.
    pub fn stt_websocket_url(self) -> Url {
        Endpoints::from(self).stt_ws
    }

    /// Websocket endpoint for realtime text-to-speech.
    pub fn tts_websocket_url(self) -> Url {
        Endpoints::from(self).tts_ws
    }

    /// Base URL of the REST API, without the `v1` version segment.
    pub fn rest_url(self) -> Url {
        Endpoints::from(self).api
    }

    /// Base URL of the REST text-to-speech endpoint.
    pub fn tts_rest_url(self) -> Url {
        Endpoints::from(self).tts_api
    }
}

impl FromStr for Region {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "us" => Ok(Region::Us),
            "eu" => Ok(Region::Eu),
            "jp" => Ok(Region::Jp),
            "in" => Ok(Region::In),
            other => Err(Error::InvalidInput(format!("unknown Soniox region {other:?}"))),
        }
    }
}

/// The four hosts a client talks to. Built from a [`Region`] in the normal
/// case; each URL can be replaced to point at a proxy or a test server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoints {
    /// REST API base, without the `v1` segment.
    pub api: Url,
    /// Realtime speech-to-text websocket.
    pub stt_ws: Url,
    /// REST text-to-speech base.
    pub tts_api: Url,
    /// Realtime text-to-speech websocket.
    pub tts_ws: Url,
}

impl Endpoints {
    /// Derive all four URLs from a base domain, e.g. `eu.soniox.com` or the
    /// domain of a private deployment.
    pub fn for_base_domain(domain: &str) -> Result<Self> {
        let parse = |raw: String| {
            Url::parse(&raw)
                .map_err(|e| Error::InvalidInput(format!("bad base domain {domain:?}: {e}")))
        };
        Ok(Self {
            api: parse(format!("https://api.{domain}/"))?,
            stt_ws: parse(format!("wss://stt-rt.{domain}/transcribe-websocket"))?,
            tts_api: parse(format!("https://tts-rt.{domain}/"))?,
            tts_ws: parse(format!("wss://tts-rt.{domain}/tts-websocket"))?,
        })
    }

    /// The URL of a `v1` REST resource. Each segment is percent-encoded, so an
    /// id can never add a path component or a query string.
    pub(crate) fn api_v1(&self, segments: &[&str]) -> Result<Url> {
        join(&self.api, std::iter::once("v1").chain(segments.iter().copied()))
    }

    pub(crate) fn tts_generate(&self) -> Result<Url> {
        join(&self.tts_api, std::iter::once("tts"))
    }
}

impl From<Region> for Endpoints {
    fn from(region: Region) -> Self {
        Self::for_base_domain(region.base_domain()).expect("region URLs are well formed")
    }
}

/// Append path segments to a base, whether or not the base ends in `/`.
///
/// `Url::join` would instead drop the base's last segment when the slash is
/// missing (`https://proxy.test/soniox` + `v1/files` resolves to
/// `https://proxy.test/v1/files`).
fn join<'a>(base: &Url, segments: impl Iterator<Item = &'a str>) -> Result<Url> {
    let mut url = base.clone();
    url.path_segments_mut()
        .map_err(|()| Error::InvalidInput(format!("{base} cannot be a base URL")))?
        .pop_if_empty()
        .extend(segments);
    Ok(url)
}

/// Normalise a REST base given by a caller or the environment: the Python SDK
/// documents `SONIOX_API_BASE_URL` with a trailing `/v1`, the JS SDK without.
/// Both mean the same host, so the version segment is dropped here and added
/// back per request.
pub(crate) fn strip_version_segment(mut url: Url) -> Url {
    let trimmed = url.path().trim_end_matches('/');
    if let Some(prefix) = trimmed.strip_suffix("/v1") {
        let path = format!("{prefix}/");
        url.set_path(&path);
    }
    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_each_region_to_its_hosts() {
        assert_eq!(
            Region::Us.stt_websocket_url().as_str(),
            "wss://stt-rt.soniox.com/transcribe-websocket"
        );
        assert_eq!(
            Region::Eu.stt_websocket_url().as_str(),
            "wss://stt-rt.eu.soniox.com/transcribe-websocket"
        );
        assert_eq!(
            Region::Jp.tts_websocket_url().as_str(),
            "wss://tts-rt.jp.soniox.com/tts-websocket"
        );
        assert_eq!(
            Region::In.tts_websocket_url().as_str(),
            "wss://tts-rt.in.soniox.com/tts-websocket"
        );
        assert_eq!(Region::Us.rest_url().as_str(), "https://api.soniox.com/");
        assert_eq!(Region::Eu.rest_url().as_str(), "https://api.eu.soniox.com/");
        assert_eq!(Region::Jp.tts_rest_url().as_str(), "https://tts-rt.jp.soniox.com/");
    }

    #[test]
    fn defaults_to_us_and_parses_names_in_any_case() {
        assert_eq!(Region::default(), Region::Us);
        let r: Region = serde_json::from_str("\"eu\"").unwrap();
        assert_eq!(r, Region::Eu);
        assert_eq!("JP".parse::<Region>().unwrap(), Region::Jp);
        assert!("mars".parse::<Region>().is_err());
    }

    #[test]
    fn rest_paths_are_appended_with_or_without_a_trailing_slash_on_the_base() {
        let mut endpoints = Endpoints::from(Region::Us);
        assert_eq!(
            endpoints.api_v1(&["transcriptions"]).unwrap().as_str(),
            "https://api.soniox.com/v1/transcriptions"
        );
        endpoints.api = Url::parse("https://proxy.test/soniox").unwrap();
        assert_eq!(
            endpoints.api_v1(&["files"]).unwrap().as_str(),
            "https://proxy.test/soniox/v1/files"
        );
        endpoints.api = Url::parse("https://proxy.test/soniox/").unwrap();
        assert_eq!(
            endpoints.api_v1(&["files"]).unwrap().as_str(),
            "https://proxy.test/soniox/v1/files"
        );
    }

    #[test]
    fn an_id_cannot_escape_its_path_segment() {
        let endpoints = Endpoints::from(Region::Us);
        let url = endpoints.api_v1(&["files", "../transcriptions?limit=1"]).unwrap();
        assert_eq!(url.path(), "/v1/files/..%2Ftranscriptions%3Flimit=1");
        assert_eq!(url.query(), None);
    }

    #[test]
    fn a_base_that_cannot_hold_a_path_is_an_error_not_a_panic() {
        let mut endpoints = Endpoints::from(Region::Us);
        endpoints.api = Url::parse("mailto:someone@example.test").unwrap();
        assert!(matches!(endpoints.api_v1(&["files"]), Err(Error::InvalidInput(_))));
    }

    #[test]
    fn a_trailing_version_segment_on_the_rest_base_is_dropped() {
        let strip = |raw: &str| strip_version_segment(Url::parse(raw).unwrap()).to_string();
        assert_eq!(strip("https://api.soniox.com/v1"), "https://api.soniox.com/");
        assert_eq!(strip("https://api.soniox.com/v1/"), "https://api.soniox.com/");
        assert_eq!(strip("https://api.soniox.com"), "https://api.soniox.com/");
        assert_eq!(strip("https://proxy.test/soniox/v1"), "https://proxy.test/soniox/");
        assert_eq!(strip("https://proxy.test/v1x"), "https://proxy.test/v1x");
    }
}
