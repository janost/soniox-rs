//! The REST API: files, asynchronous transcriptions, models, text-to-speech,
//! voices, temporary keys, usage and organization administration.
//!
//! Each resource is a handle borrowed from the [`Client`](crate::Client):
//! `client.files()`, `client.transcriptions()` and so on.
//!
//! Response types decode leniently: only identifying fields are required, and
//! the rest default when absent. A field the server stops sending, or one a
//! proxy strips, does not turn a successful call into a decode error.

mod auth;
mod files;
mod models;
mod organization;
mod transcriptions;
mod tts;
mod usage;
mod voices;

use std::future::Future;
use std::path::PathBuf;

use bytes::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use reqwest::multipart::Part;
use serde::Deserialize;
use serde::de::DeserializeOwned;

pub use auth::{Auth, TemporaryApiKey, TemporaryApiKeyRequest, TemporaryApiKeyUsage};
pub use files::{File, Files};
pub use models::{
    Language, Model, Models, TranscriptionMode, TranslationTarget, TtsModel, TtsVoice,
};
pub use organization::{
    ApiKeyPermission, CreatedProjectApiKey, MonthlyUsage, Organization, OrganizationBalance,
    OrganizationInfo, OrganizationLimits, OrganizationUsageSummary, Project, ProjectApiKey,
    ProjectMonthlyUsage, ProjectUsageSummary, RegionLimits,
};
pub use transcriptions::{
    AudioSource, CreateTranscription, DEFAULT_ASYNC_MODEL, Transcript, Transcription,
    TranscriptionStatus, Transcriptions, WaitOptions,
};
pub use tts::{Tts, TtsRequest};
pub use usage::{
    ConcurrencyCounts, ConcurrencyLimitValues, ConcurrencyLimits, ConcurrencyScope,
    ConcurrentStreamsHistory, ConcurrentStreamsPeriod, StreamKind, StreamsHistoryEntry, Usage,
    UsageLogEntry, UsageLogsQuery, UsageLogsSort, UsageSummary, UsageSummaryEntry,
};
pub use voices::{
    SharedVoice, SharedVoiceFilter, Voice, VoiceAge, VoiceGender, VoiceModel, VoiceModelStatus,
    Voices,
};

use crate::error::{ApiError, Error, Result};

/// One page of a listing.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Page<T> {
    pub items: Vec<T>,
    /// Pass this as the cursor of the next call. `None` on the last page.
    pub next_page_cursor: Option<String>,
}

/// How many resources exist, split by where they were created.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct Count {
    pub total: u64,
    pub public_api: u64,
    pub playground: u64,
}

/// Audio to upload: a recording to transcribe, or a reference clip for a
/// cloned voice.
#[derive(Debug, Clone)]
pub struct Upload {
    filename: Option<String>,
    source: UploadSource,
}

#[derive(Debug, Clone)]
enum UploadSource {
    Bytes(Bytes),
    Path(PathBuf),
}

impl Upload {
    /// Upload audio already in memory. A `Vec<u8>` or `Bytes` is not copied.
    pub fn from_bytes(filename: impl Into<String>, data: impl Into<Bytes>) -> Self {
        Self { filename: Some(filename.into()), source: UploadSource::Bytes(data.into()) }
    }

    /// Upload a file from disk. It is streamed, never read into memory
    /// whole, and is named after the path unless
    /// [`with_filename`](Self::with_filename) says otherwise.
    pub fn from_path(path: impl Into<PathBuf>) -> Self {
        Self { filename: None, source: UploadSource::Path(path.into()) }
    }

    pub fn with_filename(mut self, filename: impl Into<String>) -> Self {
        self.filename = Some(filename.into());
        self
    }

    /// The multipart `file` field.
    pub(crate) async fn into_part(self) -> Result<Part> {
        let part = match self.source {
            UploadSource::Bytes(data) => {
                let length = data.len() as u64;
                Part::stream_with_length(data, length)
            }
            UploadSource::Path(path) => Part::file(&path)
                .await
                .map_err(|source| Error::Io { path: path.display().to_string(), source })?,
        };
        Ok(match self.filename {
            Some(filename) => part.file_name(filename),
            None => part,
        })
    }
}

/// Send a request. A non-2xx response becomes `Error::Api`, so callers never
/// inspect status codes.
pub(crate) async fn send(request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
    let response = request.send().await.map_err(Error::Http)?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }
    // An error body that cannot be read still leaves the status to report.
    let body = response.bytes().await.unwrap_or_default();
    Err(ApiError::from_response(status, &body).into())
}

/// Send a request and decode its JSON response.
pub(crate) async fn json<T: DeserializeOwned>(request: reqwest::RequestBuilder) -> Result<T> {
    let body = send(request).await?.bytes().await.map_err(Error::Http)?;
    Ok(serde_json::from_slice(&body)?)
}

/// Send a request whose response body carries nothing of interest.
pub(crate) async fn empty(request: reqwest::RequestBuilder) -> Result<()> {
    send(request).await.map(drop)
}

/// The `limit` and `cursor` query parameters of a listing.
pub(crate) fn page_query(limit: Option<u32>, cursor: Option<&str>) -> Vec<(&'static str, String)> {
    let mut query = Vec::with_capacity(2);
    if let Some(limit) = limit {
        query.push(("limit", limit.to_string()));
    }
    if let Some(cursor) = cursor {
        query.push(("cursor", cursor.to_owned()));
    }
    query
}

/// Walk every page of a listing as one stream of items. `fetch` is called
/// with the cursor of the page to load, `None` for the first.
pub(crate) fn paginate<'a, T, F, Fut>(mut fetch: F) -> BoxStream<'a, Result<T>>
where
    T: Send + 'a,
    F: FnMut(Option<String>) -> Fut + Send + 'a,
    Fut: Future<Output = Result<Page<T>>> + Send + 'a,
{
    // `Some(cursor)` is a page still to load; `None` means the last page has
    // been read.
    stream::try_unfold(Some(None::<String>), move |pending| {
        let page = pending.map(&mut fetch);
        async move {
            let Some(page) = page else { return Ok::<_, Error>(None) };
            let page = page.await?;
            let pending = page.next_page_cursor.map(Some);
            let items = stream::iter(page.items.into_iter().map(Ok::<T, Error>));
            Ok(Some((items, pending)))
        }
    })
    .try_flatten()
    .boxed()
}

/// A UTC instant as the API's time parameters expect it.
pub(crate) fn timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pagination_follows_cursors_until_a_page_has_none() {
        let mut asked = Vec::new();
        let items: Vec<u32> = paginate(|cursor: Option<String>| {
            asked.push(cursor.clone());
            async move {
                Ok(match cursor.as_deref() {
                    None => Page { items: vec![1, 2], next_page_cursor: Some("b".into()) },
                    Some("b") => Page { items: vec![], next_page_cursor: Some("c".into()) },
                    _ => Page { items: vec![3], next_page_cursor: None },
                })
            }
        })
        .try_collect()
        .await
        .unwrap();
        assert_eq!(items, [1, 2, 3]);
        assert_eq!(asked, [None, Some("b".to_owned()), Some("c".to_owned())]);
    }

    #[tokio::test]
    async fn pagination_stops_at_the_first_failed_page() {
        let mut calls = 0;
        let result: Result<Vec<u32>> = paginate(|_cursor| {
            calls += 1;
            let page = if calls == 1 {
                Ok(Page { items: vec![1], next_page_cursor: Some("b".into()) })
            } else {
                Err(Error::Closed)
            };
            async move { page }
        })
        .try_collect()
        .await;
        assert!(matches!(result, Err(Error::Closed)));
        assert_eq!(calls, 2);
    }

    #[test]
    fn time_parameters_are_utc_with_a_z_suffix() {
        let time = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.678+01:00").unwrap();
        assert_eq!(timestamp(time.with_timezone(&Utc)), "2026-01-02T02:04:05Z");
    }
}
