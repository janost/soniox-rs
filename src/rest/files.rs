use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use reqwest::Method;
use reqwest::multipart::Form;
use serde::Deserialize;

use super::{Count, Page, Upload, empty, json, page_query, paginate};
use crate::Client;
use crate::error::Result;
use crate::types::FileId;

/// An uploaded file. Soniox deletes uploads 30 days after they arrive.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct File {
    pub id: FileId,
    #[serde(default)]
    pub filename: String,
    /// Size in bytes.
    #[serde(default)]
    pub size: u64,
    pub created_at: Option<DateTime<Utc>>,
    pub client_reference_id: Option<String>,
}

#[derive(Deserialize)]
struct FilesPage {
    #[serde(default)]
    files: Vec<File>,
    next_page_cursor: Option<String>,
}

/// Uploaded audio files. Get one from [`Client::files`].
#[derive(Debug, Clone, Copy)]
pub struct Files<'a> {
    client: &'a Client,
}

impl<'a> Files<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Upload audio and return the file to transcribe.
    pub async fn upload(&self, upload: Upload) -> Result<File> {
        self.send_upload(upload, None).await
    }

    /// Upload audio tagged with a tracking id, which need not be unique.
    pub async fn upload_with_reference_id(
        &self,
        upload: Upload,
        client_reference_id: &str,
    ) -> Result<File> {
        self.send_upload(upload, Some(client_reference_id)).await
    }

    async fn send_upload(&self, upload: Upload, reference_id: Option<&str>) -> Result<File> {
        let mut form = Form::new().part("file", upload.into_part().await?);
        if let Some(reference_id) = reference_id {
            form = form.text("client_reference_id", reference_id.to_owned());
        }
        json(self.client.upload_request(&["files"])?.multipart(form)).await
    }

    pub async fn get(&self, id: &FileId) -> Result<File> {
        json(self.client.request(Method::GET, &["files", id.as_str()])?).await
    }

    /// One page of files. `limit` is at most 1000.
    pub async fn list(&self, limit: Option<u32>, cursor: Option<&str>) -> Result<Page<File>> {
        let request = self.client.request(Method::GET, &["files"])?;
        let page: FilesPage = json(request.query(&page_query(limit, cursor))).await?;
        Ok(Page { items: page.files, next_page_cursor: page.next_page_cursor })
    }

    /// Every file, fetched page by page as the stream is consumed.
    pub fn list_all(&self) -> BoxStream<'a, Result<File>> {
        let files = *self;
        paginate(move |cursor| async move { files.list(None, cursor.as_deref()).await })
    }

    pub async fn count(&self) -> Result<Count> {
        json(self.client.request(Method::GET, &["files", "count"])?).await
    }

    /// Uploaded audio counts against storage, so a finished job should
    /// delete its file rather than leave it for the 30-day expiry.
    pub async fn delete(&self, id: &FileId) -> Result<()> {
        empty(self.client.request(Method::DELETE, &["files", id.as_str()])?).await
    }
}
