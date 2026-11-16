use chrono::{DateTime, Utc};
use futures_util::stream::BoxStream;
use reqwest::Method;
use reqwest::multipart::Form;
use serde::{Deserialize, Serialize};

use super::{Count, Page, Upload, empty, json, page_query, paginate};
use crate::Client;
use crate::error::Result;
use crate::types::VoiceId;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VoiceGender {
    Male,
    Female,
    Neutral,
    #[default]
    #[serde(other)]
    Unknown,
}

/// The perceived age of a voice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VoiceAge {
    Young,
    MiddleAged,
    Old,
    #[default]
    #[serde(other)]
    Unknown,
}

/// Whether a cloned voice can be used with one model yet.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum VoiceModelStatus {
    NotComputed,
    Processing,
    /// The only status in which the voice works with the model.
    Ready,
    Failed,
    #[default]
    #[serde(other)]
    Unknown,
}

/// The readiness of a cloned voice for one model.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct VoiceModel {
    pub model: String,
    pub status: VoiceModelStatus,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
}

/// A voice cloned from a reference audio clip.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[non_exhaustive]
pub struct Voice {
    pub id: VoiceId,
    /// Unique within the project.
    #[serde(default)]
    pub name: String,
    /// File name of the uploaded reference clip.
    #[serde(default)]
    pub filename: String,
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub models: Vec<VoiceModel>,
}

impl Voice {
    /// Whether the voice can be used with `model` right now.
    pub fn is_ready_for(&self, model: &str) -> bool {
        self.models.iter().any(|m| m.model == model && m.status == VoiceModelStatus::Ready)
    }
}

/// A voice from the shared catalogue, with its descriptive tags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct SharedVoice {
    pub id: String,
    pub description: String,
    pub gender: VoiceGender,
    pub age: VoiceAge,
    /// e.g. `american`, `british`.
    pub accent: String,
    /// What the voice suits, e.g. `narration`, `conversational`.
    pub use_case: Vec<String>,
    /// How the voice sounds, e.g. `warm`, `energetic`.
    pub style: Vec<String>,
}

/// Narrows a shared-voice listing. Every field left unset matches anything,
/// and so does an `Unknown` gender or age, which is a value the server sent
/// that this crate could not name and is not one it accepts back.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct SharedVoiceFilter {
    #[serde(skip_serializing_if = "is_unset_gender")]
    pub gender: Option<VoiceGender>,
    #[serde(skip_serializing_if = "is_unset_age")]
    pub age: Option<VoiceAge>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accent: Option<String>,
    /// A voice must carry every listed use case.
    #[serde(skip)]
    pub use_case: Vec<String>,
    /// A voice must carry every listed style.
    #[serde(skip)]
    pub style: Vec<String>,
}

fn is_unset_gender(gender: &Option<VoiceGender>) -> bool {
    matches!(gender, None | Some(VoiceGender::Unknown))
}

fn is_unset_age(age: &Option<VoiceAge>) -> bool {
    matches!(age, None | Some(VoiceAge::Unknown))
}

impl SharedVoiceFilter {
    /// The list-valued filters, as the repeated query keys the API expects.
    fn repeated(&self) -> Vec<(&'static str, &str)> {
        let use_cases = self.use_case.iter().map(|value| ("use_case", value.as_str()));
        let styles = self.style.iter().map(|value| ("style", value.as_str()));
        use_cases.chain(styles).collect()
    }
}

#[derive(Deserialize)]
struct VoicesPage<T> {
    #[serde(default = "Vec::new")]
    voices: Vec<T>,
    next_page_cursor: Option<String>,
}

impl<T> From<VoicesPage<T>> for Page<T> {
    fn from(page: VoicesPage<T>) -> Self {
        Page { items: page.voices, next_page_cursor: page.next_page_cursor }
    }
}

/// Cloned voices and the shared catalogue. Get one from [`Client::voices`].
#[derive(Debug, Clone, Copy)]
pub struct Voices<'a> {
    client: &'a Client,
}

impl<'a> Voices<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// Clone a voice from a reference clip. The voice is usable with a model
    /// once that model reports [`VoiceModelStatus::Ready`]; poll
    /// [`get`](Self::get) until then.
    pub async fn create(&self, name: &str, clip: Upload) -> Result<Voice> {
        let form = Form::new().text("name", name.to_owned()).part("file", clip.into_part().await?);
        json(self.client.upload_request(&["voices"])?.multipart(form)).await
    }

    pub async fn get(&self, id: &VoiceId) -> Result<Voice> {
        json(self.client.request(Method::GET, &["voices", id.as_str()])?).await
    }

    /// One page of the project's cloned voices. `limit` is at most 1000.
    pub async fn list(&self, limit: Option<u32>, cursor: Option<&str>) -> Result<Page<Voice>> {
        let request = self.client.request(Method::GET, &["voices"])?;
        let page: VoicesPage<Voice> = json(request.query(&page_query(limit, cursor))).await?;
        Ok(page.into())
    }

    /// Every cloned voice, fetched page by page as the stream is consumed.
    pub fn list_all(&self) -> BoxStream<'a, Result<Voice>> {
        let voices = *self;
        paginate(move |cursor| async move { voices.list(None, cursor.as_deref()).await })
    }

    pub async fn count(&self) -> Result<u64> {
        let count: Count = json(self.client.request(Method::GET, &["voices", "count"])?).await?;
        Ok(count.total)
    }

    /// Prepare a voice for a model released after it was created. `None`
    /// prepares it for every model it is not ready for yet.
    pub async fn recompute(&self, id: &VoiceId, model: Option<&str>) -> Result<Voice> {
        #[derive(Serialize)]
        struct Body<'m> {
            #[serde(skip_serializing_if = "Option::is_none")]
            model: Option<&'m str>,
        }
        let request = self.client.request(Method::POST, &["voices", id.as_str(), "recompute"])?;
        json(request.json(&Body { model })).await
    }

    pub async fn delete(&self, id: &VoiceId) -> Result<()> {
        empty(self.client.request(Method::DELETE, &["voices", id.as_str()])?).await
    }

    /// One page of the shared voices of a text-to-speech model. `limit` is at
    /// most 200. A cursor points into the filtered list, so pass the same
    /// filter with it.
    pub async fn list_shared(
        &self,
        model: &str,
        filter: &SharedVoiceFilter,
        limit: Option<u32>,
        cursor: Option<&str>,
    ) -> Result<Page<SharedVoice>> {
        let request = self
            .client
            .request(Method::GET, &["shared-voices"])?
            .query(&[("model", model)])
            .query(filter)
            .query(&filter.repeated())
            .query(&page_query(limit, cursor));
        let page: VoicesPage<SharedVoice> = json(request).await?;
        Ok(page.into())
    }

    /// Every shared voice matching the filter.
    pub fn list_all_shared(
        &self,
        model: &'a str,
        filter: &'a SharedVoiceFilter,
    ) -> BoxStream<'a, Result<SharedVoice>> {
        let voices = *self;
        paginate(move |cursor| async move {
            voices.list_shared(model, filter, None, cursor.as_deref()).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_voice_is_ready_for_a_model_only_once_that_model_says_so() {
        let voice: Voice = serde_json::from_str(
            r#"{"id":"v-1","name":"Narrator","filename":"clip.wav","created_at":"2026-10-02T08:00:00Z",
                "models":[{"model":"tts-rt-v2","status":"ready","error_type":null,"error_message":null},
                          {"model":"tts-rt-v3","status":"processing"},
                          {"model":"tts-rt-v4","status":"something_new"}]}"#,
        )
        .unwrap();
        assert!(voice.is_ready_for("tts-rt-v2"));
        assert!(!voice.is_ready_for("tts-rt-v3"));
        assert!(!voice.is_ready_for("tts-rt-v9"));
        assert_eq!(voice.models[2].status, VoiceModelStatus::Unknown);
    }

    #[test]
    fn a_shared_voice_decodes_with_its_tags() {
        let voice: SharedVoice = serde_json::from_str(
            r#"{"id":"Maya","description":"Warm narrator","gender":"female","age":"middle_aged",
                "accent":"british","use_case":["narration"],"style":["warm","calm"]}"#,
        )
        .unwrap();
        assert_eq!(voice.gender, VoiceGender::Female);
        assert_eq!(voice.age, VoiceAge::MiddleAged);
        assert_eq!(voice.style, ["warm", "calm"]);
    }

    #[test]
    fn list_filters_repeat_their_query_key_once_per_value() {
        let filter = SharedVoiceFilter {
            use_case: vec!["narration".into(), "ads".into()],
            style: vec!["warm".into()],
            ..Default::default()
        };
        assert_eq!(
            filter.repeated(),
            [("use_case", "narration"), ("use_case", "ads"), ("style", "warm")]
        );
    }
}
