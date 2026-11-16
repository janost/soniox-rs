use chrono::{DateTime, Utc};
use reqwest::Method;
use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use super::usage::{UsageSummaryEntry, window};
use super::{empty, json};
use crate::Client;
use crate::error::Result;

/// A permission of an API key, as `resource:level`. Write includes read.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ApiKeyPermission {
    #[serde(rename = "stt_rt:write")]
    SttRealtimeWrite,
    #[serde(rename = "stt_async:read")]
    SttAsyncRead,
    #[serde(rename = "stt_async:write")]
    SttAsyncWrite,
    #[serde(rename = "files:read")]
    FilesRead,
    #[serde(rename = "files:write")]
    FilesWrite,
    #[serde(rename = "tts_rt:write")]
    TtsRealtimeWrite,
    #[serde(rename = "voices:read")]
    VoicesRead,
    #[serde(rename = "voices:write")]
    VoicesWrite,
    #[serde(rename = "voice_agent:read")]
    VoiceAgentRead,
    #[serde(rename = "voice_agent:write")]
    VoiceAgentWrite,
    #[serde(rename = "usage:read")]
    UsageRead,
    #[serde(rename = "models:read")]
    ModelsRead,
    #[serde(rename = "temporary_keys:write")]
    TemporaryKeysWrite,
    #[serde(rename = "legacy:write")]
    LegacyWrite,
    /// A permission this crate does not know, kept verbatim so a key that
    /// holds one still lists and round-trips.
    #[serde(untagged)]
    Other(String),
}

/// The organization an API key belongs to.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct OrganizationInfo {
    pub id: String,
    pub name: String,
    pub created_at: Option<DateTime<Utc>>,
}

/// Prepaid balance and payment settings. Amounts are decimal strings in US
/// dollars.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct OrganizationBalance {
    /// What is left to spend: paid credit plus remaining free credit.
    pub available_balance_usd: String,
    pub free_balance_usd: String,
    pub used_balance_usd: String,
    /// Usage is invoiced rather than drawn from the balance.
    pub invoice_billing_enabled: bool,
    /// The balance can no longer cover requests.
    pub balance_exhausted: bool,
    pub autopay_enabled: bool,
    pub autopay_threshold_usd: Option<String>,
    pub autopay_amount_usd: Option<String>,
    pub has_payment_method: bool,
    pub has_payment_error: bool,
}

/// Rate and quota limits of one region. Rates are requests per minute, as
/// decimal strings.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct RegionLimits {
    pub region: String,
    pub transcribe_rpm: String,
    pub transcribe_concurrent: u32,
    pub tts_rpm: String,
    pub tts_concurrent: u32,
    pub transcribe_async_rpm: String,
    pub transcribe_async_concurrent: u32,
    pub transcribe_async_total_num_files: u64,
    pub transcribe_async_pending_num_files: u64,
    pub files_rpm: String,
    pub files_total_size_gb: String,
    pub files_total_count: u64,
    pub voices_rpm: String,
    pub voices_total_count: u64,
    pub webhooks_concurrent: u32,
}

/// Budget and per-region limits of the organization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct OrganizationLimits {
    pub monthly_budget_usd: String,
    pub monthly_budget_max_usd: String,
    /// Spend at which a notification goes out, when one is configured.
    pub monthly_budget_notification_usd: Option<String>,
    pub regions: Vec<RegionLimits>,
}

/// One project's share of a month's usage.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ProjectMonthlyUsage {
    pub id: String,
    pub name: String,
    pub usage_usd: String,
    pub free_usage_usd: String,
    pub invoiced_usage_usd: String,
    pub api_usage_usd: String,
    pub playground_usage_usd: String,
    /// The project's own budget, when it has one.
    pub monthly_budget_usd: Option<String>,
}

/// A calendar month of organization usage, split per project.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct MonthlyUsage {
    /// `YYYY-MM`, UTC.
    pub month: String,
    pub usage_usd: String,
    pub free_usage_usd: String,
    pub invoiced_usage_usd: String,
    pub api_usage_usd: String,
    pub playground_usage_usd: String,
    pub monthly_budget_usd: Option<String>,
    pub monthly_budget_notification_usd: Option<String>,
    /// `None` when no budget is set.
    pub remaining_budget_usd: Option<String>,
    /// `None` when no budget is set.
    pub budget_exhausted: Option<bool>,
    pub projects: Vec<ProjectMonthlyUsage>,
}

/// One project's daily usage inside an organization summary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ProjectUsageSummary {
    pub id: String,
    pub name: String,
    /// Totals across the project's models.
    pub total: UsageSummaryEntry,
    pub models: Vec<UsageSummaryEntry>,
}

/// Daily cost and activity of the organization, per project.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct OrganizationUsageSummary {
    /// Totals across every project.
    pub total: UsageSummaryEntry,
    pub projects: Vec<ProjectUsageSummary>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct Project {
    pub id: String,
    pub name: String,
    pub description: String,
    pub region: String,
    pub created_at: Option<DateTime<Utc>>,
}

/// An API key of a project, without its secret.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ProjectApiKey {
    pub id: String,
    /// Leading characters of the key, enough to recognise it in a list.
    pub key_prefix: String,
    pub name: Option<String>,
    pub permissions: Vec<ApiKeyPermission>,
    pub created_at: Option<DateTime<Utc>>,
    pub created_by: String,
    /// The API key that created this one, when a key rather than a person
    /// did.
    pub created_by_key_id: Option<String>,
}

/// A newly created API key. This is the only response that carries the
/// secret, and `{:?}` does not print it.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct CreatedProjectApiKey {
    pub id: String,
    /// Returned once, at creation, and never again.
    pub key: SecretString,
    #[serde(default)]
    pub key_prefix: String,
    pub name: Option<String>,
    #[serde(default)]
    pub permissions: Vec<ApiKeyPermission>,
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub created_by: String,
    /// The API key that created this one, when a key rather than a person
    /// did.
    pub created_by_key_id: Option<String>,
}

/// Organization administration. Get one from [`Client::organization`].
///
/// These endpoints need an organization API key. A project key is answered
/// with a permission error.
#[derive(Debug, Clone, Copy)]
pub struct Organization<'a> {
    client: &'a Client,
}

impl<'a> Organization<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    pub async fn get(&self) -> Result<OrganizationInfo> {
        json(self.client.request(Method::GET, &["organization"])?).await
    }

    pub async fn balance(&self) -> Result<OrganizationBalance> {
        json(self.client.request(Method::GET, &["organization", "balance"])?).await
    }

    pub async fn limits(&self) -> Result<OrganizationLimits> {
        json(self.client.request(Method::GET, &["organization", "limits"])?).await
    }

    /// Usage of one calendar month, given as `YYYY-MM` in UTC. `None` is the
    /// current month.
    pub async fn monthly_usage(&self, month: Option<&str>) -> Result<MonthlyUsage> {
        let request = self.client.request(Method::GET, &["organization", "usage", "monthly"])?;
        let month: Vec<(&str, &str)> = month.map(|month| ("month", month)).into_iter().collect();
        json(request.query(&month)).await
    }

    /// Daily cost and activity per project. The window may span at most 366
    /// days; `end_time` is exclusive.
    pub async fn usage_summary(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<OrganizationUsageSummary> {
        let request = self.client.request(Method::GET, &["organization", "usage", "summary"])?;
        json(request.query(&window(start_time, end_time))).await
    }

    pub async fn projects(&self) -> Result<Vec<Project>> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(default)]
            projects: Vec<Project>,
        }
        let body: Body =
            json(self.client.request(Method::GET, &["organization", "projects"])?).await?;
        Ok(body.projects)
    }

    pub async fn list_project_api_keys(&self, project_id: &str) -> Result<Vec<ProjectApiKey>> {
        #[derive(Deserialize)]
        struct Body {
            #[serde(default)]
            api_keys: Vec<ProjectApiKey>,
        }
        let body: Body = json(self.client.request(Method::GET, &api_keys(project_id))?).await?;
        Ok(body.api_keys)
    }

    pub async fn create_project_api_key(
        &self,
        project_id: &str,
        name: &str,
        permissions: &[ApiKeyPermission],
    ) -> Result<CreatedProjectApiKey> {
        #[derive(Serialize)]
        struct Body<'b> {
            name: &'b str,
            permissions: &'b [ApiKeyPermission],
        }
        let request = self.client.request(Method::POST, &api_keys(project_id))?;
        json(request.json(&Body { name, permissions })).await
    }

    /// Revoke a key. Requests made with it fail from then on.
    pub async fn delete_project_api_key(&self, project_id: &str, key_id: &str) -> Result<()> {
        let segments = ["organization", "projects", project_id, "api-keys", key_id];
        empty(self.client.request(Method::DELETE, &segments)?).await
    }
}

fn api_keys(project_id: &str) -> [&str; 4] {
    ["organization", "projects", project_id, "api-keys"]
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    #[test]
    fn permissions_round_trip_including_one_this_crate_does_not_know() {
        let parsed: Vec<ApiKeyPermission> =
            serde_json::from_str(r#"["stt_rt:write","files:read","agents:write"]"#).unwrap();
        assert_eq!(
            parsed,
            [
                ApiKeyPermission::SttRealtimeWrite,
                ApiKeyPermission::FilesRead,
                ApiKeyPermission::Other("agents:write".into()),
            ]
        );
        assert_eq!(
            serde_json::to_string(&parsed).unwrap(),
            r#"["stt_rt:write","files:read","agents:write"]"#
        );
    }

    #[test]
    fn a_created_key_decodes_and_keeps_its_secret_out_of_debug_output() {
        let key: CreatedProjectApiKey = serde_json::from_str(
            r#"{"id":"k-1","key":"sk-live-abcdef","key_prefix":"sk-live-ab","name":"ci",
                "permissions":["stt_async:write"],"created_at":"2026-11-03T10:00:00Z",
                "created_by":"someone@example.test","created_by_key_id":"k-0"}"#,
        )
        .unwrap();
        assert_eq!(key.key.expose_secret(), "sk-live-abcdef");
        assert_eq!(key.permissions, [ApiKeyPermission::SttAsyncWrite]);
        assert_eq!(key.created_by_key_id.as_deref(), Some("k-0"));
        assert!(!format!("{key:?}").contains("abcdef"));
    }

    #[test]
    fn a_balance_distinguishes_autopay_off_from_a_zero_threshold() {
        let balance: OrganizationBalance = serde_json::from_str(
            r#"{"available_balance_usd":"12.50","free_balance_usd":"0","used_balance_usd":"87.50",
                "invoice_billing_enabled":false,"balance_exhausted":false,"autopay_enabled":false,
                "autopay_threshold_usd":null,"autopay_amount_usd":null,
                "has_payment_method":true,"has_payment_error":false}"#,
        )
        .unwrap();
        assert_eq!(balance.available_balance_usd, "12.50");
        assert_eq!(balance.autopay_threshold_usd, None);
        assert!(balance.has_payment_method);
    }
}
