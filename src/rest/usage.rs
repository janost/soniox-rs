use chrono::{DateTime, NaiveDate, Utc};
use futures_util::stream::BoxStream;
use reqwest::Method;
use serde::{Deserialize, Serialize};

use super::{Page, json, paginate, timestamp};
use crate::Client;
use crate::error::Result;
use crate::types::wire_enum;

wire_enum! {
    /// Order of usage-log entries.
    pub enum UsageLogsSort {
        /// Oldest first.
        EndTimeAsc => "end_time_asc",
        /// Most recent first.
        EndTimeDesc => "end_time_desc",
    }
}

/// Which usage-log entries to list. The window filters by request end time.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct UsageLogsQuery {
    /// Start of the window, inclusive.
    pub start_time: DateTime<Utc>,
    /// End of the window, exclusive.
    pub end_time: DateTime<Utc>,
    /// Entries per page, at most 1000.
    pub limit: Option<u32>,
    pub sort: UsageLogsSort,
}

impl UsageLogsQuery {
    pub fn new(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> Self {
        Self { start_time, end_time, limit: None, sort: UsageLogsSort::EndTimeAsc }
    }

    pub fn with_limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_sort(mut self, sort: UsageLogsSort) -> Self {
        self.sort = sort;
        self
    }

    fn params(&self, cursor: Option<&str>) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("start_time", timestamp(self.start_time)),
            ("end_time", timestamp(self.end_time)),
            ("sort", self.sort.as_str().to_owned()),
        ];
        if let Some(limit) = self.limit {
            params.push(("limit", limit.to_string()));
        }
        if let Some(cursor) = cursor {
            params.push(("cursor", cursor.to_owned()));
        }
        params
    }
}

/// One API request as the usage log records it. Costs are decimal strings in
/// US dollars, as the API sends them, so no precision is lost to a float.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct UsageLogEntry {
    pub uuid: String,
    /// `api` or `playground`.
    pub request_scope: String,
    /// Empty when the request carried none.
    pub client_reference_id: String,
    pub model: String,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub input_text_tokens: u64,
    pub input_audio_tokens: u64,
    pub input_agent_text_tokens: u64,
    pub input_audio_duration_ms: u64,
    pub output_text_tokens: u64,
    pub output_audio_tokens: u64,
    pub output_audio_duration_ms: u64,
    pub cost_usd: String,
    pub input_cost_usd: String,
    pub input_text_cost_usd: String,
    pub input_audio_cost_usd: String,
    pub input_agent_text_cost_usd: String,
    pub output_cost_usd: String,
    pub output_text_cost_usd: String,
    pub output_audio_cost_usd: String,
}

/// Daily usage for one model, or for all models when `model` is `None`.
///
/// The per-day lists are aligned by index with `days`. Every day of the
/// window is present, so a day without usage is a zero rather than a gap.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct UsageSummaryEntry {
    pub model: Option<String>,
    /// UTC dates covered, ascending.
    pub days: Vec<NaiveDate>,

    pub total_cost_usd: String,
    pub total_input_cost_usd: String,
    pub total_output_cost_usd: String,
    pub total_duration_cost_usd: String,
    pub total_api_cost_usd: String,
    pub total_playground_cost_usd: String,

    pub cost_usd: Vec<String>,
    pub input_cost_usd: Vec<String>,
    pub output_cost_usd: Vec<String>,
    pub duration_cost_usd: Vec<String>,
    pub api_cost_usd: Vec<String>,
    pub playground_cost_usd: Vec<String>,

    pub total_num_requests: u64,
    pub total_input_text_tokens: u64,
    pub total_input_audio_tokens: u64,
    pub total_input_agent_text_tokens: u64,
    pub total_input_audio_duration_ms: u64,
    pub total_output_text_tokens: u64,
    pub total_output_audio_tokens: u64,
    pub total_output_audio_duration_ms: u64,
    pub total_duration_ms: u64,

    pub num_requests: Vec<u64>,
    pub input_text_tokens: Vec<u64>,
    pub input_audio_tokens: Vec<u64>,
    pub input_agent_text_tokens: Vec<u64>,
    pub input_audio_duration_ms: Vec<u64>,
    pub output_text_tokens: Vec<u64>,
    pub output_audio_tokens: Vec<u64>,
    pub output_audio_duration_ms: Vec<u64>,
    pub duration_ms: Vec<u64>,
}

/// Daily cost and activity of a project.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct UsageSummary {
    /// Totals across all models.
    pub total: UsageSummaryEntry,
    /// One entry per model that recorded usage.
    pub models: Vec<UsageSummaryEntry>,
}

/// Live counts of concurrent realtime sessions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ConcurrencyCounts {
    pub transcribe_concurrent: u32,
    pub tts_concurrent: u32,
}

/// Configured concurrency limits. `None` means unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ConcurrencyLimitValues {
    pub transcribe_concurrent: Option<u32>,
    pub tts_concurrent: Option<u32>,
}

/// Current use and limits for one scope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ConcurrencyScope {
    pub current: ConcurrencyCounts,
    pub limits: ConcurrencyLimitValues,
}

/// Concurrency of realtime sessions, for the project and its organization.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ConcurrencyLimits {
    pub project: ConcurrencyScope,
    pub organization: ConcurrencyScope,
}

/// Which kind of realtime stream a history covers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum StreamKind {
    #[default]
    Stt,
    Tts,
}

/// The aggregation period of a concurrency history. It also caps how long
/// the requested window may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConcurrentStreamsPeriod {
    Minute,
    Hour,
    Day,
}

impl ConcurrentStreamsPeriod {
    pub const fn as_secs(self) -> u32 {
        match self {
            Self::Minute => 60,
            Self::Hour => 3600,
            Self::Day => 86_400,
        }
    }
}

/// Concurrent stream counts aggregated over one period.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct StreamsHistoryEntry {
    pub period_start: Option<DateTime<Utc>>,
    pub period_sec: u32,
    /// Always 0; use `sample_max` for the peak.
    pub sample_min: u64,
    /// Peak concurrent streams in the period.
    pub sample_max: u64,
    /// Sum of recorded values. Divide by `sample_count` for the average
    /// while streams were active, or by `total_count` to count idle slots as
    /// zero.
    pub sample_sum: u64,
    pub sample_count: u64,
    /// Slots the period covers: 1 per minute, 60 per hour, 24 per day.
    pub total_count: u64,
}

/// Per-period concurrency of a project. Every period of the window is
/// present; one without activity is all zeros.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct ConcurrentStreamsHistory {
    pub kind: StreamKind,
    pub entries: Vec<StreamsHistoryEntry>,
}

#[derive(Deserialize)]
struct UsageLogsPage {
    #[serde(default)]
    usage_logs: Vec<UsageLogEntry>,
    next_page_cursor: Option<String>,
}

/// Usage and limits of the project. Get one from [`Client::usage`].
#[derive(Debug, Clone, Copy)]
pub struct Usage<'a> {
    client: &'a Client,
}

impl<'a> Usage<'a> {
    pub(crate) fn new(client: &'a Client) -> Self {
        Self { client }
    }

    /// One page of per-request usage logs.
    pub async fn logs(
        &self,
        query: &UsageLogsQuery,
        cursor: Option<&str>,
    ) -> Result<Page<UsageLogEntry>> {
        let request = self.client.request(Method::GET, &["usage-logs"])?;
        let page: UsageLogsPage = json(request.query(&query.params(cursor))).await?;
        Ok(Page { items: page.usage_logs, next_page_cursor: page.next_page_cursor })
    }

    /// Every usage-log entry of the window, fetched page by page.
    pub fn logs_all(&self, query: &'a UsageLogsQuery) -> BoxStream<'a, Result<UsageLogEntry>> {
        let usage = *self;
        paginate(move |cursor| async move { usage.logs(query, cursor.as_deref()).await })
    }

    /// Daily cost and activity, per model and in total. The window may span
    /// at most 366 days; `end_time` is exclusive.
    pub async fn summary(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<UsageSummary> {
        let request = self.client.request(Method::GET, &["usage", "summary"])?;
        json(request.query(&window(start_time, end_time))).await
    }

    /// Live counts of realtime sessions and the limits they run under.
    pub async fn concurrency_limits(&self) -> Result<ConcurrencyLimits> {
        json(self.client.request(Method::GET, &["concurrency-limits"])?).await
    }

    /// Historical concurrent stream counts, aggregated per `period`.
    pub async fn concurrent_streams_history(
        &self,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        period: ConcurrentStreamsPeriod,
        kind: StreamKind,
    ) -> Result<ConcurrentStreamsHistory> {
        let kind = match kind {
            StreamKind::Stt => "stt",
            StreamKind::Tts => "tts",
        };
        let request = self
            .client
            .request(Method::GET, &["concurrent-streams-history"])?
            .query(&window(start_time, end_time))
            .query(&[("period_sec", period.as_secs().to_string().as_str()), ("kind", kind)]);
        json(request).await
    }
}

/// The `start_time` and `end_time` query parameters.
pub(super) fn window(start: DateTime<Utc>, end: DateTime<Utc>) -> [(&'static str, String); 2] {
    [("start_time", timestamp(start)), ("end_time", timestamp(end))]
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn the_logs_query_carries_its_window_sort_and_optional_paging() {
        let start = Utc.with_ymd_and_hms(2026, 11, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 11, 2, 0, 0, 0).unwrap();
        let query = UsageLogsQuery::new(start, end);
        assert_eq!(
            query.params(None),
            [
                ("start_time", "2026-11-01T00:00:00Z".to_owned()),
                ("end_time", "2026-11-02T00:00:00Z".to_owned()),
                ("sort", "end_time_asc".to_owned()),
            ]
        );
        let query = query.with_limit(50).with_sort(UsageLogsSort::EndTimeDesc);
        let params = query.params(Some("c-2"));
        assert!(params.contains(&("sort", "end_time_desc".to_owned())));
        assert!(params.contains(&("limit", "50".to_owned())));
        assert!(params.contains(&("cursor", "c-2".to_owned())));
    }

    #[test]
    fn a_log_entry_decodes_and_fields_added_later_default() {
        let entry: UsageLogEntry = serde_json::from_str(
            r#"{"uuid":"u-1","request_scope":"api","client_reference_id":"","model":"stt-rt-v6",
                "start_time":"2026-11-01T10:00:00Z","end_time":"2026-11-01T10:05:00Z",
                "input_text_tokens":0,"input_audio_tokens":3000,"input_audio_duration_ms":300000,
                "output_text_tokens":900,"output_audio_tokens":0,"output_audio_duration_ms":0,
                "cost_usd":"0.012","input_cost_usd":"0.010","input_text_cost_usd":"0",
                "input_audio_cost_usd":"0.010","output_cost_usd":"0.002",
                "output_text_cost_usd":"0.002","output_audio_cost_usd":"0"}"#,
        )
        .unwrap();
        assert_eq!(entry.input_audio_duration_ms, 300_000);
        assert_eq!(entry.cost_usd, "0.012");
        assert_eq!(entry.input_agent_text_tokens, 0, "absent from older responses");
        assert!(entry.end_time.is_some());
    }

    #[test]
    fn a_summary_aligns_its_per_day_lists_with_its_days() {
        let summary: UsageSummary = serde_json::from_str(
            r#"{"total":{"model":null,"days":["2026-11-01","2026-11-02"],"total_cost_usd":"1.5",
                         "cost_usd":["1.0","0.5"],"total_num_requests":3,"num_requests":[2,1]},
                "models":[{"model":"stt-rt-v6","days":["2026-11-01","2026-11-02"],
                           "cost_usd":["1.0","0.5"],"num_requests":[2,1]}]}"#,
        )
        .unwrap();
        assert_eq!(summary.total.model, None);
        assert_eq!(summary.total.days.len(), summary.total.cost_usd.len());
        assert_eq!(summary.models[0].model.as_deref(), Some("stt-rt-v6"));
        assert_eq!(summary.total.num_requests, [2, 1]);
    }

    #[test]
    fn concurrency_limits_distinguish_unlimited_from_zero() {
        let limits: ConcurrencyLimits = serde_json::from_str(
            r#"{"project":{"current":{"transcribe_concurrent":2,"tts_concurrent":0},
                           "limits":{"transcribe_concurrent":10,"tts_concurrent":null}},
                "organization":{"current":{"transcribe_concurrent":5,"tts_concurrent":1},
                                "limits":{"transcribe_concurrent":null,"tts_concurrent":null}}}"#,
        )
        .unwrap();
        assert_eq!(limits.project.limits.transcribe_concurrent, Some(10));
        assert_eq!(limits.project.limits.tts_concurrent, None);
        assert_eq!(limits.organization.current.transcribe_concurrent, 5);
    }
}
