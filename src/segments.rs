//! Turning a token stream into larger units: segments (a run of tokens by one
//! speaker in one language) and utterances (everything between two
//! endpoints). Works on realtime events and on asynchronous transcripts
//! alike.
//!
//! Only final tokens are buffered. Non-final tokens are provisional and are
//! resent in full with every event, so accumulating them would duplicate
//! text.

use crate::realtime::stt::Event;
use crate::types::{Marker, Token, TranslationStatus};

/// Which token attributes start a new segment when they change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupBy {
    pub speaker: bool,
    pub language: bool,
}

impl Default for GroupBy {
    fn default() -> Self {
        Self { speaker: true, language: true }
    }
}

impl GroupBy {
    fn same_group(self, a: &Token, b: &Token) -> bool {
        (!self.speaker || a.speaker == b.speaker) && (!self.language || a.language == b.language)
    }
}

/// A run of consecutive tokens that belong together.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Segment {
    /// The tokens' text, concatenated.
    pub text: String,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    /// The speaker all tokens share. `None` when they differ, which can only
    /// happen when not grouping by speaker.
    pub speaker: Option<String>,
    /// The language all tokens share. `None` when they differ.
    pub language: Option<String>,
    pub tokens: Vec<Token>,
}

impl Segment {
    fn push(&mut self, token: Token) {
        if self.tokens.is_empty() {
            self.speaker.clone_from(&token.speaker);
            self.language.clone_from(&token.language);
        } else {
            if self.speaker != token.speaker {
                self.speaker = None;
            }
            if self.language != token.language {
                self.language = None;
            }
        }
        self.text.push_str(&token.text);
        self.start_ms = self.start_ms.or(token.start_ms);
        self.end_ms = token.end_ms.or(self.end_ms);
        self.tokens.push(token);
    }
}

/// Split tokens into segments wherever a grouped attribute changes. Control
/// markers are dropped.
pub fn segment_tokens(tokens: impl IntoIterator<Item = Token>, group_by: GroupBy) -> Vec<Segment> {
    let mut segments: Vec<Segment> = Vec::new();
    for token in tokens.into_iter().filter(Token::is_text) {
        let continues = segments
            .last()
            .and_then(|segment| segment.tokens.last())
            .is_some_and(|previous| group_by.same_group(previous, &token));
        if !continues {
            segments.push(Segment::default());
        }
        if let Some(segment) = segments.last_mut() {
            segment.push(token);
        }
    }
    segments
}

/// Collects final tokens from realtime events and releases each segment once
/// it can no longer grow, which is when a later segment has started.
#[derive(Debug, Clone)]
pub struct SegmentBuffer {
    tokens: Vec<Token>,
    group_by: GroupBy,
    max_tokens: usize,
}

impl Default for SegmentBuffer {
    fn default() -> Self {
        Self::new(GroupBy::default())
    }
}

impl SegmentBuffer {
    /// Without a cap, one speaker talking for hours in one language would
    /// grow the open segment without bound.
    pub const DEFAULT_MAX_TOKENS: usize = 2000;

    pub fn new(group_by: GroupBy) -> Self {
        Self { tokens: Vec::new(), group_by, max_tokens: Self::DEFAULT_MAX_TOKENS }
    }

    /// Cap the tokens held for the segment still open. When the cap is hit
    /// the oldest are dropped.
    pub fn with_max_tokens(mut self, max_tokens: usize) -> Self {
        self.max_tokens = max_tokens.max(1);
        self
    }

    /// How many tokens are waiting in segments not yet released.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Take in one event and return the segments it completed.
    pub fn add(&mut self, event: &Event) -> Vec<Segment> {
        self.tokens.extend(event.text_tokens().filter(|token| token.is_final()).cloned());
        let stable = self.stable_len(event.final_audio_proc_ms.unwrap_or(0));
        let released = segment_tokens(self.tokens.drain(..stable), self.group_by);
        if self.tokens.len() > self.max_tokens {
            let excess = self.tokens.len() - self.max_tokens;
            self.tokens.drain(..excess);
        }
        released
    }

    /// Release everything held, the open segment included.
    pub fn flush(&mut self) -> Vec<Segment> {
        segment_tokens(self.tokens.drain(..), self.group_by)
    }

    pub fn reset(&mut self) {
        self.tokens.clear();
    }

    /// How many leading tokens form segments that are complete: every segment
    /// but the last, as long as its audio is inside the finalized range.
    fn stable_len(&self, final_audio_proc_ms: u64) -> usize {
        let mut stable = 0;
        let mut segment_end = None;
        for (index, pair) in self.tokens.windows(2).enumerate() {
            segment_end = pair[0].end_ms.or(segment_end);
            if self.group_by.same_group(&pair[0], &pair[1]) {
                continue;
            }
            // `pair[0]` closes a segment and `pair[1]` opens the next.
            if segment_end.is_none_or(|end| end > final_audio_proc_ms) {
                break;
            }
            stable = index + 1;
            segment_end = None;
        }
        stable
    }
}

/// Everything said between two endpoints.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Utterance {
    pub text: String,
    pub segments: Vec<Segment>,
    pub start_ms: Option<u64>,
    pub end_ms: Option<u64>,
    /// Set when one speaker said all of it.
    pub speaker: Option<String>,
    /// Set when all of it is in one language.
    pub language: Option<String>,
    pub final_audio_proc_ms: Option<u64>,
    pub total_audio_proc_ms: Option<u64>,
}

/// Collects final tokens from realtime events and releases them as one
/// utterance each time the server marks an endpoint. The session needs
/// endpoint detection on for that to happen.
#[derive(Debug, Clone, Default)]
pub struct UtteranceBuffer {
    tokens: Vec<Token>,
    group_by: GroupBy,
    final_audio_proc_ms: Option<u64>,
    total_audio_proc_ms: Option<u64>,
}

impl UtteranceBuffer {
    pub fn new(group_by: GroupBy) -> Self {
        Self { group_by, ..Default::default() }
    }

    /// Take in one event and return the utterances it completed: one per
    /// `<end>` marker it carries, normally none or one.
    pub fn add(&mut self, event: &Event) -> Vec<Utterance> {
        self.final_audio_proc_ms = event.final_audio_proc_ms.or(self.final_audio_proc_ms);
        self.total_audio_proc_ms = event.total_audio_proc_ms.or(self.total_audio_proc_ms);
        let mut completed = Vec::new();
        for token in &event.tokens {
            match token.marker() {
                Some(Marker::Endpoint) => completed.extend(self.flush()),
                Some(Marker::Finalized) => {}
                None if token.is_final() => self.tokens.push(token.clone()),
                None => {}
            }
        }
        completed
    }

    /// Release what has been said since the last endpoint, for when the
    /// boundary comes from elsewhere: a `<fin>` marker after a manual
    /// finalize, or the end of the session. `None` when nothing was said.
    pub fn flush(&mut self) -> Option<Utterance> {
        if self.tokens.is_empty() {
            return None;
        }
        let segments = segment_tokens(self.tokens.drain(..), self.group_by);
        Some(Utterance {
            text: segments.iter().map(|segment| segment.text.as_str()).collect(),
            start_ms: segments.iter().find_map(|segment| segment.start_ms),
            end_ms: segments.iter().rev().find_map(|segment| segment.end_ms),
            speaker: common(segments.iter().map(|segment| segment.speaker.as_deref())),
            language: common(segments.iter().map(|segment| segment.language.as_deref())),
            final_audio_proc_ms: self.final_audio_proc_ms,
            total_audio_proc_ms: self.total_audio_proc_ms,
            segments,
        })
    }

    pub fn reset(&mut self) {
        self.tokens.clear();
    }
}

/// The value every item shares, if they all have one and it is the same.
fn common<'a>(mut values: impl Iterator<Item = Option<&'a str>>) -> Option<String> {
    let first = values.next()??;
    values.all(|value| value == Some(first)).then(|| first.to_owned())
}

/// Render tokens as a readable transcript: a `Speaker N:` heading whenever
/// the speaker changes and a `[lang]` tag whenever the language does, with
/// translated runs marked as such. Control markers are skipped.
///
/// Chain final and non-final tokens to show the provisional tail too.
pub fn render_tokens<'a>(tokens: impl IntoIterator<Item = &'a Token>) -> String {
    let mut out = String::new();
    let mut speaker: Option<&str> = None;
    let mut language: Option<&str> = None;
    for token in tokens.into_iter().filter(|token| token.is_text()) {
        let mut text = token.text.as_str();
        if let Some(current) = token.speaker.as_deref()
            && speaker != Some(current)
        {
            if speaker.is_some() {
                out.push_str("\n\n");
            }
            speaker = Some(current);
            language = None;
            out.push_str("Speaker ");
            out.push_str(current);
            out.push(':');
        }
        if let Some(current) = token.language.as_deref()
            && language != Some(current)
        {
            language = Some(current);
            out.push('\n');
            if token.translation_status() == TranslationStatus::Translation {
                out.push_str("[Translation] ");
            }
            out.push('[');
            out.push_str(current);
            out.push_str("] ");
            text = text.trim_start();
        }
        out.push_str(text);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(text: &str, start: u64, end: u64, speaker: &str, language: &str) -> Token {
        Token {
            text: text.into(),
            start_ms: Some(start),
            end_ms: Some(end),
            is_final: Some(true),
            speaker: Some(speaker.into()),
            language: Some(language.into()),
            ..Default::default()
        }
    }

    fn marker(marker: Marker) -> Token {
        Token { text: marker.as_str().into(), is_final: Some(true), ..Default::default() }
    }

    fn event(tokens: Vec<Token>, final_ms: u64) -> Event {
        Event { tokens, final_audio_proc_ms: Some(final_ms), ..Default::default() }
    }

    #[test]
    fn a_segment_breaks_on_a_speaker_or_language_change() {
        let segments = segment_tokens(
            [
                token("Hel", 0, 100, "1", "en"),
                token("lo", 100, 200, "1", "en"),
                token(" Szia", 200, 400, "1", "hu"),
                token(" Hi", 400, 500, "2", "hu"),
                marker(Marker::Endpoint),
            ],
            GroupBy::default(),
        );
        let summary: Vec<_> = segments
            .iter()
            .map(|s| (s.text.as_str(), s.start_ms, s.end_ms, s.speaker.as_deref()))
            .collect();
        assert_eq!(
            summary,
            [
                ("Hello", Some(0), Some(200), Some("1")),
                (" Szia", Some(200), Some(400), Some("1")),
                (" Hi", Some(400), Some(500), Some("2")),
            ]
        );
    }

    #[test]
    fn an_ungrouped_attribute_is_reported_only_when_the_whole_segment_shares_it() {
        let by_speaker = GroupBy { speaker: true, language: false };
        let segments = segment_tokens(
            [token("Hello", 0, 200, "1", "en"), token(" szia", 200, 400, "1", "hu")],
            by_speaker,
        );
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].speaker.as_deref(), Some("1"));
        assert_eq!(segments[0].language, None, "two languages in one segment");
    }

    #[test]
    fn the_buffer_holds_the_open_segment_until_the_next_one_starts() {
        let mut buffer = SegmentBuffer::default();
        assert!(buffer.add(&event(vec![token("Hel", 0, 100, "1", "en")], 100)).is_empty());
        assert!(buffer.add(&event(vec![token("lo", 100, 200, "1", "en")], 200)).is_empty());

        let released = buffer.add(&event(vec![token(" Hi", 200, 300, "2", "en")], 300));
        assert_eq!(released.len(), 1);
        assert_eq!(released[0].text, "Hello");
        assert_eq!(buffer.len(), 1, "speaker 2 may still be talking");

        let rest = buffer.flush();
        assert_eq!(rest[0].text, " Hi");
        assert!(buffer.is_empty());
    }

    #[test]
    fn the_buffer_ignores_non_final_tokens_and_markers() {
        let mut buffer = SegmentBuffer::default();
        let provisional = Token { is_final: Some(false), ..token("Hel", 0, 100, "1", "en") };
        buffer.add(&event(vec![provisional, marker(Marker::Endpoint)], 0));
        assert!(buffer.is_empty());
    }

    #[test]
    fn a_closed_segment_waits_for_its_audio_to_be_finalized() {
        let mut buffer = SegmentBuffer::default();
        let tokens = vec![token("Hello", 0, 200, "1", "en"), token(" Hi", 200, 300, "2", "en")];
        assert!(buffer.add(&event(tokens, 150)).is_empty(), "finalized only up to 150 ms");
        assert_eq!(buffer.add(&event(vec![], 300)).len(), 1);
    }

    #[test]
    fn the_buffer_drops_its_oldest_tokens_past_the_cap() {
        let mut buffer = SegmentBuffer::default().with_max_tokens(2);
        let tokens = (0..5).map(|i| token("x", i * 10, i * 10 + 10, "1", "en")).collect();
        buffer.add(&event(tokens, 50));
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn an_endpoint_marker_releases_one_utterance() {
        let mut buffer = UtteranceBuffer::default();
        assert!(buffer.add(&event(vec![token("Hello", 0, 200, "1", "en")], 200)).is_empty());
        let done = buffer
            .add(&event(vec![token(" there", 200, 400, "1", "en"), marker(Marker::Endpoint)], 400));
        assert_eq!(done.len(), 1);
        let utterance = &done[0];
        assert_eq!(utterance.text, "Hello there");
        assert_eq!((utterance.start_ms, utterance.end_ms), (Some(0), Some(400)));
        assert_eq!(utterance.speaker.as_deref(), Some("1"));
        assert_eq!(utterance.language.as_deref(), Some("en"));
        assert_eq!(utterance.final_audio_proc_ms, Some(400));
        assert!(buffer.flush().is_none(), "nothing is left after the endpoint");
    }

    #[test]
    fn an_utterance_by_two_speakers_names_neither() {
        let mut buffer = UtteranceBuffer::default();
        buffer.add(&event(
            vec![token("Hi", 0, 100, "1", "en"), token(" Hey", 100, 200, "2", "en")],
            200,
        ));
        let utterance = buffer.flush().expect("two tokens were said");
        assert_eq!(utterance.segments.len(), 2);
        assert_eq!(utterance.speaker, None);
        assert_eq!(utterance.language.as_deref(), Some("en"));
    }

    #[test]
    fn renders_speakers_languages_and_translations() {
        let translated = Token {
            translation_status: Some(TranslationStatus::Translation),
            ..token(" Szia", 0, 0, "1", "hu")
        };
        let tokens = [
            token(" Hello", 0, 100, "1", "en"),
            translated,
            token(" Bye", 200, 300, "2", "en"),
            marker(Marker::Endpoint),
        ];
        assert_eq!(
            render_tokens(&tokens),
            "Speaker 1:\n[en] Hello\n[Translation] [hu] Szia\n\nSpeaker 2:\n[en] Bye"
        );
    }
}
