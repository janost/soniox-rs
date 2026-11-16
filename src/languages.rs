//! The languages Soniox recognises, synthesises and translates between, as
//! of the 2026-10 documentation: 60 ISO 639-1 codes with English names. For
//! the live list of one model, see [`crate::rest::Model::languages`].

/// `(code, English name)`, sorted by name.
pub const LANGUAGES: &[(&str, &str)] = &[
    ("af", "Afrikaans"),
    ("sq", "Albanian"),
    ("ar", "Arabic"),
    ("az", "Azerbaijani"),
    ("eu", "Basque"),
    ("be", "Belarusian"),
    ("bn", "Bengali"),
    ("bs", "Bosnian"),
    ("bg", "Bulgarian"),
    ("ca", "Catalan"),
    ("zh", "Chinese"),
    ("hr", "Croatian"),
    ("cs", "Czech"),
    ("da", "Danish"),
    ("nl", "Dutch"),
    ("en", "English"),
    ("et", "Estonian"),
    ("fi", "Finnish"),
    ("fr", "French"),
    ("gl", "Galician"),
    ("de", "German"),
    ("el", "Greek"),
    ("gu", "Gujarati"),
    ("he", "Hebrew"),
    ("hi", "Hindi"),
    ("hu", "Hungarian"),
    ("id", "Indonesian"),
    ("it", "Italian"),
    ("ja", "Japanese"),
    ("kn", "Kannada"),
    ("kk", "Kazakh"),
    ("ko", "Korean"),
    ("lv", "Latvian"),
    ("lt", "Lithuanian"),
    ("mk", "Macedonian"),
    ("ms", "Malay"),
    ("ml", "Malayalam"),
    ("mr", "Marathi"),
    ("no", "Norwegian"),
    ("fa", "Persian"),
    ("pl", "Polish"),
    ("pt", "Portuguese"),
    ("pa", "Punjabi"),
    ("ro", "Romanian"),
    ("ru", "Russian"),
    ("sr", "Serbian"),
    ("sk", "Slovak"),
    ("sl", "Slovenian"),
    ("es", "Spanish"),
    ("sw", "Swahili"),
    ("sv", "Swedish"),
    ("tl", "Tagalog"),
    ("ta", "Tamil"),
    ("te", "Telugu"),
    ("th", "Thai"),
    ("tr", "Turkish"),
    ("uk", "Ukrainian"),
    ("ur", "Urdu"),
    ("vi", "Vietnamese"),
    ("cy", "Welsh"),
];

/// The English name for a code, if Soniox supports it.
pub fn name_of(code: &str) -> Option<&'static str> {
    LANGUAGES.iter().find(|(c, _)| *c == code).map(|(_, n)| *n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sixty_languages_sorted_by_name_with_unique_codes() {
        assert_eq!(LANGUAGES.len(), 60);
        let names: Vec<&str> = LANGUAGES.iter().map(|(_, n)| *n).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "sorted by name");
        let mut codes: Vec<&str> = LANGUAGES.iter().map(|(c, _)| *c).collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), 60, "codes are unique");
        assert!(
            LANGUAGES
                .iter()
                .all(|(c, _)| c.len() == 2 && c.chars().all(|ch| ch.is_ascii_lowercase()))
        );
    }

    #[test]
    fn looks_a_name_up_by_code() {
        assert_eq!(name_of("hu"), Some("Hungarian"));
        assert_eq!(name_of("en"), Some("English"));
        assert_eq!(name_of("xx"), None);
    }
}
