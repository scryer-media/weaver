use std::io::{BufRead, Cursor};

use quick_xml::Reader;
use quick_xml::events::Event;

use crate::error::NzbError;
use crate::types::{Nzb, NzbFile, NzbMeta, NzbSegment};

/// Parse an NZB XML document from bytes.
pub fn parse_nzb(xml: &[u8]) -> Result<Nzb, NzbError> {
    parse_nzb_reader(Cursor::new(xml))
}

/// Parse an NZB XML document from a buffered reader.
pub fn parse_nzb_reader<R: BufRead>(reader: R) -> Result<Nzb, NzbError> {
    let mut reader = Reader::from_reader(reader);
    reader.config_mut().trim_text(true);

    let mut buf = Vec::new();

    // Meta accumulation
    let mut meta_title: Option<String> = None;
    let mut meta_password: Option<String> = None;
    let mut meta_tags: Vec<(String, String)> = Vec::new();

    // File accumulation
    let mut files: Vec<NzbFile> = Vec::new();
    let mut current_file: Option<FileBuilder> = None;

    // Parser state
    let mut in_head = false;
    let mut current_meta_type: Option<String> = None;
    let mut in_groups = false;
    let mut in_segments = false;
    let mut current_segment: Option<SegmentBuilder> = None;
    let mut text_buf = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,

            Ok(Event::Start(e)) => {
                let name = e.name();
                let local = local_name(name.as_ref());
                match local {
                    b"head" => in_head = true,
                    b"meta" if in_head => {
                        current_meta_type = None;
                        for attr in e.attributes() {
                            let attr = attr.map_err(|e| NzbError::Xml(e.to_string()))?;
                            if attr.key.as_ref() == b"type" {
                                current_meta_type =
                                    Some(String::from_utf8_lossy(&attr.value).into_owned());
                            }
                        }
                        text_buf.clear();
                    }
                    b"file" => {
                        let mut poster = None;
                        let mut date = None;
                        let mut subject = None;
                        for attr in e.attributes() {
                            let attr = attr.map_err(|e| NzbError::Xml(e.to_string()))?;
                            match attr.key.as_ref() {
                                b"poster" => {
                                    poster = Some(
                                        attr.decode_and_unescape_value(reader.decoder())
                                            .map_err(|e| NzbError::Xml(e.to_string()))?
                                            .into_owned(),
                                    );
                                }
                                b"date" => {
                                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                                    date = Some(val.parse::<u64>().map_err(|_| {
                                        NzbError::InvalidValue {
                                            element: "file".into(),
                                            attribute: "date".into(),
                                            value: val,
                                        }
                                    })?);
                                }
                                b"subject" => {
                                    subject = Some(
                                        attr.decode_and_unescape_value(reader.decoder())
                                            .map_err(|e| NzbError::Xml(e.to_string()))?
                                            .into_owned(),
                                    );
                                }
                                _ => {}
                            }
                        }
                        let poster = poster.ok_or_else(|| NzbError::MissingAttribute {
                            element: "file".into(),
                            attribute: "poster".into(),
                        })?;
                        let subject = subject.ok_or_else(|| NzbError::MissingAttribute {
                            element: "file".into(),
                            attribute: "subject".into(),
                        })?;
                        current_file = Some(FileBuilder {
                            poster,
                            date: date.unwrap_or(0),
                            subject,
                            groups: Vec::new(),
                            segments: Vec::new(),
                        });
                    }
                    b"groups" if current_file.is_some() => in_groups = true,
                    b"group" if in_groups => {
                        text_buf.clear();
                    }
                    b"segments" if current_file.is_some() => in_segments = true,
                    b"segment" if in_segments => {
                        let mut bytes = None;
                        let mut number = None;
                        for attr in e.attributes() {
                            let attr = attr.map_err(|e| NzbError::Xml(e.to_string()))?;
                            match attr.key.as_ref() {
                                b"bytes" => {
                                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                                    bytes = Some(val.parse::<u32>().map_err(|_| {
                                        NzbError::InvalidValue {
                                            element: "segment".into(),
                                            attribute: "bytes".into(),
                                            value: val,
                                        }
                                    })?);
                                }
                                b"number" => {
                                    let val = String::from_utf8_lossy(&attr.value).into_owned();
                                    number = Some(val.parse::<u32>().map_err(|_| {
                                        NzbError::InvalidValue {
                                            element: "segment".into(),
                                            attribute: "number".into(),
                                            value: val,
                                        }
                                    })?);
                                }
                                _ => {}
                            }
                        }
                        current_segment = Some(SegmentBuilder { bytes, number });
                        text_buf.clear();
                    }
                    _ => {}
                }
            }

            Ok(Event::End(e)) => {
                let name = e.name();
                let local = local_name(name.as_ref());
                match local {
                    b"head" => in_head = false,
                    b"meta" if in_head => {
                        if let Some(key) = current_meta_type.take() {
                            match key.as_str() {
                                "title" => meta_title = Some(text_buf.clone()),
                                "password" => meta_password = Some(text_buf.clone()),
                                _ => meta_tags.push((key, text_buf.clone())),
                            }
                        }
                    }
                    b"group" if in_groups => {
                        if let Some(ref mut f) = current_file
                            && !text_buf.is_empty()
                        {
                            f.groups.push(text_buf.clone());
                        }
                    }
                    b"groups" => in_groups = false,
                    b"segment" if in_segments => {
                        if let Some(seg) = current_segment.take()
                            && let Some(ref mut f) = current_file
                        {
                            let number = seg.number.unwrap_or(0);
                            let bytes = seg.bytes.unwrap_or(0);
                            // Strip angle brackets from message-ID if present
                            let message_id = text_buf
                                .trim_start_matches('<')
                                .trim_end_matches('>')
                                .to_owned();
                            f.segments.push(NzbSegment {
                                number,
                                bytes,
                                message_id,
                            });
                        }
                    }
                    b"segments" => in_segments = false,
                    b"file" => {
                        if let Some(f) = current_file.take() {
                            if f.segments.is_empty() {
                                tracing::warn!(
                                    subject = %f.subject,
                                    "skipping file with no segments"
                                );
                            } else {
                                let mut segments = f.segments;
                                segments.sort_by_key(|s| s.number);
                                files.push(NzbFile {
                                    poster: f.poster,
                                    subject: f.subject,
                                    date: f.date,
                                    groups: f.groups,
                                    segments,
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }

            Ok(Event::Text(e)) => {
                text_buf = e
                    .unescape()
                    .map_err(|e| NzbError::Xml(e.to_string()))?
                    .into_owned();
            }

            Err(e) => return Err(NzbError::Xml(e.to_string())),

            _ => {}
        }

        buf.clear();
    }

    if files.is_empty() {
        return Err(NzbError::EmptyNzb);
    }

    Ok(Nzb {
        meta: NzbMeta {
            title: meta_title,
            password: meta_password,
            tags: meta_tags,
        },
        files,
    })
}

/// Strip namespace prefix from an element name (e.g. `nzb:file` -> `file`).
fn local_name(name: &[u8]) -> &[u8] {
    match name.iter().position(|&b| b == b':') {
        Some(pos) => &name[pos + 1..],
        None => name,
    }
}

struct FileBuilder {
    poster: String,
    date: u64,
    subject: String,
    groups: Vec<String>,
    segments: Vec<NzbSegment>,
}

struct SegmentBuilder {
    bytes: Option<u32>,
    number: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use weaver_core::classify::FileRole;

    const MINIMAL_NZB: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?>
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="user@example.com" date="1234567890" subject="test file - &quot;movie.part01.rar&quot; yEnc (1/5)">
    <groups>
      <group>alt.binaries.test</group>
    </groups>
    <segments>
      <segment bytes="500000" number="1">abc123@example.com</segment>
    </segments>
  </file>
</nzb>"#;

    const FULL_NZB: &[u8] = br#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="title">Some Title</meta>
    <meta type="password">secret</meta>
    <meta type="tag">category</meta>
    <meta type="category">tv</meta>
  </head>
  <file poster="user@example.com" date="1234567890" subject="Title - [01/10] - &quot;file.rar&quot; yEnc (1/5)">
    <groups>
      <group>alt.binaries.test</group>
    </groups>
    <segments>
      <segment bytes="500000" number="1">abc123@example.com</segment>
      <segment bytes="500000" number="2">def456@example.com</segment>
    </segments>
  </file>
  <file poster="other@example.com" date="1234567891" subject="Title - [02/10] - &quot;file.r00&quot; yEnc (1/3)">
    <groups>
      <group>alt.binaries.test</group>
      <group>alt.binaries.misc</group>
    </groups>
    <segments>
      <segment bytes="300000" number="1">ghi789@example.com</segment>
    </segments>
  </file>
</nzb>"#;

    // 1. Parse a minimal valid NZB (1 file, 1 segment)
    #[test]
    fn parse_minimal_nzb() {
        let nzb = parse_nzb(MINIMAL_NZB).unwrap();
        assert_eq!(nzb.files.len(), 1);

        let file = &nzb.files[0];
        assert_eq!(file.poster, "user@example.com");
        assert_eq!(file.date, 1234567890);
        assert_eq!(file.groups, vec!["alt.binaries.test"]);
        assert_eq!(file.segments.len(), 1);
        assert_eq!(file.segments[0].number, 1);
        assert_eq!(file.segments[0].bytes, 500000);
        assert_eq!(file.segments[0].message_id, "abc123@example.com");
    }

    // 2. Parse with full metadata (title, password, tags)
    #[test]
    fn parse_full_metadata() {
        let nzb = parse_nzb(FULL_NZB).unwrap();
        assert_eq!(nzb.meta.title.as_deref(), Some("Some Title"));
        assert_eq!(nzb.meta.password.as_deref(), Some("secret"));
        assert_eq!(nzb.meta.tags.len(), 2);
        assert_eq!(
            nzb.meta.tags[0],
            ("tag".to_string(), "category".to_string())
        );
        assert_eq!(nzb.meta.tags[1], ("category".to_string(), "tv".to_string()));
    }

    // 3. Parse multi-file NZB with multiple groups
    #[test]
    fn parse_multi_file_multi_group() {
        let nzb = parse_nzb(FULL_NZB).unwrap();
        assert_eq!(nzb.files.len(), 2);

        let f0 = &nzb.files[0];
        assert_eq!(f0.groups, vec!["alt.binaries.test"]);
        assert_eq!(f0.segments.len(), 2);

        let f1 = &nzb.files[1];
        assert_eq!(f1.groups, vec!["alt.binaries.test", "alt.binaries.misc"]);
        assert_eq!(f1.segments.len(), 1);
    }

    // 4. Filename extraction from various subject patterns
    #[test]
    fn filename_from_quoted_subject() {
        let file = make_file("Title - [01/10] - \"movie.part01.rar\" yEnc (1/5)");
        assert_eq!(file.filename(), Some("movie.part01.rar"));
    }

    #[test]
    fn filename_from_group_prefixed_subject() {
        let file = make_file("[GROUP] Title - \"file.nfo\" yEnc (1/1)");
        assert_eq!(file.filename(), Some("file.nfo"));
    }

    #[test]
    fn filename_from_simple_subject() {
        let file = make_file("simple.txt (1/1)");
        assert_eq!(file.filename(), Some("simple.txt"));
    }

    // 5. File role inference
    #[test]
    fn role_rar_volume() {
        let nzb = parse_nzb(FULL_NZB).unwrap();
        let role = nzb.files[0].role();
        assert_eq!(role, FileRole::RarVolume { volume_number: 0 });
    }

    #[test]
    fn role_par2_file() {
        let file = make_file("\"data.par2\" yEnc (1/1)");
        assert_eq!(
            file.role(),
            FileRole::Par2 {
                is_index: true,
                recovery_block_count: 0
            }
        );
    }

    #[test]
    fn role_par2_recovery() {
        let file = make_file("\"data.vol00+05.par2\" yEnc (1/10)");
        assert_eq!(
            file.role(),
            FileRole::Par2 {
                is_index: false,
                recovery_block_count: 5
            }
        );
    }

    #[test]
    fn role_standalone() {
        let file = make_file("\"info.nfo\" yEnc (1/1)");
        assert_eq!(file.role(), FileRole::Standalone);
    }

    // 6. total_bytes computation
    #[test]
    fn total_bytes_computation() {
        let nzb = parse_nzb(FULL_NZB).unwrap();
        // file 0: 500000 + 500000 = 1_000_000
        assert_eq!(nzb.files[0].total_bytes(), 1_000_000);
        // file 1: 300000
        assert_eq!(nzb.files[1].total_bytes(), 300_000);
    }

    // 7. Segments sorted by number after parse
    #[test]
    fn segments_sorted_by_number() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0" subject="s">
    <groups><group>g</group></groups>
    <segments>
      <segment bytes="100" number="3">c@host</segment>
      <segment bytes="100" number="1">a@host</segment>
      <segment bytes="100" number="2">b@host</segment>
    </segments>
  </file>
</nzb>"#;
        let nzb = parse_nzb(xml).unwrap();
        let segs = &nzb.files[0].segments;
        assert_eq!(segs[0].number, 1);
        assert_eq!(segs[0].message_id, "a@host");
        assert_eq!(segs[1].number, 2);
        assert_eq!(segs[1].message_id, "b@host");
        assert_eq!(segs[2].number, 3);
        assert_eq!(segs[2].message_id, "c@host");
    }

    // 8. Empty NZB returns error
    #[test]
    fn empty_nzb_returns_error() {
        let xml = br#"<?xml version="1.0"?><nzb></nzb>"#;
        let err = parse_nzb(xml).unwrap_err();
        assert!(matches!(err, NzbError::EmptyNzb));
    }

    // 9. Missing attributes handled gracefully
    #[test]
    fn missing_poster_attribute() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file date="0" subject="s">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">id</segment></segments>
  </file>
</nzb>"#;
        let err = parse_nzb(xml).unwrap_err();
        assert!(matches!(
            err,
            NzbError::MissingAttribute {
                ref element,
                ref attribute,
            } if element == "file" && attribute == "poster"
        ));
    }

    #[test]
    fn missing_subject_attribute() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">id</segment></segments>
  </file>
</nzb>"#;
        let err = parse_nzb(xml).unwrap_err();
        assert!(matches!(
            err,
            NzbError::MissingAttribute {
                ref element,
                ref attribute,
            } if element == "file" && attribute == "subject"
        ));
    }

    // 10. Malformed XML returns Xml error
    #[test]
    fn malformed_xml_returns_error() {
        let xml = b"<nzb><not closed";
        let err = parse_nzb(xml).unwrap_err();
        assert!(matches!(err, NzbError::Xml(_)));
    }

    // Additional: no head section is fine
    #[test]
    fn parse_no_head_section() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0" subject="s">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">id</segment></segments>
  </file>
</nzb>"#;
        let nzb = parse_nzb(xml).unwrap();
        assert!(nzb.meta.title.is_none());
        assert!(nzb.meta.password.is_none());
        assert!(nzb.meta.tags.is_empty());
    }

    // Additional: file with no segments is skipped (not an error if other files have segments)
    #[test]
    fn file_with_no_segments_skipped() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p1" date="0" subject="empty">
    <groups><group>g</group></groups>
    <segments></segments>
  </file>
  <file poster="p2" date="0" subject="good">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">id</segment></segments>
  </file>
</nzb>"#;
        let nzb = parse_nzb(xml).unwrap();
        assert_eq!(nzb.files.len(), 1);
        assert_eq!(nzb.files[0].subject, "good");
    }

    // Additional: invalid date value
    #[test]
    fn invalid_date_value() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="notanumber" subject="s">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">id</segment></segments>
  </file>
</nzb>"#;
        let err = parse_nzb(xml).unwrap_err();
        assert!(
            matches!(err, NzbError::InvalidValue { ref element, ref attribute, .. }
            if element == "file" && attribute == "date")
        );
    }

    // Additional: namespace prefix handling
    #[test]
    fn parse_with_namespace_prefix() {
        let xml = br#"<?xml version="1.0"?>
<n:nzb xmlns:n="http://www.newzbin.com/DTD/2003/nzb">
  <n:file poster="p" date="0" subject="s">
    <n:groups><n:group>g</n:group></n:groups>
    <n:segments><n:segment bytes="1" number="1">id</n:segment></n:segments>
  </n:file>
</n:nzb>"#;
        let nzb = parse_nzb(xml).unwrap();
        assert_eq!(nzb.files.len(), 1);
    }

    // Additional: angle brackets stripped from message-ID (encoded as entities)
    #[test]
    fn message_id_angle_brackets_stripped() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0" subject="s">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">&lt;abc@host&gt;</segment></segments>
  </file>
</nzb>"#;
        let nzb = parse_nzb(xml).unwrap();
        assert_eq!(nzb.files[0].segments[0].message_id, "abc@host");
    }

    // Additional: HTML entities in subjects are decoded
    #[test]
    fn html_entities_decoded_in_subject() {
        let xml = br#"<?xml version="1.0"?>
<nzb>
  <file poster="p" date="0" subject="Title &amp; More - &quot;file.rar&quot; yEnc (1/1)">
    <groups><group>g</group></groups>
    <segments><segment bytes="1" number="1">id</segment></segments>
  </file>
</nzb>"#;
        let nzb = parse_nzb(xml).unwrap();
        assert_eq!(
            nzb.files[0].subject,
            "Title & More - \"file.rar\" yEnc (1/1)"
        );
        assert_eq!(nzb.files[0].filename(), Some("file.rar"));
    }

    /// Helper to create an NzbFile with just a subject for filename/role tests.
    fn make_file(subject: &str) -> NzbFile {
        NzbFile {
            poster: String::new(),
            subject: subject.to_string(),
            date: 0,
            groups: Vec::new(),
            segments: vec![NzbSegment {
                number: 1,
                bytes: 0,
                message_id: String::new(),
            }],
        }
    }
}
