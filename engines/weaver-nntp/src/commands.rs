use bytes::{Bytes, BytesMut};

use crate::types::ArticleId;

/// Write an article identifier as its wire argument.
///
/// RFC 3977 requires a message-id argument to be the *bracketed* form. An
/// unbracketed argument is a legal article *number* reference instead, so a
/// server that has a group selected answers 430 for every article rather than
/// rejecting the command — a silent, total download failure. Callers construct
/// `ArticleId::MessageId` from many places (some of which strip the brackets
/// during NZB parsing), so the encoder is the one place that can guarantee the
/// frame is right. Bracketing here is idempotent: an already-bracketed id is
/// written verbatim.
fn encode_article_id(buf: &mut BytesMut, id: &ArticleId) {
    match id {
        ArticleId::MessageId(message_id) => {
            if message_id.starts_with('<') && message_id.ends_with('>') {
                buf.extend_from_slice(message_id.as_bytes());
            } else {
                buf.extend_from_slice(b"<");
                buf.extend_from_slice(message_id.as_bytes());
                buf.extend_from_slice(b">");
            }
        }
        ArticleId::Number(number) => {
            buf.extend_from_slice(number.to_string().as_bytes());
        }
    }
}

/// An NNTP command to be sent to the server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    /// Retrieve a complete article (headers + body).
    Article(ArticleId),
    /// Retrieve only the article body.
    Body(ArticleId),
    /// Retrieve only the article headers.
    Head(ArticleId),
    /// Check if an article exists (returns status only, no data).
    Stat(ArticleId),
    /// Select a newsgroup.
    Group(String),
    /// Request the server's capability list.
    Capabilities,
    /// Send username for authentication (RFC 4643).
    AuthInfoUser(String),
    /// Send password for authentication (RFC 4643).
    AuthInfoPass(String),
    /// Request STARTTLS upgrade.
    StartTls,
    /// Switch to reader mode.
    ModeReader,
    /// Quit the session.
    Quit,
    /// Request the server's current date and time (RFC 3977 DATE).
    /// Lightweight command suitable for health probes.
    Date,
}

impl Command {
    /// Encode the command into its wire format (including trailing CRLF).
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(128);
        match self {
            Command::Article(id) => {
                buf.extend_from_slice(b"ARTICLE ");
                encode_article_id(&mut buf, id);
            }
            Command::Body(id) => {
                buf.extend_from_slice(b"BODY ");
                encode_article_id(&mut buf, id);
            }
            Command::Head(id) => {
                buf.extend_from_slice(b"HEAD ");
                encode_article_id(&mut buf, id);
            }
            Command::Stat(id) => {
                buf.extend_from_slice(b"STAT ");
                encode_article_id(&mut buf, id);
            }
            Command::Group(name) => {
                buf.extend_from_slice(b"GROUP ");
                buf.extend_from_slice(name.as_bytes());
            }
            Command::Capabilities => {
                buf.extend_from_slice(b"CAPABILITIES");
            }
            Command::AuthInfoUser(user) => {
                buf.extend_from_slice(b"AUTHINFO USER ");
                buf.extend_from_slice(user.as_bytes());
            }
            Command::AuthInfoPass(pass) => {
                buf.extend_from_slice(b"AUTHINFO PASS ");
                buf.extend_from_slice(pass.as_bytes());
            }
            Command::StartTls => {
                buf.extend_from_slice(b"STARTTLS");
            }
            Command::ModeReader => {
                buf.extend_from_slice(b"MODE READER");
            }
            Command::Quit => {
                buf.extend_from_slice(b"QUIT");
            }
            Command::Date => {
                buf.extend_from_slice(b"DATE");
            }
        }
        buf.extend_from_slice(b"\r\n");
        buf.freeze()
    }

    /// Whether this command expects a multi-line response from the server.
    pub fn expects_multiline(&self) -> bool {
        matches!(
            self,
            Command::Article(_) | Command::Body(_) | Command::Head(_) | Command::Capabilities
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_body_message_id() {
        let cmd = Command::Body(ArticleId::MessageId("<test@example.com>".into()));
        assert_eq!(cmd.encode().as_ref(), b"BODY <test@example.com>\r\n");
    }

    #[test]
    fn encode_brackets_a_bare_message_id() {
        // An unbracketed argument is a legal article-number reference, so a
        // server with a group selected answers 430 instead of erroring — the
        // encoder must never emit one.
        let cmd = Command::Body(ArticleId::MessageId("test@example.com".into()));
        assert_eq!(cmd.encode().as_ref(), b"BODY <test@example.com>\r\n");
    }

    #[test]
    fn encode_bracketing_is_idempotent_across_id_commands() {
        for (bare, bracketed) in [
            (
                Command::Article(ArticleId::MessageId("a@b".into())),
                Command::Article(ArticleId::MessageId("<a@b>".into())),
            ),
            (
                Command::Head(ArticleId::MessageId("a@b".into())),
                Command::Head(ArticleId::MessageId("<a@b>".into())),
            ),
            (
                Command::Stat(ArticleId::MessageId("a@b".into())),
                Command::Stat(ArticleId::MessageId("<a@b>".into())),
            ),
        ] {
            assert_eq!(bare.encode(), bracketed.encode());
        }
    }

    #[test]
    fn encode_body_number() {
        let cmd = Command::Body(ArticleId::Number(42));
        assert_eq!(cmd.encode().as_ref(), b"BODY 42\r\n");
    }

    #[test]
    fn encode_article() {
        let cmd = Command::Article(ArticleId::MessageId("<a@b>".into()));
        assert_eq!(cmd.encode().as_ref(), b"ARTICLE <a@b>\r\n");
    }

    #[test]
    fn encode_head() {
        let cmd = Command::Head(ArticleId::Number(99));
        assert_eq!(cmd.encode().as_ref(), b"HEAD 99\r\n");
    }

    #[test]
    fn encode_stat() {
        let cmd = Command::Stat(ArticleId::MessageId("<x@y>".into()));
        assert_eq!(cmd.encode().as_ref(), b"STAT <x@y>\r\n");
    }

    #[test]
    fn encode_group() {
        let cmd = Command::Group("alt.binaries.test".into());
        assert_eq!(cmd.encode().as_ref(), b"GROUP alt.binaries.test\r\n");
    }

    #[test]
    fn encode_capabilities() {
        assert_eq!(Command::Capabilities.encode().as_ref(), b"CAPABILITIES\r\n");
    }

    #[test]
    fn encode_auth() {
        let user = Command::AuthInfoUser("myuser".into());
        assert_eq!(user.encode().as_ref(), b"AUTHINFO USER myuser\r\n");

        let pass = Command::AuthInfoPass("secret".into());
        assert_eq!(pass.encode().as_ref(), b"AUTHINFO PASS secret\r\n");
    }

    #[test]
    fn encode_starttls() {
        assert_eq!(Command::StartTls.encode().as_ref(), b"STARTTLS\r\n");
    }

    #[test]
    fn encode_mode_reader() {
        assert_eq!(Command::ModeReader.encode().as_ref(), b"MODE READER\r\n");
    }

    #[test]
    fn encode_quit() {
        assert_eq!(Command::Quit.encode().as_ref(), b"QUIT\r\n");
    }

    #[test]
    fn expects_multiline() {
        assert!(Command::Body(ArticleId::Number(1)).expects_multiline());
        assert!(Command::Article(ArticleId::Number(1)).expects_multiline());
        assert!(Command::Head(ArticleId::Number(1)).expects_multiline());
        assert!(Command::Capabilities.expects_multiline());
        assert!(!Command::Quit.expects_multiline());
        assert!(!Command::Stat(ArticleId::Number(1)).expects_multiline());
        assert!(!Command::Group("test".into()).expects_multiline());
    }
}
