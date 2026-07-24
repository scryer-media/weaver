use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub(crate) trait ScriptedStep: Send + 'static {
    fn expected_prefix(&self) -> Option<&str>;
    fn response(&self) -> &[u8];

    fn delay(&self) -> Duration {
        Duration::ZERO
    }
}

pub(crate) async fn read_command_line(socket: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        let n = socket.read(&mut byte).await.unwrap();
        assert!(n > 0, "client closed connection before command completed");
        buf.push(byte[0]);
        if byte[0] == b'\n' {
            return String::from_utf8(buf).unwrap();
        }
    }
}

pub(crate) async fn spawn_scripted_server<S>(steps: Vec<S>, hold_open_after_last: Duration) -> u16
where
    S: ScriptedStep,
{
    spawn_scripted_server_with_handle(steps, hold_open_after_last)
        .await
        .0
}

async fn spawn_scripted_server_with_handle<S>(
    steps: Vec<S>,
    hold_open_after_last: Duration,
) -> (u16, JoinHandle<()>)
where
    S: ScriptedStep,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        for step in steps {
            if let Some(prefix) = step.expected_prefix() {
                let line = read_command_line(&mut socket).await;
                assert!(
                    line.starts_with(prefix),
                    "expected command starting with {prefix:?}, got {line:?}"
                );
            }
            let delay = step.delay();
            if delay > Duration::ZERO {
                tokio::time::sleep(delay).await;
            }
            if !step.response().is_empty() {
                socket.write_all(step.response()).await.unwrap();
                socket.flush().await.unwrap();
            }
        }
        if hold_open_after_last > Duration::ZERO {
            tokio::time::sleep(hold_open_after_last).await;
        }
    });
    (port, task)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Instant;

    struct Step {
        prefix: Option<&'static str>,
        response: &'static [u8],
        delay: Duration,
    }

    impl ScriptedStep for Step {
        fn expected_prefix(&self) -> Option<&str> {
            self.prefix
        }

        fn response(&self) -> &[u8] {
            self.response
        }

        fn delay(&self) -> Duration {
            self.delay
        }
    }

    #[tokio::test]
    async fn reads_split_commands_and_applies_response_delay() {
        let delay = Duration::from_millis(20);
        let (port, task) = spawn_scripted_server_with_handle(
            vec![Step {
                prefix: Some("BODY "),
                response: b"222 follows\r\n",
                delay,
            }],
            Duration::ZERO,
        )
        .await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        client.write_all(b"BO").await.unwrap();
        client.write_all(b"DY <id>\r\n").await.unwrap();

        let started = Instant::now();
        let mut response = [0u8; 13];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"222 follows\r\n");
        assert!(started.elapsed() >= delay);
        task.await.unwrap();
    }

    #[tokio::test]
    async fn reports_the_unexpected_command() {
        let (port, task) = spawn_scripted_server_with_handle(
            vec![Step {
                prefix: Some("GROUP "),
                response: b"",
                delay: Duration::ZERO,
            }],
            Duration::ZERO,
        )
        .await;
        let mut client = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        client.write_all(b"BODY <id>\r\n").await.unwrap();

        let panic = task.await.expect_err("unexpected command should panic");
        let message = if let Some(message) = panic.into_panic().downcast_ref::<String>() {
            message.clone()
        } else {
            String::new()
        };
        assert!(message.contains("expected command starting with \"GROUP \""));
        assert!(message.contains("BODY <id>"));
    }
}
