use tokio::sync::watch;

#[derive(Clone, Copy)]
enum Deadline {
    Monotonic(tokio::time::Instant),
    E2e(chrono::DateTime<chrono::Utc>),
}

impl Deadline {
    fn expired(self) -> bool {
        match self {
            Self::Monotonic(at) => at <= tokio::time::Instant::now(),
            Self::E2e(at) => at <= crate::e2e_clock::utc_now(),
        }
    }

    async fn wait(self) {
        match self {
            Self::Monotonic(at) => tokio::time::sleep_until(at).await,
            Self::E2e(_) => {
                while !self.expired() {
                    tokio::time::sleep(crate::e2e_clock::schedule_poll_interval()).await;
                }
            }
        }
    }
}

// When a run's work must end: the operator cancelled, the run decided it has
// measured enough, or the measurement reached its maximum duration.
#[derive(Clone)]
pub(crate) struct Stop {
    cancel: watch::Receiver<bool>,
    halt: Option<watch::Receiver<bool>>,
    deadline: Option<Deadline>,
}

impl Stop {
    pub(crate) fn new(cancel: watch::Receiver<bool>) -> Self {
        Self {
            cancel,
            halt: None,
            deadline: None,
        }
    }

    pub(crate) fn with_duration(&self, duration: std::time::Duration) -> Self {
        let deadline = if crate::e2e_clock::override_enabled() {
            Deadline::E2e(
                crate::e2e_clock::utc_now()
                    + chrono::TimeDelta::from_std(duration).expect("bounded speed-test duration"),
            )
        } else {
            Deadline::Monotonic(tokio::time::Instant::now() + duration)
        };
        Self {
            deadline: Some(deadline),
            ..self.clone()
        }
    }

    // Also stop once `halt` is set.
    pub(crate) fn with_halt(&self, halt: watch::Receiver<bool>) -> Self {
        Self {
            halt: Some(halt),
            ..self.clone()
        }
    }

    pub(crate) fn cancelled(&self) -> bool {
        *self.cancel.borrow()
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.cancelled()
            || self.halt.as_ref().is_some_and(|halt| *halt.borrow())
            || self.deadline.is_some_and(Deadline::expired)
    }

    pub(crate) async fn stopped(&mut self) {
        let Self {
            cancel,
            halt,
            deadline,
        } = self;
        let deadline = *deadline;
        let expired = async move {
            match deadline {
                Some(deadline) => deadline.wait().await,
                None => std::future::pending().await,
            }
        };
        let halted = async move {
            match halt {
                Some(halt) => {
                    let _ = halt.wait_for(|halted| *halted).await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            // A dropped sender means the service is gone; stop as well.
            _ = cancel.wait_for(|cancelled| *cancelled) => {}
            _ = halted => {}
            _ = expired => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn duration_deadline_uses_the_monotonic_clock() {
        let (_cancel, receiver) = watch::channel(false);
        let mut stop = Stop::new(receiver).with_duration(std::time::Duration::from_secs(10));
        assert!(!stop.is_stopped());
        tokio::time::advance(std::time::Duration::from_secs(9)).await;
        assert!(!stop.is_stopped());
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        stop.stopped().await;
        assert!(stop.is_stopped());
    }

    #[tokio::test]
    async fn an_article_boundary_hold_can_be_cancelled_before_its_deadline() {
        let (cancel, receiver) = watch::channel(false);
        let mut stop = Stop::new(receiver).with_duration(std::time::Duration::from_secs(600));
        cancel.send_replace(true);
        stop.stopped().await;
        assert!(stop.cancelled());
    }
}
