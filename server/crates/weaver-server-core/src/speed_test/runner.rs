use tokio::sync::watch;

// When a run's work must end: the operator cancelled, the run decided it has
// measured enough, or the measurement reached its maximum duration.
#[derive(Clone)]
pub(crate) struct Stop {
    cancel: watch::Receiver<bool>,
    halt: Option<watch::Receiver<bool>>,
    deadline: Option<tokio::time::Instant>,
}

impl Stop {
    pub(crate) fn new(cancel: watch::Receiver<bool>) -> Self {
        Self {
            cancel,
            halt: None,
            deadline: None,
        }
    }

    pub(crate) fn with_deadline(&self, deadline: tokio::time::Instant) -> Self {
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
            || self
                .deadline
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
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
                Some(deadline) => tokio::time::sleep_until(deadline).await,
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
