use tokio::sync::watch;

// When a run's work must end: the operator cancelled, or the measurement
// reached its maximum duration.
#[derive(Clone)]
pub(crate) struct Stop {
    cancel: watch::Receiver<bool>,
    deadline: Option<tokio::time::Instant>,
}

impl Stop {
    pub(crate) fn new(cancel: watch::Receiver<bool>) -> Self {
        Self {
            cancel,
            deadline: None,
        }
    }

    pub(crate) fn with_deadline(&self, deadline: tokio::time::Instant) -> Self {
        Self {
            cancel: self.cancel.clone(),
            deadline: Some(deadline),
        }
    }

    pub(crate) fn cancelled(&self) -> bool {
        *self.cancel.borrow()
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.cancelled()
            || self
                .deadline
                .is_some_and(|deadline| deadline <= tokio::time::Instant::now())
    }

    pub(crate) async fn stopped(&mut self) {
        let deadline = self.deadline;
        let expired = async move {
            match deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            // A dropped sender means the service is gone; stop as well.
            _ = self.cancel.wait_for(|cancelled| *cancelled) => {}
            _ = expired => {}
        }
    }
}
