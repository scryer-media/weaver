// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The HTTP half of a bandwidth run's upload: one bounded POST through the
// route to the provider the run pulled from, timed from the first byte sent
// to the answer.

use std::time::Instant;

use super::http::{HttpLeg, HttpOptions, RoutedClient};
use super::model::{DEFAULT_UPLOAD_BYTES, HttpUpload, MAX_UPLOAD_BYTES, UploadRequest};

// Why an upload request cannot be run, if it cannot.
pub(crate) fn validate(upload: &UploadRequest) -> Result<(), String> {
    if let Some(bytes) = upload.bytes
        && (bytes == 0 || bytes > MAX_UPLOAD_BYTES)
    {
        return Err(format!("an upload sends 1 to {MAX_UPLOAD_BYTES} bytes"));
    }
    Ok(())
}

// Post the request's body through `leg` to `url`, the provider's upload
// endpoint.
pub(crate) async fn run(
    leg: &HttpLeg,
    url: &reqwest::Url,
    upload: &UploadRequest,
    options: &HttpOptions,
) -> HttpUpload {
    let bytes = upload
        .bytes
        .unwrap_or(DEFAULT_UPLOAD_BYTES)
        .min(MAX_UPLOAD_BYTES);
    let client = match RoutedClient::connect(leg, url, options, None).await {
        Ok(client) => client,
        Err((_, error)) => {
            return HttpUpload::Failed {
                url: url.to_string(),
                error,
            };
        }
    };
    // Zeroed memory the allocator maps lazily: sending it reads the zero
    // page, so the body costs address space, not resident memory.
    let body = bytes::Bytes::from(vec![0u8; bytes as usize]);
    let started = Instant::now();
    let posted = client.post(url, body).await.map(drop);
    let elapsed = started.elapsed();
    client.close().await;
    match posted {
        Ok(()) => HttpUpload::Measured {
            url: url.to_string(),
            bytes,
            elapsed_ms: elapsed.as_millis() as u64,
            bps: bytes as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        },
        Err(error) => HttpUpload::Failed {
            url: url.to_string(),
            error,
        },
    }
}
