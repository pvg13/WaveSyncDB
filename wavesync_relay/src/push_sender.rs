//! FCM and APNs push notification sender.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Result of sending a push notification.
///
/// Replaces the prior `Error(String)` catch-all so the retry queue can
/// make policy decisions (retry vs drop) off the variant alone, without
/// parsing error message strings. Each call site in this module maps
/// deterministically to one variant.
#[derive(Debug, Clone)]
pub enum PushResult {
    /// 2xx from the provider — notification accepted.
    Sent,
    /// Provider says the token is unusable. Caller prunes it from the
    /// token store and any pending retries.
    TokenInvalid { reason: TokenInvalidReason },
    /// Retryable failure. The retry queue persists this and re-attempts
    /// with backoff; `retry_after` (if present) overrides the schedule
    /// for the next attempt.
    Transient(TransientError),
    /// Permanent non-token failure (misconfiguration, bad credentials,
    /// bad request body). Caller logs and drops — retrying won't help
    /// until config changes.
    Permanent(PermanentError),
}

/// Which provider classified the token as invalid, for logging clarity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenInvalidReason {
    /// APNs returned HTTP 410 Gone, or body said `BadDeviceToken` /
    /// `Unregistered`. The token has been recycled or the app
    /// uninstalled.
    Apns410,
    /// FCM returned `UNREGISTERED` — token revoked by Firebase.
    FcmUnregistered,
    /// FCM returned `INVALID_ARGUMENT` for the token field — malformed
    /// or wrong-project token.
    FcmInvalidArgument,
}

/// Retryable failure carrying enough info for the queue to honor
/// `Retry-After` and pick the right backoff slot.
///
/// Fields are tagged `#[allow(dead_code)]` because they are read by the
/// persistent retry queue (see commit B5) which lands separately from
/// this refactor. The compiler can't see those readers yet.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum TransientError {
    /// HTTP-level failure with a known status. `retry_after` is parsed
    /// from the `Retry-After` response header when present.
    HttpStatus {
        platform: Platform,
        status: u16,
        retry_after: Option<Duration>,
        body: String,
    },
    /// Transport-level failure — DNS, connect, TLS, timeout,
    /// EOF mid-stream. The provider never saw the request.
    Transport { platform: Platform, message: String },
    /// FCM OAuth2 token-exchange failure (network or 5xx from
    /// `oauth2.googleapis.com`). Treated transient — Google's OAuth
    /// occasionally 5xxs and recovers quickly.
    OauthTransport { message: String },
}

/// Misconfiguration that won't fix itself. Caller drops the send.
///
/// Same `#[allow(dead_code)]` reasoning as [`TransientError`].
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum PermanentError {
    /// Provider isn't configured for this platform. Won't change
    /// without restart with new config.
    NotConfigured { platform: Platform },
    /// JWT / OAuth credential construction failed (bad PEM, missing
    /// fields in service-account JSON). Operator action required.
    CredentialError { platform: Platform, message: String },
    /// Provider returned 400/401/403 (or, for FCM, 404) with a
    /// non-token-related body — malformed payload, bad APNs topic,
    /// wrong API enabled, etc.
    BadRequest {
        platform: Platform,
        status: u16,
        body: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Fcm,
    Apns,
}

/// Parse the `Retry-After` header into a Duration. Supports integer
/// seconds only (the format both FCM and APNs actually use); HTTP-date
/// form is rare in practice and we'd rather honor a missing header by
/// falling back to the backoff schedule than misparse one.
pub(crate) fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> Option<Duration> {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_secs)
}

/// Derive an APNs `apns-collapse-id` from a topic: ASCII-safe characters
/// only (alphanumeric, `-`, `_`), truncated to APNs' documented 64-byte
/// identifier limit. Standard topics (`wavesync2-<64 hex>`) keep their
/// prefix plus enough of the hash that collisions are practically
/// impossible; anything else degrades to its sanitized prefix rather than
/// risking a `BadCollapseId` rejection.
pub(crate) fn collapse_id(topic: &str) -> String {
    topic
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
        .take(64)
        .collect()
}

/// Configuration for FCM (Firebase Cloud Messaging) HTTP v1 API.
pub struct FcmConfig {
    /// Google Cloud project ID.
    pub project_id: String,
    /// Service account JSON content (for OAuth2 token generation).
    pub service_account_json: String,
}

/// Configuration for APNs (Apple Push Notification service) HTTP/2 API.
pub struct ApnsConfig {
    /// `.p8` private key PEM content.
    pub key_pem: String,
    /// Key ID from Apple Developer portal.
    pub key_id: String,
    /// Team ID from Apple Developer portal.
    pub team_id: String,
    /// App bundle ID (e.g., `com.example.myapp`).
    pub bundle_id: String,
    /// Whether to use the sandbox endpoint.
    pub sandbox: bool,
    /// Relay-operator-configured placeholder title used on ALERT-class
    /// sends (`visible: true`). This is the ONLY user-facing text that
    /// ever rides an alert push — it comes from relay operator
    /// configuration (`--apns-alert-title` / `APNS_ALERT_TITLE`), never
    /// from client-supplied content. The receiving app's own SyncNotify
    /// policy is what produces the real title/body once it wakes and
    /// syncs.
    pub alert_title: String,
}

/// Cached JWT for APNs with expiry tracking.
struct ApnsJwtCache {
    token: String,
    created_at: Instant,
}

/// Push notification sender supporting FCM and APNs.
pub struct PushSender {
    client: reqwest::Client,
    fcm: Option<FcmConfig>,
    apns: Option<ApnsConfig>,
    apns_jwt_cache: Mutex<Option<ApnsJwtCache>>,
}

impl PushSender {
    /// Create a new push sender with optional FCM and APNs configs.
    pub fn new(fcm: Option<FcmConfig>, apns: Option<ApnsConfig>) -> Self {
        Self {
            client: reqwest::Client::new(),
            fcm,
            apns,
            apns_jwt_cache: Mutex::new(None),
        }
    }

    /// Send a data-only FCM notification to a device token.
    pub async fn send_fcm(&self, token: &str, topic: &str, peer_addrs: &[String]) -> PushResult {
        let fcm = match &self.fcm {
            Some(c) => c,
            None => {
                return PushResult::Permanent(PermanentError::NotConfigured {
                    platform: Platform::Fcm,
                });
            }
        };

        let access_token = match self.get_fcm_access_token(fcm).await {
            Ok(t) => t,
            Err(e) => {
                // OAuth failures are mostly transient (Google's token
                // endpoint occasionally 5xxs). The retry budget caps us
                // if the underlying cause is actually permanent (e.g.
                // expired service account) — first 7 retries fail, then
                // the row drops with a clear log line.
                return PushResult::Transient(TransientError::OauthTransport { message: e });
            }
        };

        let url = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            fcm.project_id
        );

        // `android.priority: "high"` is required for data-only wake-up
        // messages on Android. Without it FCM treats the message as normal
        // priority, which Doze mode and App Standby can delay indefinitely
        // (or drop entirely). Use sparingly — Google rate-limits high-priority
        // data messages — but here it is exactly the case the policy is for:
        // a real, user-relevant sync needs to wake a sleeping app.
        let body = serde_json::json!({
            "message": {
                "token": token,
                "data": {
                    "type": "sync_available",
                    "topic": topic,
                    "peer_addrs": serde_json::to_string(peer_addrs).unwrap_or_default()
                },
                "android": {
                    "priority": "high"
                }
            }
        });

        match self
            .client
            .post(&url)
            .bearer_auth(&access_token)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let status_u16 = status.as_u16();
                if status.is_success() {
                    PushResult::Sent
                } else {
                    let retry_after = parse_retry_after(resp.headers());
                    let body_text = resp.text().await.unwrap_or_default();
                    if body_text.contains("UNREGISTERED") {
                        PushResult::TokenInvalid {
                            reason: TokenInvalidReason::FcmUnregistered,
                        }
                    } else if body_text.contains("INVALID_ARGUMENT") {
                        PushResult::TokenInvalid {
                            reason: TokenInvalidReason::FcmInvalidArgument,
                        }
                    } else if status_u16 == 429 || (500..600).contains(&status_u16) {
                        PushResult::Transient(TransientError::HttpStatus {
                            platform: Platform::Fcm,
                            status: status_u16,
                            retry_after,
                            body: body_text,
                        })
                    } else {
                        // 400/401/403/404 with a body that isn't the
                        // token-invalid signature — config drift; retry
                        // won't recover.
                        PushResult::Permanent(PermanentError::BadRequest {
                            platform: Platform::Fcm,
                            status: status_u16,
                            body: body_text,
                        })
                    }
                }
            }
            Err(e) => PushResult::Transient(TransientError::Transport {
                platform: Platform::Fcm,
                message: e.to_string(),
            }),
        }
    }

    /// Send an APNs notification to a device token.
    ///
    /// `visible` selects between the two send classes:
    /// - `false` (default, unchanged from before #78): silent
    ///   `content-available` background wake — byte-identical payload and
    ///   headers to before this was introduced.
    /// - `true`: unbudgeted ALERT-class send for a changeset that touched a
    ///   `SyncNotify`-visible table. `aps.alert.title` carries ONLY the
    ///   relay-operator placeholder (`ApnsConfig::alert_title`) — never
    ///   client-supplied text. `mutable-content: 1` lets an app's
    ///   Notification Service Extension rewrite it with real content before
    ///   display; `content-available: 1` is kept alongside so the existing
    ///   background-sync path still runs for apps without an NSE.
    pub async fn send_apns(
        &self,
        token: &str,
        topic: &str,
        peer_addrs: &[String],
        visible: bool,
    ) -> PushResult {
        let apns = match &self.apns {
            Some(c) => c,
            None => {
                return PushResult::Permanent(PermanentError::NotConfigured {
                    platform: Platform::Apns,
                });
            }
        };

        let jwt = match self.get_apns_jwt(apns) {
            Ok(t) => t,
            // JWT signing failure is purely local — bad PEM, malformed
            // key. Won't fix without an operator config change.
            Err(e) => {
                return PushResult::Permanent(PermanentError::CredentialError {
                    platform: Platform::Apns,
                    message: e,
                });
            }
        };

        let host = if apns.sandbox {
            "https://api.sandbox.push.apple.com"
        } else {
            "https://api.push.apple.com"
        };
        let url = format!("{host}/3/device/{token}");

        let body = if visible {
            serde_json::json!({
                "aps": {
                    "alert": { "title": apns.alert_title },
                    "mutable-content": 1,
                    "content-available": 1,
                    "sound": "default"
                },
                "topic": topic,
                "peer_addrs": serde_json::to_string(peer_addrs).unwrap_or_default()
            })
        } else {
            serde_json::json!({
                "aps": {
                    "content-available": 1
                },
                "topic": topic,
                "peer_addrs": serde_json::to_string(peer_addrs).unwrap_or_default()
            })
        };

        // Retain the wake for 10 minutes. Without `apns-expiration`, APNs uses
        // expiry 0 = "deliver now or discard"; combined with a throttled
        // background priority, a device that's asleep/offline at send time
        // would never get woken. A future expiry makes APNs store and retry,
        // which is what the "wake a killed device" use case needs — kept for
        // both send classes.
        let expiration = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600;

        let mut request = self.client.post(&url).bearer_auth(&jwt);
        request = if visible {
            // `apns-priority: 10` + `apns-push-type: alert` request immediate
            // delivery — required for an alert to actually land on the lock
            // screen rather than being throttled like a background push.
            // `apns-collapse-id` (topic-derived) coalesces bursts of alerts
            // for the same group into one notification-center entry instead
            // of stacking duplicates. Not `crate::short_topic` — that embeds
            // a non-ASCII ellipsis and is unbounded for non-standard topics,
            // while APNs documents collapse-id as an identifier of at most
            // 64 bytes with undocumented non-ASCII behavior (a BadCollapseId
            // rejection would classify as Permanent and silently drop the
            // alert).
            request
                .header("apns-push-type", "alert")
                .header("apns-priority", "10")
                .header("apns-collapse-id", collapse_id(topic))
        } else {
            request
                .header("apns-push-type", "background")
                .header("apns-priority", "5")
        };
        request = request
            .header("apns-expiration", expiration.to_string())
            .header("apns-topic", &apns.bundle_id)
            .json(&body);

        match request.send().await {
            Ok(resp) => {
                let status = resp.status();
                let status_u16 = status.as_u16();
                if status.is_success() {
                    PushResult::Sent
                } else if status_u16 == 410 {
                    PushResult::TokenInvalid {
                        reason: TokenInvalidReason::Apns410,
                    }
                } else {
                    let retry_after = parse_retry_after(resp.headers());
                    let body_text = resp.text().await.unwrap_or_default();
                    if body_text.contains("BadDeviceToken") || body_text.contains("Unregistered") {
                        PushResult::TokenInvalid {
                            reason: TokenInvalidReason::Apns410,
                        }
                    } else if status_u16 == 429 || (500..600).contains(&status_u16) {
                        PushResult::Transient(TransientError::HttpStatus {
                            platform: Platform::Apns,
                            status: status_u16,
                            retry_after,
                            body: body_text,
                        })
                    } else {
                        // 400/403 with a non-token-related body —
                        // malformed payload, bad apns-topic, etc.
                        PushResult::Permanent(PermanentError::BadRequest {
                            platform: Platform::Apns,
                            status: status_u16,
                            body: body_text,
                        })
                    }
                }
            }
            Err(e) => PushResult::Transient(TransientError::Transport {
                platform: Platform::Apns,
                message: e.to_string(),
            }),
        }
    }

    /// Get an OAuth2 access token for FCM using the service account JSON.
    async fn get_fcm_access_token(&self, fcm: &FcmConfig) -> Result<String, String> {
        // Parse service account JSON to extract key and email
        let sa: serde_json::Value = serde_json::from_str(&fcm.service_account_json)
            .map_err(|e| format!("Invalid service account JSON: {e}"))?;

        let client_email = sa["client_email"]
            .as_str()
            .ok_or("Missing client_email in service account")?;
        let private_key = sa["private_key"]
            .as_str()
            .ok_or("Missing private_key in service account")?;

        // Build JWT for Google OAuth2
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let claims = serde_json::json!({
            "iss": client_email,
            "scope": "https://www.googleapis.com/auth/firebase.messaging",
            "aud": "https://oauth2.googleapis.com/token",
            "iat": now,
            "exp": now + 3600,
        });

        let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(private_key.as_bytes())
            .map_err(|e| format!("Invalid RSA key: {e}"))?;

        let jwt_header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        let jwt = jsonwebtoken::encode(&jwt_header, &claims, &encoding_key)
            .map_err(|e| format!("JWT encoding error: {e}"))?;

        // Exchange JWT for access token
        let resp = self
            .client
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", &jwt),
            ])
            .send()
            .await
            .map_err(|e| format!("OAuth2 request error: {e}"))?;

        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| format!("OAuth2 response parse error: {e}"))?;

        body["access_token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| format!("No access_token in OAuth2 response: {body}"))
    }

    /// Get (or refresh) an APNs JWT. Cached for 50 minutes (tokens valid for 60 min).
    fn get_apns_jwt(&self, apns: &ApnsConfig) -> Result<String, String> {
        let mut cache = self.apns_jwt_cache.lock().unwrap();
        if let Some(ref cached) = *cache
            && cached.created_at.elapsed().as_secs() < 3000
        {
            return Ok(cached.token.clone());
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let header = jsonwebtoken::Header {
            alg: jsonwebtoken::Algorithm::ES256,
            kid: Some(apns.key_id.clone()),
            ..Default::default()
        };

        let claims = serde_json::json!({
            "iss": apns.team_id,
            "iat": now,
        });

        let encoding_key = jsonwebtoken::EncodingKey::from_ec_pem(apns.key_pem.as_bytes())
            .map_err(|e| format!("Invalid APNs EC key: {e}"))?;

        let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
            .map_err(|e| format!("APNs JWT encoding error: {e}"))?;

        *cache = Some(ApnsJwtCache {
            token: token.clone(),
            created_at: Instant::now(),
        });

        Ok(token)
    }
}

#[cfg(test)]
mod collapse_id_tests {
    use super::collapse_id;

    #[test]
    fn standard_topic_is_ascii_and_bounded() {
        let topic = format!("wavesync2-{}", "ab".repeat(32));
        let id = collapse_id(&topic);
        assert!(id.len() <= 64);
        assert!(id.is_ascii());
        assert!(id.starts_with("wavesync2-"));
    }

    #[test]
    fn non_ascii_and_oversize_topics_are_sanitized() {
        let id = collapse_id(&format!("groc…ery/list {}", "x".repeat(100)));
        assert!(id.len() <= 64);
        assert!(id.is_ascii());
        assert!(!id.contains('…') && !id.contains('/') && !id.contains(' '));
    }
}
