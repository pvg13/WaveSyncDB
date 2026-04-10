//! FCM and APNs push notification sender.

use std::sync::Mutex;
use std::time::Instant;

/// Result of sending a push notification.
#[derive(Debug)]
pub enum PushResult {
    /// Successfully sent.
    Sent,
    /// Token is invalid/expired — caller should prune it.
    TokenInvalid,
    /// Transient error — retry later.
    Error(String),
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
            None => return PushResult::Error("FCM not configured".to_string()),
        };

        let access_token = match self.get_fcm_access_token(fcm).await {
            Ok(t) => t,
            Err(e) => return PushResult::Error(format!("FCM auth error: {e}")),
        };

        let url = format!(
            "https://fcm.googleapis.com/v1/projects/{}/messages:send",
            fcm.project_id
        );

        let body = serde_json::json!({
            "message": {
                "token": token,
                "data": {
                    "type": "sync_available",
                    "topic": topic,
                    "peer_addrs": serde_json::to_string(peer_addrs).unwrap_or_default()
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
                if status.is_success() {
                    PushResult::Sent
                } else {
                    let body_text = resp.text().await.unwrap_or_default();
                    if body_text.contains("UNREGISTERED") || body_text.contains("INVALID_ARGUMENT")
                    {
                        PushResult::TokenInvalid
                    } else {
                        PushResult::Error(format!("FCM {status}: {body_text}"))
                    }
                }
            }
            Err(e) => PushResult::Error(format!("FCM request error: {e}")),
        }
    }

    /// Send a silent/background APNs notification to a device token.
    pub async fn send_apns(&self, token: &str, topic: &str) -> PushResult {
        let apns = match &self.apns {
            Some(c) => c,
            None => return PushResult::Error("APNs not configured".to_string()),
        };

        let jwt = match self.get_apns_jwt(apns) {
            Ok(t) => t,
            Err(e) => return PushResult::Error(format!("APNs JWT error: {e}")),
        };

        let host = if apns.sandbox {
            "https://api.sandbox.push.apple.com"
        } else {
            "https://api.push.apple.com"
        };
        let url = format!("{host}/3/device/{token}");

        let body = serde_json::json!({
            "aps": {
                "content-available": 1
            },
            "topic": topic
        });

        match self
            .client
            .post(&url)
            .bearer_auth(&jwt)
            .header("apns-push-type", "background")
            .header("apns-priority", "5")
            .header("apns-topic", &apns.bundle_id)
            .json(&body)
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    PushResult::Sent
                } else if status.as_u16() == 410 {
                    PushResult::TokenInvalid
                } else {
                    let body_text = resp.text().await.unwrap_or_default();
                    if body_text.contains("BadDeviceToken") || body_text.contains("Unregistered") {
                        PushResult::TokenInvalid
                    } else {
                        PushResult::Error(format!("APNs {status}: {body_text}"))
                    }
                }
            }
            Err(e) => PushResult::Error(format!("APNs request error: {e}")),
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
