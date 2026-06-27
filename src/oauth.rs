use crate::config::TokenSet;
use anyhow::{bail, Context};
use chrono::Utc;
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpSocket};
use url::Url;

const OAUTH_AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const OAUTH_TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const SCOPES: &str = "https://www.googleapis.com/auth/calendar.events https://www.googleapis.com/auth/calendar.readonly";

fn redirect_uri(port: u16) -> String {
    format!("http://localhost:{}", port)
}

fn generate_state() -> String {
    rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

pub fn build_auth_url(client_id: &str, email: &str, port: u16, state: &str) -> String {
    let mut url = Url::parse(OAUTH_AUTH_URL).unwrap();
    {
        let mut q = url.query_pairs_mut();
        q.append_pair("client_id", client_id);
        q.append_pair("redirect_uri", &redirect_uri(port));
        q.append_pair("response_type", "code");
        q.append_pair("scope", SCOPES);
        q.append_pair("access_type", "offline");
        q.append_pair("prompt", "consent");
        q.append_pair("state", state);
        q.append_pair("login_hint", email);
    }
    url.to_string()
}

fn bind_localhost(port: u16) -> anyhow::Result<TcpListener> {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
    let socket = TcpSocket::new_v4().context("Failed to create IPv4 socket")?;
    socket
        .set_reuseaddr(true)
        .context("Failed to set SO_REUSEADDR")?;
    socket.bind(addr).context("Failed to bind address")?;
    let listener = socket.listen(1).context("Failed to listen")?;
    Ok(listener)
}

struct CallbackResult {
    code: Option<String>,
    error: Option<String>,
    raw_line: String,
}

async fn read_callback(listener: TcpListener) -> anyhow::Result<CallbackResult> {
    let (socket, _) = listener.accept().await.context("No connection received")?;
    let (reader_half, mut writer_half) = socket.into_split();
    let mut buf_reader = BufReader::new(reader_half);

    let mut request_line = String::new();
    buf_reader
        .read_line(&mut request_line)
        .await
        .context("Failed to read request line")?;

    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 {
        let _ = writer_half
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await;
        bail!("Invalid HTTP request: {}", request_line.trim());
    }

    let path_and_query = parts[1];
    let full_url = format!("http://localhost{}", path_and_query);

    let params: Vec<(String, String)> = match Url::parse(&full_url) {
        Ok(parsed) => parsed
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        Err(_) => Vec::new(),
    };

    let code = params
        .iter()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.clone());

    let error = params
        .iter()
        .find(|(k, _)| k == "error")
        .map(|(_, v)| v.clone());

    let _state = params
        .iter()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.clone());

    let body = if error.is_some() {
        format!(
            "<html><body><h1>Authorization Error</h1><p>{}</p></body></html>",
            error.as_ref().unwrap()
        )
    } else if code.is_some() {
        "<html><body><h1>Authorization Complete</h1><p>You may close this window.</p></body></html>".to_string()
    } else {
        "<html><body><h1>Bad Request</h1><p>No authorization code received.</p></body></html>".to_string()
    };

    let response = format!(
        "HTTP/1.1 {} {}\r\nContent-Type: text/html\r\nContent-Length: {}\r\n\r\n{}",
        if code.is_some() { 200 } else { 400 },
        if code.is_some() { "OK" } else { "Bad Request" },
        body.len(),
        body
    );
    let _ = writer_half.write_all(response.as_bytes()).await;
    let _ = writer_half.shutdown().await;

    Ok(CallbackResult {
        code,
        error,
        raw_line: request_line.trim().to_string(),
    })
}

fn parse_code_from_paste(input: &str) -> Option<String> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }
    if input.starts_with("http") {
        if let Ok(url) = Url::parse(input) {
            return url
                .query_pairs()
                .find(|(k, _)| k == "code")
                .map(|(_, v)| v.to_string());
        }
        return None;
    }
    Some(input.to_string())
}

#[derive(Debug, Deserialize)]
struct GoogleOAuthError {
    error: String,
}

pub fn is_token_revoked_error(error_body: &str) -> bool {
    if let Ok(err) = serde_json::from_str::<GoogleOAuthError>(error_body) {
        err.error == "invalid_grant"
    } else {
        error_body.contains("invalid_grant")
    }
}

pub fn is_retryable_error(error_msg: &str, status: Option<reqwest::StatusCode>) -> bool {
    if let Some(code) = status {
        if code.is_server_error() {
            return true;
        }
        if code.is_client_error() && !is_token_revoked_error(error_msg) {
            return false;
        }
    }
    if error_msg.contains("HTTP 5") {
        return true;
    }
    if error_msg.contains("HTTP 4") {
        return false;
    }
    error_msg.contains("timeout")
        || error_msg.contains("connection")
        || error_msg.contains("tls")
        || error_msg.contains("dns")
        || error_msg.contains("reset")
        || error_msg.contains("refused")
}

async fn obtain_code(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    email: &str,
    port: u16,
    state: &str,
    token_path: &Path,
) -> anyhow::Result<()> {
    let auth_url = build_auth_url(client_id, email, port, state);

    println!();
    println!("══════════════════════════════════════════════════");
    println!("Account: {}", email);
    println!("══════════════════════════════════════════════════");
    println!();
    println!("Open this URL in a browser logged into that account:");
    println!();
    println!("  {}", auth_url);
    println!();

    let listener = match bind_localhost(port) {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "Could not bind to port {}: {}. Using manual code entry.",
                port, e
            );
            return manual_entry(client, client_id, client_secret, email, port, token_path)
                .await;
        }
    };

    println!("Waiting for browser redirect on http://localhost:{} ...", port);
    println!("(timeout in 120s — manual fallback available)");

    match tokio::time::timeout(Duration::from_secs(120), read_callback(listener)).await {
        Ok(Ok(cb)) => {
            if let Some(error) = cb.error {
                eprintln!("Google returned an error: {}", error);
                if error == "access_denied" || error == "invalid_scope" {
                    bail!("Authorization denied by Google: {}", error);
                }
                eprintln!("Falling back to manual code entry...");
                return manual_entry(client, client_id, client_secret, email, port, token_path)
                    .await;
            }
            if let Some(code) = cb.code {
                let state_val = cb
                    .raw_line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|p| {
                        Url::parse(&format!("http://localhost{}", p))
                            .ok()?
                            .query_pairs()
                            .find(|(k, _)| k == "state")
                            .map(|(_, v)| v.to_string())
                    })
                    .unwrap_or_default();

                if state_val != *state && !state_val.is_empty() {
                    eprintln!(
                        "State mismatch (expected {}, got {}). This may indicate a CSRF attempt.",
                        state, state_val
                    );
                    bail!("CSRF check failed: state mismatch");
                }

                println!("Authorization code received. Exchanging for tokens...");
                let token =
                    exchange_code(client, client_id, client_secret, &code, port).await?;
                token.save(token_path)?;
                println!("✓ Token saved to {}", token_path.display());
                return Ok(());
            }
            eprintln!(
                "Callback received but no code found. Request was: {}",
                cb.raw_line
            );
            eprintln!("Falling back to manual code entry...");
            manual_entry(client, client_id, client_secret, email, port, token_path).await
        }
        Ok(Err(e)) => {
            eprintln!("Redirect listener error: {}. Falling back to manual entry.", e);
            manual_entry(client, client_id, client_secret, email, port, token_path).await
        }
        Err(_) => {
            eprintln!("Timed out waiting for browser redirect (120s).");
            manual_entry(client, client_id, client_secret, email, port, token_path).await
        }
    }
}

async fn manual_entry(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    _email: &str,
    port: u16,
    token_path: &Path,
) -> anyhow::Result<()> {
    println!();
    println!("--- Manual Authorization Code Entry ---");
    println!("After approving in the browser, Google will try to redirect you to:");
    println!("  http://localhost:{}?code=SOMETHING_LONG&scope=...&state=...", port);
    println!();
    println!("The page likely won't load, but the authorization 'code' is visible");
    println!("in the browser's URL bar. Copy the code value and paste it below.");
    println!();
    println!("(You can paste just the code, or the full URL — I'll extract the code)");
    println!();

    let token = loop {
        print!("Paste code (or 'quit' to abort): ");
        use std::io::Write;
        std::io::stdout().flush().ok();

        let mut input = String::new();
        std::io::stdin()
            .read_line(&mut input)
            .context("Failed to read input")?;

        let input = input.trim();
        if input.eq_ignore_ascii_case("quit") || input.eq_ignore_ascii_case("q") {
            bail!("User aborted manual code entry.");
        }

        match parse_code_from_paste(input) {
            Some(code) => {
                println!("Exchanging code for tokens...");
                match exchange_code(client, client_id, client_secret, &code, port).await {
                    Ok(token) => break token,
                    Err(e) => {
                        eprintln!("Token exchange failed: {}", e);
                        eprintln!("Make sure you copied the entire code (it may be long). Try again.");
                    }
                }
            }
            None => {
                eprintln!("Could not find a code in that input. Paste the full URL or just the code value.");
            }
        }
    };

    token.save(token_path)?;
    println!("✓ Token saved to {}", token_path.display());
    Ok(())
}

pub async fn run_auth_flow(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    email: &str,
    port: u16,
    token_path: &Path,
) -> anyhow::Result<()> {
    let state = generate_state();
    obtain_code(client, client_id, client_secret, email, port, &state, token_path).await
}

pub async fn exchange_code(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    code: &str,
    port: u16,
) -> anyhow::Result<TokenSet> {
    let params = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("code", code),
        ("grant_type", "authorization_code"),
        ("redirect_uri", &redirect_uri(port)),
    ];

    let resp = client
        .post(OAUTH_TOKEN_URL)
        .form(&params)
        .send()
        .await
        .context("Failed to send token exchange request")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Token exchange failed: {}", body);
    }

    let mut token: TokenSet = resp
        .json()
        .await
        .context("Failed to parse token response")?;

    token.obtained_at = Some(Utc::now().timestamp());

    if token.refresh_token.as_deref().unwrap_or("").is_empty() {
        bail!("No refresh token returned. Did you approve all scopes? Try re-authorizing.");
    }

    Ok(token)
}

pub async fn refresh_access_token(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    refresh_token: &str,
) -> anyhow::Result<TokenSet> {
    let params = [
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("refresh_token", refresh_token),
        ("grant_type", "refresh_token"),
    ];

    let resp = client
        .post(OAUTH_TOKEN_URL)
        .form(&params)
        .send()
        .await
        .context("Failed to refresh token")?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!("Token refresh failed (HTTP {}): {}", status.as_u16(), body);
    }

    let mut token: TokenSet = resp
        .json()
        .await
        .context("Failed to parse refreshed token")?;

    token.refresh_token = Some(refresh_token.to_string());
    token.obtained_at = Some(Utc::now().timestamp());

    Ok(token)
}
