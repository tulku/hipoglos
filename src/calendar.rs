use crate::config::TokenSet;
use crate::oauth;
use anyhow::{bail, Context};
use serde::Deserialize;

const CALENDAR_API_BASE: &str = "https://www.googleapis.com/calendar/v3";

#[derive(Debug, Deserialize)]
pub struct CalendarListResponse {
    pub items: Vec<CalendarEntry>,
}

#[derive(Debug, Deserialize)]
pub struct CalendarEntry {
    pub id: String,
    pub summary: String,
    #[serde(default)]
    pub primary: bool,
}

fn auth_header(token: &str) -> String {
    format!("Bearer {}", token)
}

fn urlencode(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

pub async fn ensure_fresh_token(
    client: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    token: &mut TokenSet,
    token_path: &std::path::Path,
) -> anyhow::Result<String> {
    let refresh_token = token
        .refresh_token
        .as_deref()
        .context("No refresh token stored. Re-run 'cargo run -- setup' to re-authenticate.")?;
    let new_token =
        oauth::refresh_access_token(client, client_id, client_secret, refresh_token).await?;
    new_token.save(token_path)?;
    *token = new_token;
    Ok(token.access_token.clone())
}

pub async fn list_calendars(
    client: &reqwest::Client,
    access_token: &str,
) -> anyhow::Result<Vec<CalendarEntry>> {
    let resp = client
        .get(format!("{}/users/me/calendarList", CALENDAR_API_BASE))
        .header("Authorization", auth_header(access_token))
        .send()
        .await
        .context("Failed to list calendars")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Calendar list API error: {}", body);
    }

    let data: CalendarListResponse = resp
        .json()
        .await
        .context("Failed to parse calendar list")?;

    Ok(data.items)
}

pub async fn list_events_preview(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    max_results: Option<u32>,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let limit = max_results.unwrap_or(10);

    let resp = client
        .get(format!(
            "{}/calendars/{}/events",
            CALENDAR_API_BASE,
            urlencode(calendar_id)
        ))
        .header("Authorization", auth_header(access_token))
        .query(&[
            ("maxResults", limit.to_string().as_str()),
            ("singleEvents", "true"),
            ("orderBy", "startTime"),
        ])
        .send()
        .await
        .context("Failed to list events")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Events list API error: {}", body);
    }

    let data: EventsPage = resp
        .json()
        .await
        .context("Failed to parse events response")?;

    Ok(data.items)
}

#[derive(Debug, Deserialize)]
struct EventsPage {
    items: Vec<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    next_page_token: Option<String>,
}

pub async fn list_events_sync(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    updated_min: &str,
    single_events: bool,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let mut all_events: Vec<serde_json::Value> = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let url = format!(
            "{}/calendars/{}/events",
            CALENDAR_API_BASE,
            urlencode(calendar_id)
        );

        let mut req = client
            .get(&url)
            .header("Authorization", auth_header(access_token))
            .query(&[
                ("updatedMin", updated_min),
                ("showDeleted", "true"),
                ("singleEvents", if single_events { "true" } else { "false" }),
                ("maxResults", "2500"),
            ]);

        if let Some(ref token) = page_token {
            req = req.query(&[("pageToken", token.as_str())]);
        }

        let resp = req.send().await.context("Failed to list events for sync")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("Events sync list API error: {}", body);
        }

        let page: EventsPage = resp
            .json()
            .await
            .context("Failed to parse events page")?;

        all_events.extend(page.items);
        page_token = page.next_page_token;

        if page_token.is_none() {
            break;
        }
    }

    Ok(all_events)
}

pub async fn list_instances_forward(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    time_min: &str,
    time_max: &str,
) -> anyhow::Result<Vec<serde_json::Value>> {
    let mut all_events: Vec<serde_json::Value> = Vec::new();
    let mut page_token: Option<String> = None;

    loop {
        let url = format!(
            "{}/calendars/{}/events",
            CALENDAR_API_BASE,
            urlencode(calendar_id)
        );

        let mut req = client
            .get(&url)
            .header("Authorization", auth_header(access_token))
            .query(&[
                ("timeMin", time_min),
                ("timeMax", time_max),
                ("singleEvents", "true"),
                ("maxResults", "2500"),
            ]);

        if let Some(ref token) = page_token {
            req = req.query(&[("pageToken", token.as_str())]);
        }

        let resp = req.send().await.context("Failed to list forward instances")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("Forward instance list API error: {}", body);
        }

        let page: EventsPage = resp
            .json()
            .await
            .context("Failed to parse forward instances")?;

        all_events.extend(page.items);
        page_token = page.next_page_token;

        if page_token.is_none() {
            break;
        }
    }

    Ok(all_events)
}

pub fn is_mirror_event(event: &serde_json::Value) -> bool {
    event
        .pointer("/extendedProperties/private/mirrorSource")
        .and_then(|v| v.as_str())
        .is_some()
}

pub fn is_working_location(event: &serde_json::Value) -> bool {
    matches!(
        event["eventType"].as_str(),
        Some("workingLocation" | "outOfOffice")
    )
}

pub fn is_self_declined(event: &serde_json::Value) -> bool {
    event["attendees"]
        .as_array()
        .and_then(|a| a.iter().find(|a| a["self"].as_bool() == Some(true)))
        .and_then(|a| a["responseStatus"].as_str())
        == Some("declined")
}

#[allow(dead_code)]
pub fn mirror_source_id(event: &serde_json::Value) -> Option<&str> {
    event
        .pointer("/extendedProperties/private/mirrorSource")
        .and_then(|v| v.as_str())
}

pub fn mirror_color(source_email: &str, configured: Option<&str>) -> String {
    if let Some(c) = configured {
        if !c.is_empty() {
            return c.to_string();
        }
    }
    let colors = ["1", "3", "8"];
    let hash: usize = source_email
        .bytes()
        .fold(0usize, |acc, b| acc.wrapping_mul(31).wrapping_add(b as usize));
    colors[hash % colors.len()].to_string()
}

fn format_mirror_description(source_event: &serde_json::Value, source_calendar_id: &str) -> String {
    let status_line = attendee_status_line(source_event);

    let original_desc = source_event["description"].as_str().unwrap_or("");
    let mut parts: Vec<&str> = Vec::new();
    if !original_desc.is_empty() {
        parts.push(original_desc);
    }
    if let Some(ref s) = status_line {
        parts.push(s);
    }
    parts.push(&"Mirror from");
    parts.push(source_calendar_id);
    parts.push(&"(hipoglos)");

    parts.join("\n")
}

fn attendee_status_line(event: &serde_json::Value) -> Option<String> {
    let attendees = event["attendees"].as_array()?;
    for a in attendees {
        if a["self"].as_bool() == Some(true) {
            let status = a["responseStatus"].as_str().unwrap_or("needsAction");
            let label = match status {
                "accepted" => "Accepted",
                "declined" => "Declined",
                "tentative" => "Tentative",
                _ => "Pending",
            };
            return Some(format!("Status: {}", label));
        }
    }
    None
}

pub fn build_mirror_body(
    source_event: &serde_json::Value,
    source_calendar_id: &str,
    color_id: &str,
) -> serde_json::Value {
    let source_event_id = source_event["id"].as_str().unwrap_or("");

    let original_summary = source_event["summary"].as_str().unwrap_or("(no title)");
    let mirror_summary = format!("\u{2197} {}", original_summary);

    let mirror_desc = format_mirror_description(source_event, source_calendar_id);

    let mut body = serde_json::json!({
        "visibility": "private",
        "transparency": "opaque",
        "colorId": color_id,
        "reminders": {"useDefault": false},
        "summary": mirror_summary,
        "description": mirror_desc,
        "extendedProperties": {
            "private": {
                "mirrorSource": source_calendar_id,
                "mirrorEventId": source_event_id
            }
        }
    });

    for field in &["start", "end", "location", "recurrence"] {
        if let Some(val) = source_event.get(field) {
            if !val.is_null() {
                body[field] = val.clone();
            }
        }
    }

    body
}

pub fn build_mirror_update(
    source_event: &serde_json::Value,
    source_calendar_id: &str,
    color_id: &str,
) -> serde_json::Value {
    let original_summary = source_event["summary"].as_str().unwrap_or("(no title)");
    let mirror_summary = format!("\u{2197} {}", original_summary);

    let mirror_desc = format_mirror_description(source_event, source_calendar_id);

    let mut body = serde_json::json!({
        "reminders": {"useDefault": false},
        "colorId": color_id,
        "summary": mirror_summary,
        "description": mirror_desc,
    });

    for field in &["start", "end", "location", "recurrence"] {
        if let Some(val) = source_event.get(field) {
            if !val.is_null() {
                body[field] = val.clone();
            }
        }
    }

    body
}

pub async fn create_event(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    body: &serde_json::Value,
) -> anyhow::Result<String> {
    let resp = client
        .post(format!(
            "{}/calendars/{}/events",
            CALENDAR_API_BASE,
            urlencode(calendar_id)
        ))
        .header("Authorization", auth_header(access_token))
        .json(body)
        .send()
        .await
        .context("Failed to create event")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Event creation failed: {}", body);
    }

    let created: serde_json::Value = resp
        .json()
        .await
        .context("Failed to parse created event")?;

    created["id"]
        .as_str()
        .context("No event ID in creation response")
        .map(|s| s.to_string())
}

pub async fn update_event(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    event_id: &str,
    body: &serde_json::Value,
) -> anyhow::Result<()> {
    let resp = client
        .patch(format!(
            "{}/calendars/{}/events/{}",
            CALENDAR_API_BASE,
            urlencode(calendar_id),
            urlencode(event_id)
        ))
        .header("Authorization", auth_header(access_token))
        .json(body)
        .send()
        .await
        .context("Failed to update event")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Event update failed: {}", body);
    }

    Ok(())
}

pub async fn delete_event(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    event_id: &str,
) -> anyhow::Result<()> {
    let resp = client
        .delete(format!(
            "{}/calendars/{}/events/{}",
            CALENDAR_API_BASE,
            urlencode(calendar_id),
            urlencode(event_id)
        ))
        .header("Authorization", auth_header(access_token))
        .send()
        .await
        .context("Failed to delete event")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Event deletion failed: {}", body);
    }

    Ok(())
}

pub async fn get_event(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    event_id: &str,
) -> anyhow::Result<serde_json::Value> {
    let resp = client
        .get(format!(
            "{}/calendars/{}/events/{}",
            CALENDAR_API_BASE,
            urlencode(calendar_id),
            urlencode(event_id)
        ))
        .header("Authorization", auth_header(access_token))
        .send()
        .await
        .context("Failed to fetch event")?;

    if !resp.status().is_success() {
        let body = resp.text().await.unwrap_or_default();
        bail!("Event fetch failed: {}", body);
    }

    resp.json()
        .await
        .context("Failed to parse event")
}

pub async fn create_test_event(
    client: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
) -> anyhow::Result<String> {
    let now = chrono::Utc::now();
    let start = now + chrono::Duration::hours(1);
    let end = start + chrono::Duration::hours(1);

    let start_str = start.format("%Y-%m-%dT%H:%M:%S").to_string();
    let end_str = end.format("%Y-%m-%dT%H:%M:%S").to_string();

    let body = serde_json::json!({
        "summary": "[hipoglos verification]",
        "start": {
            "dateTime": start_str,
            "timeZone": "UTC"
        },
        "end": {
            "dateTime": end_str,
            "timeZone": "UTC"
        },
        "visibility": "private"
    });

    create_event(client, access_token, calendar_id, &body).await
}
