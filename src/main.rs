mod calendar;
mod config;
mod db;
mod oauth;
mod sync;

use crate::config::{CalendarConfig, ClientSecret, HipoglosConfig};
use anyhow::Context;
use reqwest::Client;
use std::path::{Path, PathBuf};

const CLIENT_SECRET_FILE: &str = "hipoglos_client_secret.json";
const CONFIG_FILE: &str = "config.toml";
const TOKENS_DIR: &str = "data/tokens";

const ACCOUNTS: &[(&str, u16)] = &[
    ("your-personal@gmail.com", 9876),
    ("your-work@company1.com", 9877),
    ("your-work@company2.com", 9878),
];

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let command = args.get(1).map(|s| s.as_str()).unwrap_or("setup");

    match command {
        "setup" => cmd_setup().await?,
        "sync" => cmd_sync().await?,
        "status" => cmd_status().await?,
        other => {
            eprintln!("Unknown command: {}", other);
            eprintln!("Usage: hipoglos <setup|sync|status>");
        }
    }

    Ok(())
}

async fn cmd_setup() -> anyhow::Result<()> {
    println!("=== hipoglos Calendar Sync Setup ===\n");

    let config_path = Path::new(CONFIG_FILE);
    let existing_config: Option<HipoglosConfig> = if config_path.exists() {
        match HipoglosConfig::load(config_path) {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("Failed to load existing config: {}. Will use defaults.", e);
                None
            }
        }
    } else {
        None
    };

    let accounts: Vec<(&str, u16)> = if let Some(ref config) = existing_config {
        config
            .calendars
            .iter()
            .enumerate()
            .map(|(i, cal)| {
                let port: u16 = 9876 + i as u16;
                (cal.email.as_str(), port)
            })
            .collect()
    } else {
        ACCOUNTS.iter().map(|&(e, p)| (e, p)).collect()
    };

    println!("This will authenticate you to {} Google accounts.", accounts.len());
    println!("You'll need {} separate browsers (or incognito/private windows):", accounts.len());
    for (i, &(email, _)) in accounts.iter().enumerate() {
        println!("  {}. Browser {} → {}", i + 1, i + 1, email);
    }
    println!();
    if !accounts.is_empty() {
        let ports: Vec<String> = accounts.iter().map(|(_, p)| p.to_string()).collect();
        println!("IMPORTANT: If running on a remote machine via SSH, you must forward");
        println!("ports {} back to your local browser:", ports.join(", "));
        let forward: Vec<String> = accounts
            .iter()
            .map(|(_, p)| format!("-L {}:localhost:{}", p, p))
            .collect();
        println!("  ssh {}", forward.join(" "));
        println!();
    }
    println!("If the redirect doesn't work, the tool will prompt you to paste");
    println!("the authorization code manually from the browser's URL bar.");
    println!();

    let client_secret_path = Path::new(CLIENT_SECRET_FILE);
    if !client_secret_path.exists() {
        anyhow::bail!(
            "Client secret file not found: {}\n\
             Place your Google OAuth client secret JSON in this file.",
            client_secret_path.display()
        );
    }

    let secret = ClientSecret::load(client_secret_path)?;
    let client = Client::new();

    let mut calendars: Vec<CalendarConfig> = Vec::new();

    for &(email, port) in &accounts {
        let token_filename = format!("{}.json", email.replace('@', "_at_"));
        let token_path = Path::new(TOKENS_DIR).join(&token_filename);

        if token_path.exists() {
            println!(
                "Token already exists for {} ({}), skipping auth.",
                email,
                token_path.display()
            );
        } else {
            oauth::run_auth_flow(
                &client,
                &secret.installed.client_id,
                &secret.installed.client_secret,
                email,
                port,
                &token_path,
            )
            .await?;
        }

        let existing_cal = existing_config
            .as_ref()
            .and_then(|cfg| cfg.calendars.iter().find(|c| c.email == email));

        calendars.push(CalendarConfig {
            email: email.to_string(),
            calendar_id: existing_cal
                .map(|c| c.calendar_id.clone())
                .unwrap_or_else(|| "primary".to_string()),
            token_file: PathBuf::from(&token_path),
            color_id: existing_cal.and_then(|c| c.color_id.clone()),
            mirror_style: existing_cal
                .map(|c| c.mirror_style.clone())
                .unwrap_or_default(),
        });
    }

    println!("\n=== Verifying Calendar Access ===\n");

    for cal in &calendars {
        verify_calendar_access(
            &client,
            &secret.installed.client_id,
            &secret.installed.client_secret,
            cal,
        )
        .await?;
    }

    let config = HipoglosConfig {
        poll_interval_seconds: 300,
        calendars,
    };

    config.save(Path::new(CONFIG_FILE))?;
    println!("\n✓ Config saved to {}", CONFIG_FILE);
    println!("  You can now run: cargo run -- sync");

    Ok(())
}

async fn verify_calendar_access(
    client: &Client,
    client_id: &str,
    client_secret: &str,
    cal: &CalendarConfig,
) -> anyhow::Result<()> {
    println!("──────────────────────────────────────────");
    println!("Verifying: {}", cal.email);
    println!("  Calendar ID: {}", cal.calendar_id);

    let mut token = config::TokenSet::load(&cal.token_file)
        .with_context(|| format!("Failed to load token for {}", cal.email))?;

    let access_token = calendar::ensure_fresh_token(
        client,
        client_id,
        client_secret,
        &mut token,
        &cal.token_file,
    )
    .await
    .with_context(|| format!("Failed to get access token for {}", cal.email))?;

    match calendar::list_calendars(client, &access_token).await {
        Ok(calendars) => {
            println!("  Calendars found:");
            for entry in &calendars {
                let primary = if entry.primary { " (primary)" } else { "" };
                println!("    - {} [{}]{}", entry.summary, entry.id, primary);
            }
        }
        Err(e) => {
            eprintln!("  Warning: Could not list calendars: {}", e);
        }
    }

    match calendar::list_events_preview(client, &access_token, &cal.calendar_id, Some(3)).await {
        Ok(events) => {
            println!("  ✓ Read access OK ({} recent events)", events.len());
            for ev in &events {
                let summary = ev["summary"].as_str().unwrap_or("(no title)");
                let start = ev["start"]
                    .get("dateTime")
                    .or(ev["start"].get("date"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                println!("    - {} @ {}", summary, start);
            }
        }
        Err(e) => {
            eprintln!("  ✗ Read access FAILED: {}", e);
        }
    }

    match calendar::create_test_event(client, &access_token, &cal.calendar_id).await {
        Ok(event_id) => {
            println!("  ✓ Write access OK (created test event)");
            match calendar::delete_event(client, &access_token, &cal.calendar_id, &event_id).await {
                Ok(()) => println!("  ✓ Delete access OK (cleaned up test event)"),
                Err(e) => eprintln!("  ✗ Delete access FAILED: {}", e),
            }
        }
        Err(e) => {
            eprintln!("  ✗ Write access FAILED: {}", e);
        }
    }

    println!();
    Ok(())
}

async fn cmd_sync() -> anyhow::Result<()> {
    let config_path = Path::new(CONFIG_FILE);
    if !config_path.exists() {
        anyhow::bail!(
            "Config file not found: {}\nRun 'cargo run -- setup' first.",
            config_path.display()
        );
    }

    let client_secret_path = Path::new(CLIENT_SECRET_FILE);
    let secret = ClientSecret::load(client_secret_path)?;
    let config = HipoglosConfig::load(config_path)?;

    sync::run_sync_loop(
        &config,
        &secret.installed.client_id,
        &secret.installed.client_secret,
    )
    .await
}

async fn cmd_status() -> anyhow::Result<()> {
    let config_path = Path::new(CONFIG_FILE);
    if !config_path.exists() {
        anyhow::bail!("Config not found. Run 'cargo run -- setup' first.");
    }

    let config = HipoglosConfig::load(config_path)?;
    println!("Configured calendars:");
    for cal in &config.calendars {
        let token_exists = cal.token_file.exists();
        let status = if token_exists { "✓" } else { "✗" };
        println!(
            "  {} {} (calendar_id={}, token={})",
            status,
            cal.email,
            cal.calendar_id,
            cal.token_file.display()
        );
    }

    Ok(())
}
