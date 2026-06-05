use crate::calendar;
use crate::config::{CalendarConfig, TokenSet};
use crate::db::{Database, MirrorMapping};
use anyhow::Context;
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

pub struct SyncStats {
    pub created: usize,
    pub updated: usize,
    pub deleted: usize,
}

pub async fn run_sync_loop(
    config: &crate::config::HipoglosConfig,
    client_id: &str,
    client_secret: &str,
) -> anyhow::Result<()> {
    let db = Database::open(Path::new("data/sync.db"))?;
    db.initialize()?;
    let client = reqwest::Client::new();

    let color_fp = color_fingerprint(config);
    if db.get_meta("mirror_color_fp")?.as_deref() != Some(&color_fp) {
        tracing::info!("Color config changed — running mirror style migration...");
        match migrate_mirror_style(&db, &client, config, client_id, client_secret).await {
            Ok(n) => {
                tracing::info!("Mirror style migration complete: {} mirrors updated", n);
                db.set_meta("mirror_color_fp", &color_fp)?;
            }
            Err(e) => {
                tracing::error!("Mirror style migration failed: {:#}", e);
            }
        }
    }

    let content_fp = mirror_content_fingerprint(config);
    if db.get_meta("mirror_content_fp")?.as_deref() != Some(&content_fp) {
        tracing::info!("Mirror content config changed — deleting all mirror events for recreation...");
        match migrate_mirror_content(&db, &client, config, client_id, client_secret).await {
            Ok(n) => {
                tracing::info!("Mirror content migration complete: {} mirror events deleted", n);
                db.set_meta("mirror_content_fp", &content_fp)?;
            }
            Err(e) => {
                tracing::error!("Mirror content migration failed: {:#}", e);
            }
        }
    }

    tracing::info!(
        "Sync engine started: {} calendars, {}s interval",
        config.calendars.len(),
        config.poll_interval_seconds
    );

    loop {
        tracing::debug!("Starting sync cycle");

        for source in &config.calendars {
            let targets: Vec<&CalendarConfig> = config
                .calendars
                .iter()
                .filter(|c| c.email != source.email)
                .collect();

            match sync_calendar(&db, &client, client_id, client_secret, source, &targets).await {
                Ok(stats) => {
                    if stats.created > 0 || stats.updated > 0 || stats.deleted > 0 {
                        tracing::info!(
                            "{} -> {} created, {} updated, {} deleted",
                            source.email,
                            stats.created,
                            stats.updated,
                            stats.deleted
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("{} sync error: {:#}", source.email, e);
                }
            }
        }

        tracing::debug!(
            "Sync cycle done. Sleeping {}s.",
            config.poll_interval_seconds
        );
        tokio::time::sleep(Duration::from_secs(config.poll_interval_seconds)).await;
    }
}

async fn sync_calendar(
    db: &Database,
    client: &reqwest::Client,
    client_id: &str,
    client_secret: &str,
    source: &CalendarConfig,
    targets: &[&CalendarConfig],
) -> anyhow::Result<SyncStats> {
    let mut stats = SyncStats {
        created: 0,
        updated: 0,
        deleted: 0,
    };

    let mut source_token = TokenSet::load(&source.token_file)
        .with_context(|| format!("Failed to load token for {}", source.email))?;
    let source_access = calendar::ensure_fresh_token(
        client,
        client_id,
        client_secret,
        &mut source_token,
        &source.token_file,
    )
    .await
    .with_context(|| format!("Failed to get access token for {}", source.email))?;

    let mut target_access: HashMap<String, String> = HashMap::new();
    for target in targets {
        let mut token = TokenSet::load(&target.token_file)
            .with_context(|| format!("Failed to load token for {}", target.email))?;
        let access = calendar::ensure_fresh_token(
            client,
            client_id,
            client_secret,
            &mut token,
            &target.token_file,
        )
        .await
        .with_context(|| format!("Failed to get access token for {}", target.email))?;
        target_access.insert(target.email.clone(), access);
    }

    let source_color = calendar::mirror_color(&source.email, source.color_id.as_deref());

    let last_sync = db.get_last_sync(&source.email)?;
    let now = chrono::Utc::now();
    let is_first_sync = last_sync.is_none();

    if is_first_sync {
        let seed_color = calendar::mirror_color(&source.email, source.color_id.as_deref());
        seed_recurring_events(db, client, &source_access, &target_access, source, targets, &seed_color)
            .await
            .unwrap_or_else(|e| {
                tracing::error!("{} seed error: {:#}", source.email, e);
            });
    }

    // On first sync, look back 28 days to catch future events created earlier.
    // Google Calendar rejects updatedMin beyond ~30 days (410).
    let updated_min = last_sync.unwrap_or_else(|| {
        (now - chrono::Duration::days(28)).to_rfc3339()
    });

    let masters = calendar::list_events_sync(
        client,
        &source_access,
        &source.calendar_id,
        &updated_min,
        false,
    )
    .await
    .with_context(|| format!("Failed to list masters for {}", source.email))?;

    let instances = calendar::list_events_sync(
        client,
        &source_access,
        &source.calendar_id,
        &updated_min,
        true,
    )
    .await
    .with_context(|| format!("Failed to list instances for {}", source.email))?;

    let mut latest_updated = updated_min.clone();
    let mut processed_ids: HashSet<String> = HashSet::new();

    for event in &masters {
        if calendar::is_mirror_event(event) {
            continue;
        }
        if calendar::is_working_location(event) {
            continue;
        }

        let event_id = match event["id"].as_str() {
            Some(id) => id,
            None => continue,
        };

        if calendar::is_self_declined(event) {
            let mappings = db.get_mappings(&source.email, event_id)?;
            if !mappings.is_empty() {
                if let Err(e) = handle_deletion(
                    db, client, &target_access, source, event_id, targets,
                )
                .await
                {
                    tracing::error!(
                        "Failed to clean up mirrors for declined event {}: {:#}",
                        event_id,
                        e
                    );
                } else {
                    stats.deleted += mappings.len();
                }
            }
            continue;
        }

        processed_ids.insert(event_id.to_string());

        track_updated(event, &mut latest_updated);

        let status = event["status"].as_str().unwrap_or("confirmed");

        if status == "cancelled" {
            match handle_deletion(
                db,
                client,
                &target_access,
                source,
                event_id,
                targets,
            )
            .await
            {
                Ok(n) => stats.deleted += n,
                Err(e) => {
                    tracing::error!(
                        "Failed to delete mirrors for event {}: {:#}",
                        event_id,
                        e
                    )
                }
            }
        } else {
            let current_version = event["updated"].as_str().unwrap_or("");
            if let Some(stored) = db.get_event_version(&source.email, event_id)? {
                if stored == current_version {
                    continue;
                }
            }

            let mappings = db.get_mappings(&source.email, event_id)?;

            if mappings.is_empty() {
                match handle_new_event(
                    db,
                    client,
                    &target_access,
                    source,
                    event,
                    targets,
                    &source_color,
                )
                .await
                {
                    Ok(n) => stats.created += n,
                    Err(e) => {
                        tracing::error!(
                            "Failed to create mirrors for event {}: {:#}",
                            event_id,
                            e
                        )
                    }
                }
            } else {
                match handle_updated_event(
                    client,
                    &target_access,
                    event,
                    &mappings,
                    targets,
                    &source_color,
                )
                .await
                {
                    Ok(n) => stats.updated += n,
                    Err(e) => {
                        tracing::warn!(
                            "Update failed for event {} (mirror likely deleted): {:#}. Recreating...",
                            event_id,
                            e
                        );
                        if let Err(e2) = db.delete_mappings(&source.email, event_id) {
                            tracing::error!("Failed to clean up mappings: {:#}", e2);
                        }
                        match handle_new_event(
                            db, client, &target_access, source, event, targets,
                            &source_color,
                        )
                        .await
                        {
                            Ok(n) => stats.created += n,
                            Err(e2) => {
                                tracing::error!(
                                    "Failed to recreate mirrors for event {}: {:#}",
                                    event_id,
                                    e2
                                )
                            }
                        }
                    }
                }
            }

            let _ = db.set_event_version(
                &source.email,
                event_id,
                current_version,
            );
        }
    }

    for event in &instances {
        if calendar::is_mirror_event(event) {
            continue;
        }
        if calendar::is_working_location(event) {
            continue;
        }

        let event_id = match event["id"].as_str() {
            Some(id) => id,
            None => continue,
        };

        if processed_ids.contains(event_id) {
            continue;
        }

        let recurring_event_id = match event["recurringEventId"].as_str() {
            Some(id) => id,
            None => continue,
        };

        if processed_ids.contains(recurring_event_id) {
            continue;
        }

        processed_ids.insert(event_id.to_string());

        track_updated(event, &mut latest_updated);

        let status = event["status"].as_str().unwrap_or("confirmed");

        if status == "cancelled" {
            match handle_instance_cancellation(
                db,
                client,
                &target_access,
                source,
                recurring_event_id,
                event_id,
                targets,
            )
            .await
            {
                Ok(n) => stats.deleted += n,
                Err(e) => {
                    tracing::debug!(
                        "Could not cancel instance {}: {:#}",
                        event_id,
                        e
                    )
                }
            }
        } else {
            let mappings = db.get_mappings(&source.email, recurring_event_id)?;

            if mappings.is_empty() {
                match handle_new_recurring_from_instance(
                    db,
                    client,
                    &source_access,
                    &target_access,
                    source,
                    event,
                    recurring_event_id,
                    targets,
                    &source_color,
                )
                .await
                {
                    Ok(n) => stats.created += n,
                    Err(e) => {
                        tracing::error!(
                            "Failed to create recurring mirrors from instance {}: {:#}",
                            event_id,
                            e
                        )
                    }
                }
            }
        }
    }

    let new_sync_time = now.to_rfc3339();

    db.set_last_sync(&source.email, &new_sync_time)?;

    Ok(stats)
}

fn track_updated(event: &serde_json::Value, latest_updated: &mut String) {
    if let Some(ev_updated) = event["updated"].as_str() {
        if ev_updated > latest_updated.as_str() {
            *latest_updated = ev_updated.to_string();
        }
    }
}

fn mirror_instance_id(mirror_master_id: &str, source_instance_id: &str) -> Option<String> {
    let suffix = source_instance_id.splitn(2, '_').nth(1)?;
    Some(format!("{}_{}", mirror_master_id, suffix))
}

async fn handle_new_event(
    db: &Database,
    client: &reqwest::Client,
    target_access: &HashMap<String, String>,
    source: &CalendarConfig,
    event: &serde_json::Value,
    targets: &[&CalendarConfig],
    source_color: &str,
) -> anyhow::Result<usize> {
    let source_event_id = event["id"].as_str().context("Event missing id")?;

    let mut created = 0;

    for target in targets {
        let mirror_body = calendar::build_mirror_body(
            event,
            &source.email,
            source_color,
            &target.mirror_style,
        );

        let access_token = target_access
            .get(&target.email)
            .context("Target access token missing")?;

        let mirror_id = calendar::create_event(
            client,
            access_token,
            &target.calendar_id,
            &mirror_body,
        )
        .await
        .with_context(|| format!("Failed to create mirror on {}", target.email))?;

        db.save_mapping(&source.email, source_event_id, &target.email, &mirror_id)
            .context("Failed to save event mapping")?;

        tracing::debug!(
            "Created mirror {} -> {} (event {})",
            source.email,
            target.email,
            source_event_id
        );

        created += 1;
    }

    Ok(created)
}

async fn handle_updated_event(
    client: &reqwest::Client,
    target_access: &HashMap<String, String>,
    event: &serde_json::Value,
    mappings: &[MirrorMapping],
    targets: &[&CalendarConfig],
    source_color: &str,
) -> anyhow::Result<usize> {
    let mut updated = 0;

    let target_map: HashMap<&str, &CalendarConfig> =
        targets.iter().map(|t| (t.email.as_str(), *t)).collect();

    for mapping in mappings {
        let target = match target_map.get(mapping.target_calendar_id.as_str()) {
            Some(t) => t,
            None => continue,
        };

        let access_token = match target_access.get(mapping.target_calendar_id.as_str()) {
            Some(tok) => tok,
            None => continue,
        };

        let update_body = calendar::build_mirror_update(event, source_color, &target.mirror_style);

        calendar::update_event(
            client,
            access_token,
            &target.calendar_id,
            &mapping.target_event_id,
            &update_body,
        )
        .await
        .with_context(|| {
            format!(
                "Failed to update mirror on {} (event {})",
                target.email, mapping.target_event_id
            )
        })?;

        tracing::debug!(
            "Updated mirror {} (event {})",
            target.email,
            mapping.target_event_id
        );

        updated += 1;
    }

    Ok(updated)
}

async fn handle_deletion(
    db: &Database,
    client: &reqwest::Client,
    target_access: &HashMap<String, String>,
    source: &CalendarConfig,
    source_event_id: &str,
    targets: &[&CalendarConfig],
) -> anyhow::Result<usize> {
    let mappings = db.get_mappings(&source.email, source_event_id)?;

    if mappings.is_empty() {
        return Ok(0);
    }

    let target_map: HashMap<&str, &CalendarConfig> =
        targets.iter().map(|t| (t.email.as_str(), *t)).collect();

    let mut deleted = 0;

    for mapping in &mappings {
        let target = match target_map.get(mapping.target_calendar_id.as_str()) {
            Some(t) => t,
            None => continue,
        };

        let access_token = match target_access.get(mapping.target_calendar_id.as_str()) {
            Some(tok) => tok,
            None => continue,
        };

        match calendar::delete_event(
            client,
            access_token,
            &target.calendar_id,
            &mapping.target_event_id,
        )
        .await
        {
            Ok(()) => {
                tracing::debug!(
                    "Deleted mirror {} (event {})",
                    target.email,
                    mapping.target_event_id
                );
                deleted += 1;
            }
            Err(e) => {
                if format!("{}", e).contains("410") || format!("{}", e).contains("notFound") {
                    tracing::debug!(
                        "Mirror {} already deleted ({}), cleaning up mapping.",
                        target.email,
                        mapping.target_event_id
                    );
                    deleted += 1;
                } else {
                    tracing::error!(
                        "Failed to delete mirror on {} ({}): {:#}",
                        target.email,
                        mapping.target_event_id,
                        e
                    );
                }
            }
        }
    }

    db.delete_mappings(&source.email, source_event_id)?;

    Ok(deleted)
}

async fn handle_instance_cancellation(
    db: &Database,
    client: &reqwest::Client,
    target_access: &HashMap<String, String>,
    source: &CalendarConfig,
    recurring_event_id: &str,
    source_instance_id: &str,
    targets: &[&CalendarConfig],
) -> anyhow::Result<usize> {
    let mappings = db.get_mappings(&source.email, recurring_event_id)?;

    if mappings.is_empty() {
        return Ok(0);
    }

    let target_map: HashMap<&str, &CalendarConfig> =
        targets.iter().map(|t| (t.email.as_str(), *t)).collect();

    let mut cancelled = 0;

    for mapping in &mappings {
        let target = match target_map.get(mapping.target_calendar_id.as_str()) {
            Some(t) => t,
            None => continue,
        };

        let access_token = match target_access.get(mapping.target_calendar_id.as_str()) {
            Some(tok) => tok,
            None => continue,
        };

        let mirror_instance = match mirror_instance_id(&mapping.target_event_id, source_instance_id)
        {
            Some(id) => id,
            None => {
                tracing::debug!(
                    "Could not compute mirror instance ID from source instance {}",
                    source_instance_id
                );
                continue;
            }
        };

        let cancel_body = serde_json::json!({"status": "cancelled"});

        match calendar::update_event(
            client,
            access_token,
            &target.calendar_id,
            &mirror_instance,
            &cancel_body,
        )
        .await
        {
            Ok(()) => {
                tracing::debug!(
                    "Cancelled mirror instance {} on {}",
                    mirror_instance,
                    target.email
                );
                cancelled += 1;
            }
            Err(e) => {
                let err_str = format!("{}", e);
                if err_str.contains("410") || err_str.contains("notFound") {
                    tracing::debug!(
                        "Mirror instance {} already cancelled on {}",
                        mirror_instance,
                        target.email
                    );
                    cancelled += 1;
                } else {
                    tracing::error!(
                        "Failed to cancel mirror instance {} on {}: {:#}",
                        mirror_instance,
                        target.email,
                        e
                    );
                }
            }
        }
    }

    Ok(cancelled)
}

async fn handle_new_recurring_from_instance(
    db: &Database,
    client: &reqwest::Client,
    source_access: &str,
    target_access: &HashMap<String, String>,
    source: &CalendarConfig,
    _instance: &serde_json::Value,
    recurring_event_id: &str,
    targets: &[&CalendarConfig],
    source_color: &str,
) -> anyhow::Result<usize> {
    let master_event = calendar::get_event(
        client,
        source_access,
        &source.calendar_id,
        recurring_event_id,
    )
    .await
    .with_context(|| format!("Failed to fetch master event {}", recurring_event_id))?;

    if calendar::is_mirror_event(&master_event) {
        return Ok(0);
    }

    if master_event["status"].as_str() == Some("cancelled") {
        let mappings = db.get_mappings(&source.email, recurring_event_id)?;
        if !mappings.is_empty() {
            return handle_deletion(
                db,
                client,
                target_access,
                source,
                recurring_event_id,
                targets,
            )
            .await;
        }
        return Ok(0);
    }

    let mut created = 0;

    for target in targets {
        let mirror_body = calendar::build_mirror_body(
            &master_event,
            &source.email,
            source_color,
            &target.mirror_style,
        );

        let access_token = target_access
            .get(&target.email)
            .context("Target access token missing")?;

        let mirror_id = calendar::create_event(
            client,
            access_token,
            &target.calendar_id,
            &mirror_body,
        )
        .await
        .with_context(|| {
            format!(
                "Failed to create recurring mirror on {} (from instance {})",
                target.email, recurring_event_id
            )
        })?;

        db.save_mapping(&source.email, recurring_event_id, &target.email, &mirror_id)
            .context("Failed to save recurring event mapping")?;

        tracing::debug!(
            "Created recurring mirror {} -> {} (master {})",
            source.email,
            target.email,
            recurring_event_id
        );

        created += 1;
    }

    Ok(created)
}

async fn seed_recurring_events(
    db: &Database,
    client: &reqwest::Client,
    source_access: &str,
    target_access: &HashMap<String, String>,
    source: &CalendarConfig,
    targets: &[&CalendarConfig],
    source_color: &str,
) -> anyhow::Result<()> {
    let now = chrono::Utc::now();
    let four_weeks = now + chrono::Duration::weeks(4);
    let time_min = now.to_rfc3339();
    let time_max = four_weeks.to_rfc3339();

    tracing::info!(
        "{} first sync: scanning for recurring events in the next 4 weeks",
        source.email
    );

    let instances = calendar::list_instances_forward(
        client,
        source_access,
        &source.calendar_id,
        &time_min,
        &time_max,
    )
    .await
    .with_context(|| format!("Failed to scan forward for {}", source.email))?;

    let mut seen_masters: HashSet<String> = HashSet::new();

    for instance in &instances {
        let recurring_event_id = match instance["recurringEventId"].as_str() {
            Some(id) => id,
            None => continue,
        };

        if seen_masters.contains(recurring_event_id) {
            continue;
        }
        seen_masters.insert(recurring_event_id.to_string());

        let mappings = db.get_mappings(&source.email, recurring_event_id)?;
        if !mappings.is_empty() {
            continue;
        }

        let master = match calendar::get_event(
            client,
            source_access,
            &source.calendar_id,
            recurring_event_id,
        )
        .await
        {
            Ok(m) => m,
            Err(e) => {
                tracing::debug!(
                    "Could not fetch master {} for {}: {:#}",
                    recurring_event_id,
                    source.email,
                    e
                );
                continue;
            }
        };

        if calendar::is_mirror_event(&master) {
            continue;
        }

        if calendar::is_working_location(&master) {
            continue;
        }

        if master["status"].as_str() == Some("cancelled") {
            continue;
        }

        for target in targets {
            let mirror_body = calendar::build_mirror_body(
                &master,
                &source.email,
                source_color,
                &target.mirror_style,
            );

            let access_token = match target_access.get(&target.email) {
                Some(t) => t,
                None => continue,
            };

            match calendar::create_event(
                client,
                access_token,
                &target.calendar_id,
                &mirror_body,
            )
            .await
            {
                Ok(mirror_id) => {
                    let _ = db.save_mapping(
                        &source.email,
                        recurring_event_id,
                        &target.email,
                        &mirror_id,
                    );
                    tracing::info!(
                        "Seeded recurring mirror {} -> {} (master {})",
                        source.email,
                        target.email,
                        recurring_event_id
                    );
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to seed mirror on {} for {}: {:#}",
                        target.email,
                        recurring_event_id,
                        e
                    );
                }
            }
        }
    }

    Ok(())
}

async fn migrate_mirror_style(
    db: &Database,
    client: &reqwest::Client,
    config: &crate::config::HipoglosConfig,
    client_id: &str,
    client_secret: &str,
) -> anyhow::Result<usize> {
    let mappings = db.get_all_mappings()?;
    if mappings.is_empty() {
        return Ok(0);
    }

    tracing::info!(
        "Migrating style for {} existing mirror events...",
        mappings.len()
    );

    let cal_configs: HashMap<&str, &CalendarConfig> = config
        .calendars
        .iter()
        .map(|c| (c.email.as_str(), c))
        .collect();

    let mut access_tokens: HashMap<String, String> = HashMap::new();

    let mut updated = 0;

    for mapping in &mappings {
        let source_cfg = match cal_configs.get(mapping.source_calendar_id.as_str()) {
            Some(c) => *c,
            None => continue,
        };
        let target_cfg = match cal_configs.get(mapping.target_calendar_id.as_str()) {
            Some(c) => *c,
            None => continue,
        };

        let target_access = match access_tokens.get(&target_cfg.email) {
            Some(t) => t.clone(),
            None => {
                let mut token = TokenSet::load(&target_cfg.token_file)?;
                let access = calendar::ensure_fresh_token(
                    client,
                    client_id,
                    client_secret,
                    &mut token,
                    &target_cfg.token_file,
                )
                .await?;
                access_tokens.insert(target_cfg.email.clone(), access.clone());
                access
            }
        };

        let mirror = match calendar::get_event(
            client,
            &target_access,
            &target_cfg.calendar_id,
            &mapping.target_event_id,
        )
        .await
        {
            Ok(e) => e,
            Err(e) => {
                let err = format!("{:#}", e);
                if err.contains("410") || err.contains("notFound") {
                    tracing::debug!(
                        "Mirror event {} no longer exists, skipping migration.",
                        mapping.target_event_id
                    );
                } else {
                    tracing::error!(
                        "Failed to fetch mirror {} for migration: {}",
                        mapping.target_event_id,
                        e
                    );
                }
                continue;
            }
        };

        let expected_color =
            calendar::mirror_color(&source_cfg.email, source_cfg.color_id.as_deref());
        let current_color = mirror["colorId"].as_str().unwrap_or("");
        if current_color == expected_color {
            continue;
        }

        let source_access = match access_tokens.get(&source_cfg.email) {
            Some(t) => t.clone(),
            None => {
                let mut token = TokenSet::load(&source_cfg.token_file)?;
                let access = calendar::ensure_fresh_token(
                    client,
                    client_id,
                    client_secret,
                    &mut token,
                    &source_cfg.token_file,
                )
                .await?;
                access_tokens.insert(source_cfg.email.clone(), access.clone());
                access
            }
        };

        let source_event = match calendar::get_event(
            client,
            &source_access,
            &source_cfg.calendar_id,
            &mapping.source_event_id,
        )
        .await
        {
            Ok(e) => e,
            Err(_e) => {
                tracing::debug!(
                    "Source event {} no longer exists, skipping mirror update.",
                    mapping.source_event_id
                );
                continue;
            }
        };

        let migr_color =
            calendar::mirror_color(&source_cfg.email, source_cfg.color_id.as_deref());
        let new_body = calendar::build_mirror_body(
            &source_event,
            &source_cfg.email,
            &migr_color,
            &target_cfg.mirror_style,
        );

        match calendar::update_event(
            client,
            &target_access,
            &target_cfg.calendar_id,
            &mapping.target_event_id,
            &new_body,
        )
        .await
        {
            Ok(()) => {
                updated += 1;
                tracing::debug!(
                    "Migrated mirror {} on {}",
                    mapping.target_event_id,
                    target_cfg.email
                );
            }
            Err(e) => {
                tracing::error!(
                    "Failed to migrate mirror {}: {:#}",
                    mapping.target_event_id,
                    e
                );
            }
        }
    }

    Ok(updated)
}

async fn migrate_mirror_content(
    db: &Database,
    client: &reqwest::Client,
    config: &crate::config::HipoglosConfig,
    client_id: &str,
    client_secret: &str,
) -> anyhow::Result<usize> {
    let mappings = db.get_all_mappings()?;
    if mappings.is_empty() {
        return Ok(0);
    }

    tracing::info!(
        "Deleting {} existing mirror events for recreation with updated styles...",
        mappings.len()
    );

    let cal_configs: HashMap<&str, &CalendarConfig> = config
        .calendars
        .iter()
        .map(|c| (c.email.as_str(), c))
        .collect();

    let mut access_tokens: HashMap<String, String> = HashMap::new();
    let mut deleted = 0;

    for mapping in &mappings {
        let target_cfg = match cal_configs.get(mapping.target_calendar_id.as_str()) {
            Some(c) => *c,
            None => continue,
        };

        let target_access = match access_tokens.get(&target_cfg.email) {
            Some(t) => t.clone(),
            None => {
                let mut token = TokenSet::load(&target_cfg.token_file)?;
                let access = calendar::ensure_fresh_token(
                    client,
                    client_id,
                    client_secret,
                    &mut token,
                    &target_cfg.token_file,
                )
                .await?;
                access_tokens.insert(target_cfg.email.clone(), access.clone());
                access
            }
        };

        match calendar::delete_event(
            client,
            &target_access,
            &target_cfg.calendar_id,
            &mapping.target_event_id,
        )
        .await
        {
            Ok(()) => {
                deleted += 1;
                tracing::debug!(
                    "Deleted mirror {} on {}",
                    mapping.target_event_id,
                    target_cfg.email
                );
            }
            Err(e) => {
                let err_str = format!("{:#}", e);
                if err_str.contains("410") || err_str.contains("notFound") {
                    tracing::debug!(
                        "Mirror {} already gone on {}, counting as deleted.",
                        mapping.target_event_id,
                        target_cfg.email
                    );
                    deleted += 1;
                } else {
                    tracing::error!(
                        "Failed to delete mirror {} on {}: {}",
                        mapping.target_event_id,
                        target_cfg.email,
                        e
                    );
                }
            }
        }
    }

    db.clear_all_mirror_data()?;
    tracing::info!(
        "Cleared all mirror state. {} events will be recreated on next sync.",
        deleted
    );

    Ok(deleted)
}

fn color_fingerprint(config: &crate::config::HipoglosConfig) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for cal in &config.calendars {
        cal.email.hash(&mut h);
        cal.color_id.hash(&mut h);
    }
    format!("{:x}", h.finish())
}

fn mirror_content_fingerprint(config: &crate::config::HipoglosConfig) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for cal in &config.calendars {
        cal.email.hash(&mut h);
        cal.mirror_style.hash(&mut h);
    }
    format!("{:x}", h.finish())
}
