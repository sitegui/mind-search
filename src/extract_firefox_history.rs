use crate::{write_compressed_json, FirefoxHistoryItem, FIREFOX_DATABASE_PATH, HISTORY_PATH};
use chrono::{TimeZone, Utc};
use reqwest::Url;
use rusqlite::{Connection, Row};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

pub fn extract_firefox_history(profile_path: PathBuf) -> anyhow::Result<()> {
    // Create a temporary copy of the SQLite database file.
    // This is necessary because Firefox locks the database while it's running.
    fs::create_dir_all("data")?;
    fs::copy(profile_path.join("places.sqlite"), FIREFOX_DATABASE_PATH)?;
    println!("Copied Firefox database");

    // Open the SQLite database.
    let conn = Connection::open(FIREFOX_DATABASE_PATH)?;

    // Execute a query to read the browsing history.
    let mut statement = conn.prepare("SELECT url, title, last_visit_date FROM moz_places")?;

    /// Convert each row for the query above into a Rust struct
    fn convert_firefox_history_row(row: &Row) -> anyhow::Result<FirefoxHistoryItem> {
        // Remove the "fragment" part of the URL. For example:
        // "https://docs.rs/url/2.4.0/url/struct.Url.html#impl-Serialize-for-Url" becomes
        // "https://docs.rs/url/2.4.0/url/struct.Url.html"
        let url: String = row.get("url")?;
        let mut parsed_url = Url::parse(&url)?;
        parsed_url.set_fragment(None);
        let url = parsed_url.to_string();

        let title = row.get("title")?;

        let last_visit_date: Option<i64> = row.get("last_visit_date")?;
        let last_visit =
            last_visit_date.map(|last_visit_date| Utc.timestamp_nanos(last_visit_date * 1000));

        Ok(FirefoxHistoryItem {
            url,
            title,
            last_visit,
        })
    }

    // Iterate over the query results and convert the rows
    let mut history_by_url: HashMap<String, FirefoxHistoryItem> = HashMap::new();
    for maybe_item in statement.query_and_then([], convert_firefox_history_row)? {
        let item = maybe_item?;

        match history_by_url.entry(item.url.clone()) {
            Entry::Occupied(mut occupied) => {
                let previous = occupied.get_mut();
                if previous.title.is_none() {
                    previous.title = item.title;
                }
                previous.last_visit = match (previous.last_visit, item.last_visit) {
                    (Some(previous_last_visit), Some(new_last_visit)) => {
                        Some(previous_last_visit.max(new_last_visit))
                    }
                    (Some(last_visit), None) | (None, Some(last_visit)) => Some(last_visit),
                    (None, None) => None,
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(item);
            }
        }
    }
    let history: Vec<_> = history_by_url.into_values().collect();
    println!("Extracted {} visited URLs", history.len());

    write_compressed_json(Path::new(HISTORY_PATH), &history)?;
    println!("Wrote history to disk");

    Ok(())
}
