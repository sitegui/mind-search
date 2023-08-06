mod download_pages;
mod extract_firefox_history;
mod index_contents;
mod search;

use crate::download_pages::download_pages;
use crate::extract_firefox_history::extract_firefox_history;
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Simple program to greet a person
#[derive(Parser, Debug)]
enum ProgramArguments {
    /// Extract your browser history information into a JSON file
    ExtractFirefoxHistory {
        /// The path to your Firefox profile. You can obtain it in the page "about:profiles" in your
        /// Firefox
        profile_path: PathBuf,
    },
    /// Download all pages that it can from your extracted history
    DownloadPages {
        /// How many requests to do at once
        #[arg(long, default_value_t = 10)]
        parallelism: usize,
        /// Time maximum time to wait for each page to answer
        #[arg(long, default_value_t = 10)]
        timeout_seconds: u64,
        /// How many pages to store in each bundle
        #[arg(long, default_value_t = 500)]
        bundle_size: usize,
    },
    /// Read the raw pages to extract the readable text and index it for search
    IndexContents,
    /// Search the indexed content
    Search { query: String },
}

fn main() -> anyhow::Result<()> {
    let args = ProgramArguments::parse();

    match args {
        ProgramArguments::ExtractFirefoxHistory { profile_path } => {
            extract_firefox_history(profile_path)
        }
        ProgramArguments::DownloadPages {
            parallelism,
            timeout_seconds,
            bundle_size,
        } => download_pages(
            parallelism,
            Duration::from_secs(timeout_seconds),
            bundle_size,
        ),
        ProgramArguments::IndexContents => index_contents::index_contents(),
        ProgramArguments::Search { query } => search::search(query),
    }
}

const FIREFOX_DATABASE_PATH: &str = "data/places.sqlite";
const HISTORY_PATH: &str = "data/history";
const RAW_PAGES_DIR_PATH: &str = "data/raw_pages";
const TANTIVY_INDEX_DIR_PATH: &str = "data/tantivy_index";

#[derive(Deserialize, Serialize)]
struct FirefoxHistoryItem {
    url: String,
    /// The page title, if this information is available
    title: Option<String>,
    /// When this page was last visited
    last_visit: Option<DateTime<Utc>>,
}

#[derive(Deserialize, Serialize)]
struct DownloadedPage {
    url: String,
    loaded_at: DateTime<Utc>,
    content: DownloadedPageContent,
}

#[derive(Deserialize, Serialize)]
enum DownloadedPageContent {
    Failure(String),
    Html(String),
}

fn write_compressed_json<T: Serialize>(path: &Path, content: &T) -> anyhow::Result<()> {
    let file_writer = File::create(path)?;
    let compressor_writer = zstd::Encoder::new(file_writer, 0)?.auto_finish();
    serde_json::to_writer(compressor_writer, content)?;
    Ok(())
}

fn read_compressed_json<T: DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let file_reader = File::open(path)?;
    let compressor_reader = zstd::Decoder::new(file_reader)?;
    let content = serde_json::from_reader(compressor_reader)?;
    Ok(content)
}

fn list_raw_pages_bundles() -> anyhow::Result<Vec<PathBuf>> {
    fs::create_dir_all(RAW_PAGES_DIR_PATH)?;

    let mut bundles = Vec::new();
    for maybe_entry in fs::read_dir(RAW_PAGES_DIR_PATH)? {
        let entry_path = maybe_entry?.path();
        bundles.push(entry_path);
    }
    Ok(bundles)
}
