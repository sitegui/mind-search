use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use ego_tree::NodeRef;
use rayon::prelude::*;
use reqwest::blocking::Client;
use rusqlite::{Connection, Row};
use scraper::{Html, Node};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;
use std::{fs, thread};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{Document, Index, ReloadPolicy};

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
        #[arg(long, default_value_t = 15)]
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
        ProgramArguments::IndexContents => index_contents(),
        ProgramArguments::Search { query } => search(query),
    }
}

fn extract_firefox_history(profile_path: PathBuf) -> anyhow::Result<()> {
    // Create a temporary copy of the SQLite database file.
    // This is necessary because Firefox locks the database while it's running.
    fs::create_dir_all("data")?;
    let db_path = "data/places.sqlite";
    fs::copy(profile_path.join("places.sqlite"), db_path)?;
    println!("Copied Firefox database");

    // Open the SQLite database.
    let conn = Connection::open(db_path)?;

    // Execute a query to read the browsing history.
    let mut statement = conn.prepare("SELECT url, title, last_visit_date FROM moz_places")?;

    /// Convert each row for the query above into a Rust struct
    fn convert_firefox_history_row(row: &Row) -> anyhow::Result<FirefoxHistoryItem> {
        let url = row.get("url")?;

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

    // Iterate over the anyhow::Results and convert them
    let mut history = Vec::new();
    for maybe_item in statement.query_and_then([], convert_firefox_history_row)? {
        let item = maybe_item?;
        history.push(item);
    }
    println!("Extracted {} visited URLs", history.len());

    write_compressed_json(Path::new("data/history"), &history)?;
    println!("Wrote history to disk");

    Ok(())
}

/// Download all the pages into
fn download_pages(parallelism: usize, timeout: Duration, bundle_size: usize) -> anyhow::Result<()> {
    fs::create_dir_all("data/raw_pages")?;

    // Detect the pages that were already loaded
    let downloaded_urls = Mutex::new(HashSet::new());
    let mut raw_page_paths = Vec::new();
    for maybe_entry in fs::read_dir("data/raw_pages")? {
        let entry = maybe_entry?;
        raw_page_paths.push(entry.path());
    }
    raw_page_paths
        .into_par_iter()
        .try_for_each(|path| -> anyhow::Result<()> {
            let downloaded_pages: Vec<DownloadedPage> = read_compressed_json(&path)?;
            let mut downloaded_urls = downloaded_urls.lock().unwrap();
            for page in downloaded_pages {
                downloaded_urls.insert(page.url);
            }
            Ok(())
        })?;
    let downloaded_urls = downloaded_urls.into_inner().unwrap();
    println!(
        "Detected that {} URLs were already downloaded",
        downloaded_urls.len()
    );

    // Detect the pages that need to be downloaded
    let mut history: Vec<FirefoxHistoryItem> = read_compressed_json(Path::new("data/history"))?;
    println!("Read history with {} URLs", history.len());
    history.retain(|item| !downloaded_urls.contains(&item.url));
    println!("Prepare to download {} URLs", history.len());

    let history_queue = Mutex::new(history);
    let http_client = Client::builder().timeout(timeout).build()?;

    thread::scope(|scope| -> anyhow::Result<()> {
        // Start all the threads to do the heavy work
        let mut threads = Vec::new();
        for _ in 0..parallelism {
            let thread_handle =
                scope.spawn(|| download_pages_thread(bundle_size, &history_queue, &http_client));
            threads.push(thread_handle);
        }

        // Wait for all threads and propagate errors
        for thread in threads {
            thread.join().unwrap()?;
        }

        Ok(())
    })?;

    Ok(())
}

/// Represent each thread that downloads pages
fn download_pages_thread(
    bundle_size: usize,
    history_queue: &Mutex<Vec<FirefoxHistoryItem>>,
    http_client: &Client,
) -> anyhow::Result<()> {
    let mut downloaded_pages = Vec::new();

    /// Write the downloaded pages into the disk, cleaning the whole list
    fn write_downloaded_pages(downloaded_pages: &mut Vec<DownloadedPage>) -> anyhow::Result<()> {
        if !downloaded_pages.is_empty() {
            let timestamp = Utc::now().timestamp_nanos();
            let file_name = format!("data/raw_pages/{}", timestamp);
            write_compressed_json(Path::new(&file_name), downloaded_pages)?;
            downloaded_pages.clear();
            println!("Wrote bundle to {}", file_name);
        }

        Ok(())
    }

    loop {
        // Obtain the next item from the queue
        let next_item;
        let remaining_items;
        {
            let mut history_queue = history_queue.lock().unwrap();
            next_item = history_queue.pop();
            remaining_items = history_queue.len();
        }

        if remaining_items > 0 && remaining_items % 1_000 == 0 {
            println!("{} URLs remaining", remaining_items);
        }

        // Download page
        match next_item {
            None => break,
            Some(next_item) => {
                let page = download_page(http_client, next_item.url);
                downloaded_pages.push(page);

                if downloaded_pages.len() >= bundle_size {
                    write_downloaded_pages(&mut downloaded_pages)?;
                }
            }
        }
    }

    write_downloaded_pages(&mut downloaded_pages)?;
    Ok(())
}

fn download_page(http_client: &Client, url: String) -> DownloadedPage {
    let content = match try_download_page(http_client, &url) {
        Ok(content) => content,
        Err(error) => DownloadedPageContent::Failure(error.to_string()),
    };

    DownloadedPage {
        url,
        loaded_at: Utc::now(),
        content,
    }
}

fn try_download_page(http_client: &Client, url: &str) -> anyhow::Result<DownloadedPageContent> {
    let response = http_client.get(url).send()?.error_for_status()?;

    let is_html = response
        .headers()
        .get("Content-Type")
        .and_then(|value| value.to_str().ok())
        .map(|content_type| content_type.starts_with("text/html"))
        .unwrap_or(false);

    if is_html {
        let content = response.text()?;
        Ok(DownloadedPageContent::Html(content))
    } else {
        Ok(DownloadedPageContent::Failure(
            "Page is not HTML".to_string(),
        ))
    }
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

fn index_contents() -> anyhow::Result<()> {
    let index_path = "data/tantivy-index";
    fs::create_dir_all(index_path)?;

    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("url", TEXT | STORED);
    schema_builder.add_text_field("content", TEXT);
    let schema = schema_builder.build();

    let index = Index::create_in_dir(index_path, schema.clone())?;
    let mut index_writer = index.writer(50_000_000)?;
    let url_field = schema.get_field("url").unwrap();
    let content_field = schema.get_field("content").unwrap();

    for maybe_entry in fs::read_dir("data/raw_pages")? {
        let entry_path = maybe_entry?.path();
        let downloaded_pages: Vec<DownloadedPage> = read_compressed_json(&entry_path)?;
        println!(
            "Read {} pages from {}",
            downloaded_pages.len(),
            entry_path.display()
        );

        for page in downloaded_pages {
            if let DownloadedPageContent::Html(html_source) = page.content {
                let readable_text = extract_readable_text(&html_source);

                let mut document = Document::default();
                document.add_text(url_field, page.url);
                document.add_text(content_field, readable_text);
                index_writer.add_document(document)?;
            }
        }
    }

    index_writer.commit()?;

    Ok(())
}

fn extract_readable_text(html_source: &str) -> String {
    let document = Html::parse_document(html_source);
    let mut readable_text = String::new();

    fn recurse_page_tree(human_text: &mut String, node: &NodeRef<Node>) {
        match node.value() {
            Node::Text(text) => {
                human_text.push_str(text);
            }
            Node::Element(element) => {
                if element.name() != "script" && element.name() != "style" {
                    for child in node.children() {
                        recurse_page_tree(human_text, &child);
                    }
                }
            }
            _ => {}
        }
    }

    recurse_page_tree(&mut readable_text, &document.root_element());

    readable_text
}

fn search(query: String) -> anyhow::Result<()> {
    let index_path = "data/tantivy-index";
    fs::create_dir_all(index_path)?;

    let mut schema_builder = Schema::builder();
    schema_builder.add_text_field("url", TEXT | STORED);
    schema_builder.add_text_field("content", TEXT);
    let schema = schema_builder.build();
    let url_field = schema.get_field("url").unwrap();
    let content_field = schema.get_field("content").unwrap();

    let index = Index::open_in_dir(index_path)?;

    let reader = index
        .reader_builder()
        .reload_policy(ReloadPolicy::OnCommit)
        .try_into()?;
    let searcher = reader.searcher();
    let query_parser = QueryParser::for_index(&index, vec![url_field, content_field]);
    let query = query_parser.parse_query(&query)?;
    let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;
    for (_score, doc_address) in top_docs {
        let retrieved_doc = searcher.doc(doc_address)?;
        println!("{}", schema.to_json(&retrieved_doc));
    }
    Ok(())
}
