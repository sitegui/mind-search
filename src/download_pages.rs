use crate::{
    list_raw_pages_bundles, read_compressed_json, write_compressed_json, DownloadedPage,
    DownloadedPageContent, FirefoxHistoryItem, HISTORY_PATH, RAW_PAGES_DIR_PATH,
};
use chrono::Utc;
use rayon::prelude::*;
use reqwest::blocking::Client;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

/// Download all the pages into
pub fn download_pages(
    parallelism: usize,
    timeout: Duration,
    bundle_size: usize,
) -> anyhow::Result<()> {
    // Detect the pages that were already loaded
    let bundles = list_raw_pages_bundles()?;
    let downloaded_urls = Mutex::new(HashSet::new());
    bundles
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
    let mut history: Vec<FirefoxHistoryItem> = read_compressed_json(Path::new(HISTORY_PATH))?;
    println!("Read history with {} URLs", history.len());
    history.retain(|item| !downloaded_urls.contains(&item.url));
    println!("Prepare to download {} URLs", history.len());

    let history_queue = Mutex::new(history);

    thread::scope(|scope| -> anyhow::Result<()> {
        // Start all the threads to do the heavy work
        let mut threads = Vec::new();
        for _ in 0..parallelism {
            let thread_handle =
                scope.spawn(|| download_pages_thread(timeout, bundle_size, &history_queue));
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
    timeout: Duration,
    bundle_size: usize,
    history_queue: &Mutex<Vec<FirefoxHistoryItem>>,
) -> anyhow::Result<()> {
    let mut downloaded_pages = Vec::new();
    let http_client = Client::builder().timeout(timeout).build()?;

    /// Write the downloaded pages into the disk, cleaning the whole list
    fn write_downloaded_pages(downloaded_pages: &mut Vec<DownloadedPage>) -> anyhow::Result<()> {
        if !downloaded_pages.is_empty() {
            let timestamp = Utc::now().timestamp_nanos();
            let file_name = format!("{}/{}", RAW_PAGES_DIR_PATH, timestamp);
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
                let page = download_page(&http_client, next_item.url);
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
