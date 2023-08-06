use crate::{
    list_raw_pages_bundles, read_compressed_json, DownloadedPage, DownloadedPageContent,
    FirefoxHistoryItem, HISTORY_PATH, TANTIVY_INDEX_DIR_PATH,
};
use ego_tree::NodeRef;
use rayon::prelude::*;
use scraper::{Html, Node};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tantivy::directory::MmapDirectory;
use tantivy::schema::{Schema, STORED, TEXT};
use tantivy::{DateTime, Document, Index};

pub fn index_contents() -> anyhow::Result<()> {
    let history: Vec<FirefoxHistoryItem> = read_compressed_json(Path::new(HISTORY_PATH))?;
    let history_by_url: HashMap<_, _> = history
        .into_iter()
        .map(|item| (item.url.clone(), item))
        .collect();

    fs::create_dir_all(TANTIVY_INDEX_DIR_PATH)?;

    let mut schema_builder = Schema::builder();
    let url_field = schema_builder.add_text_field("url", TEXT | STORED);
    let title_field = schema_builder.add_text_field("title", TEXT | STORED);
    let last_visit_field = schema_builder.add_date_field("last_visit", STORED);
    let content_field = schema_builder.add_text_field("content", TEXT | STORED);
    let schema = schema_builder.build();

    let index_directory = MmapDirectory::open(TANTIVY_INDEX_DIR_PATH)?;
    let index = Index::open_or_create(index_directory, schema)?;
    let mut index_writer = index.writer(1024 * 1024 * 1024)?;
    index_writer.delete_all_documents()?;

    let bundles = list_raw_pages_bundles()?;
    bundles
        .into_par_iter()
        .try_for_each(|bundle| -> anyhow::Result<()> {
            let downloaded_pages: Vec<DownloadedPage> = read_compressed_json(&bundle)?;
            let total_pages = downloaded_pages.len();
            let mut indexed_pages = 0;

            for page in downloaded_pages {
                if let DownloadedPageContent::Html(html_source) = page.content {
                    let extracted_text = extract_readable_text(&html_source);

                    let mut document = Document::default();

                    let history_item = history_by_url.get(&page.url);
                    if let Some(title) = decide_title(history_item, extracted_text.title) {
                        document.add_field_value(title_field, title);
                    }

                    if let Some(last_visit) = decide_last_visit(history_item) {
                        document.add_field_value(last_visit_field, last_visit);
                    }

                    document.add_field_value(url_field, page.url);
                    document.add_field_value(content_field, extracted_text.content);

                    index_writer.add_document(document)?;
                    indexed_pages += 1;
                }
            }

            println!(
                "Indexed {} out of {} pages from {}",
                indexed_pages,
                total_pages,
                bundle.display()
            );

            Ok(())
        })?;

    index_writer.commit()?;

    Ok(())
}

fn decide_title(
    history_item: Option<&FirefoxHistoryItem>,
    extracted_title: Option<String>,
) -> Option<String> {
    let history_title = match history_item {
        None => None,
        Some(history_item) => history_item.title.clone(),
    };

    match (history_title, extracted_title) {
        (None, None) => None,
        (Some(title), None) | (None, Some(title)) => Some(title),
        (Some(history_title), Some(extracted_title)) => {
            if history_title == extracted_title {
                Some(history_title)
            } else {
                Some(format!("{}, {}", history_title, extracted_title))
            }
        }
    }
}

fn decide_last_visit(item: Option<&FirefoxHistoryItem>) -> Option<DateTime> {
    let item = item?;
    let last_visit = item.last_visit?;
    let timestamp = last_visit.timestamp_millis();
    Some(DateTime::from_timestamp_millis(timestamp))
}

struct ExtractedText {
    title: Option<String>,
    content: String,
}

fn extract_readable_text(html_source: &str) -> ExtractedText {
    let document = Html::parse_document(html_source);
    let mut extracted = ExtractedText {
        title: None,
        content: String::new(),
    };

    fn recurse_page_tree(extracted: &mut ExtractedText, node: &NodeRef<Node>) {
        match node.value() {
            Node::Text(text) => {
                extracted.content.push_str(text);
            }
            Node::Element(element) => {
                let element_name = element.name();

                if element_name == "title" && extracted.title.is_none() {
                    let mut title = String::new();
                    for sub_node in node.descendants() {
                        if let Some(text) = sub_node.value().as_text() {
                            title.push_str(text);
                        }
                    }
                    extracted.title = Some(title);
                } else if element_name != "script" && element_name != "style" {
                    for child in node.children() {
                        recurse_page_tree(extracted, &child);
                    }
                }
            }
            _ => {}
        }
    }

    recurse_page_tree(&mut extracted, &document.root_element());

    extracted
}
