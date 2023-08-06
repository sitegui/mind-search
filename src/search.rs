use crate::TANTIVY_INDEX_DIR_PATH;
use anyhow::Context;
use chrono::{TimeZone, Utc};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::{Index, SnippetGenerator};

pub fn search(query: String) -> anyhow::Result<()> {
    let index = Index::open_in_dir(TANTIVY_INDEX_DIR_PATH)?;
    let schema = index.schema();
    let url_field = schema.get_field("url")?;
    let title_field = schema.get_field("title")?;
    let last_visit_field = schema.get_field("last_visit")?;
    let content_field = schema.get_field("content")?;

    let reader = index.reader()?;
    let searcher = reader.searcher();
    let mut query_parser =
        QueryParser::for_index(&index, vec![url_field, title_field, content_field]);
    query_parser.set_field_fuzzy(content_field, false, 1, true);

    let query = query_parser.parse_query(&query)?;
    let top_hits = searcher.search(&query, &TopDocs::with_limit(10))?;

    let snippet_generator = SnippetGenerator::create(&searcher, &query, content_field)?;

    for (index, (_score, hit_id)) in top_hits.into_iter().enumerate() {
        let document = searcher.doc(hit_id)?;

        let url = document
            .get_first(url_field)
            .and_then(|url| url.as_text())
            .context("missing url")?;
        let title = document
            .get_first(title_field)
            .and_then(|title| title.as_text());
        let last_visit = document
            .get_first(last_visit_field)
            .and_then(|last_visit| last_visit.as_date());
        let content = document
            .get_first(content_field)
            .and_then(|content| content.as_text())
            .context("missing content")?;

        let snippet = snippet_generator.snippet(content);

        println!("{}. {}", index + 1, url);
        if let Some(title) = title {
            println!("  Title: {}", title);
        }
        match last_visit {
            None => println!("  Last visit: unknown"),
            Some(last_visit) => {
                let timestamp = last_visit.into_timestamp_millis();
                let date = Utc
                    .timestamp_millis_opt(timestamp)
                    .single()
                    .context("failed to convert date")?;
                println!("  Last visit: {}", date)
            }
        }
        println!("{}\n", snippet.to_html());
    }

    Ok(())
}
