use crate::TANTIVY_INDEX_DIR_PATH;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::Index;

pub fn search(query: String) -> anyhow::Result<()> {
    let index = Index::open_in_dir(TANTIVY_INDEX_DIR_PATH)?;
    let schema = index.schema();
    let url_field = schema.get_field("url").unwrap();
    let content_field = schema.get_field("content").unwrap();

    let reader = index.reader()?;
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
