import os

import pathway as pw
from pathway.xpacks.llm import document_store, vector_store

pw.set_license_key("demo-license-key-with-telemetry")

DOCS_PATH = os.environ.get("MAINT_DOCS_PATH", "./maintenance-docs")


def build_docstore() -> document_store.DocumentStore:
    docs = pw.io.fs.read(DOCS_PATH, format="binary", with_metadata=True)
    # VectorStoreServer is itself a DocumentStore; reuse its knn factory to satisfy DocumentStore API.
    vector_server = vector_store.VectorStoreServer(
        docs,
        embedder=vector_store.SentenceTransformerEmbedder("all-MiniLM-L6-v2"),
    )
    return vector_server


if __name__ == "__main__":
    store = build_docstore()
    # Example: query docstore from code (for production use the LLM servers)
    query_table = pw.debug.table_from_list([{"query": "How to replace the coolant pump?"}])
    results = store.retrieve_query(query_table, "query", k=3)
    pw.io.csv.write(results, "docstore_sample_results.csv")
    pw.run()
