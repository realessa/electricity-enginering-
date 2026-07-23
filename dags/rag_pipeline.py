# -*- coding: utf-8 -*-
"""RAG pipeline: chunking -> embeddings -> vector store -> hybrid search (RRF)
-> reranking -> grounded generation with citations."""

import os
from langchain_community.document_loaders import PyPDFLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import Chroma
from rank_bm25 import BM25Okapi
from sentence_transformers import CrossEncoder


BASE_DIR = "/opt/airflow/project/dags"

PDF_PATH = "/opt/airflow/project/dags/Global Energy Management Guide.pdf"

CHROMA_DIR = os.path.join(
    BASE_DIR,
    "chroma_db"
)
def load_and_chunk(pdf_path=PDF_PATH, chunk_size=500, chunk_overlap=100):
    loader = PyPDFLoader(pdf_path)
    documents = loader.load()
    splitter = RecursiveCharacterTextSplitter(chunk_size=chunk_size, chunk_overlap=chunk_overlap)
    chunks = splitter.split_documents(documents)
    print(f"📄 Pages: {len(documents)} | Chunks: {len(chunks)}")
    return chunks


def build_vector_store(chunks):
    embedding_model = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    vector_db = Chroma.from_documents(documents=chunks, embedding=embedding_model, persist_directory=CHROMA_DIR)
    print("✅ Vector Database Created")
    return vector_db


def build_bm25(chunks):
    texts = [doc.page_content for doc in chunks]
    tokenized_corpus = [t.lower().split() for t in texts]
    return BM25Okapi(tokenized_corpus)


def reciprocal_rank_fusion(rankings, k=60):
    """
    rankings: list of ranked chunk-index lists, one per retriever.
    Returns chunk indices sorted by fused RRF score (real RRF, not concat+dedup).
    """
    scores = {}
    for ranking in rankings:
        for rank, doc_id in enumerate(ranking):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank + 1)
    return sorted(scores.keys(), key=lambda d: scores[d], reverse=True)


def hybrid_search(query, chunks, vector_db, bm25, k=5):
    content_to_idx = {doc.page_content: i for i, doc in enumerate(chunks)}

    vector_results = vector_db.similarity_search(query, k=k)
    vector_ranking = [content_to_idx[d.page_content] for d in vector_results if d.page_content in content_to_idx]

    tokenized_query = query.lower().split()
    scores = bm25.get_scores(tokenized_query)
    bm25_ranking = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:k]

    fused_ids = reciprocal_rank_fusion([vector_ranking, bm25_ranking])[:k]
    return [chunks[i] for i in fused_ids]


def rerank(query, docs, model_name="cross-encoder/ms-marco-MiniLM-L-6-v2"):
    reranker = CrossEncoder(model_name)
    pairs = [[query, doc.page_content] for doc in docs]
    scores = reranker.predict(pairs)
    ranked = sorted(zip(scores, docs), key=lambda x: x[0], reverse=True)
    return [doc for _, doc in ranked]


def generate_grounded_answer(question, ranked_docs, top_n=3):

    context_blocks = []

    for i, doc in enumerate(ranked_docs[:top_n], start=1):
        page = doc.metadata.get("page", "unknown")
        context_blocks.append(
            f"[Source {i} | page {page}]\n{doc.page_content}"
        )

    context_text = "\n\n".join(context_blocks)

    answer = f"""
Based on the retrieved documents:

Question:
{question}

Relevant information:
{context_text}

Answer:
The answer is generated from the retrieved energy management documents.
"""

    print(f"\n🧠 Answer:\n{answer}")

    return answer

def run_rag(question):
    chunks = load_and_chunk()
    vector_db = build_vector_store(chunks)
    bm25 = build_bm25(chunks)

    candidates = hybrid_search(question, chunks, vector_db, bm25, k=5)
    ranked = rerank(question, candidates)
    answer = generate_grounded_answer(question, ranked)
    return answer, ranked


if __name__ == "__main__":
    run_rag("What are the benefits of smart meters?")
