import math
import re
from collections import Counter
from typing import Dict, Iterable, List, Tuple

from storage import (
    get_connection,
    init_db,
    clear_index,
    get_all_documents,
    insert_postings,
    get_term_postings,
    get_document,
)


TOKEN_RE = re.compile(r"[A-Za-zА-Яа-я0-9]+", re.UNICODE)


def tokenize(text: str) -> List[str]:
    return [t.lower() for t in TOKEN_RE.findall(text)]


def build_inverted_index() -> None:
    """
    Строит инвертированный индекс (terms, postings) по всем документам.
    """
    conn = get_connection()
    init_db(conn)
    clear_index(conn)

    docs = get_all_documents(conn)
    for doc_id, _, _, content in docs:
        tokens = tokenize(content)
        freqs = Counter(tokens)
        term_freqs = [(term, int(tf)) for term, tf in freqs.items()]
        insert_postings(conn, doc_id, term_freqs)

    conn.close()


def search_by_documents(
    query_terms: Iterable[str],
    pagerank: Dict[int, float] = None,
    alpha: float = 0.1
) -> List[Tuple[int, float]]:
    """
    Поиск по документам:
    - одновременно обходим документы, где есть все термы запроса;
    """
    conn = get_connection()
    try:
        terms = [t.lower() for t in query_terms]

        postings_lists = [get_term_postings(conn, t) for t in terms]
        if not postings_lists or any(not pl for pl in postings_lists):
            return []

        # Списки отсортированы по doc_id, делаем пересечение «в лоб»
        indices = [0 for _ in postings_lists]
        results: Dict[int, float] = {}

        while True:
            current_ids = []
            for i, pl in enumerate(postings_lists):
                if indices[i] >= len(pl):
                    # Применяем PageRank к результатам перед возвратом
                    if pagerank:
                        for doc_id in results:
                            results[doc_id] += alpha * pagerank.get(doc_id, 0.0)
                    return sorted(results.items(), key=lambda x: x[1], reverse=True)
                current_ids.append(pl[indices[i]][0])

            max_id = max(current_ids)
            min_id = min(current_ids)

            if max_id == min_id:
                doc_id = max_id
                score = 0.0
                for i, pl in enumerate(postings_lists):
                    _, tf = pl[indices[i]]
                    score += float(tf)
                    indices[i] += 1
                results[doc_id] = score
            else:
                for i, pl in enumerate(postings_lists):
                    while indices[i] < len(pl) and pl[indices[i]][0] < max_id:
                        indices[i] += 1
    finally:
        conn.close()


def search_by_terms(
    query_terms: Iterable[str],
    pagerank: Dict[int, float] = None,
    alpha: float = 0.1
) -> List[Tuple[int, float]]:
    """
    Поиск по термам:
    - последовательно обходим списки термов;
    - накапливаем score по документам.
    """
    conn = get_connection()
    try:
        terms = [t.lower() for t in query_terms]

        scores: Dict[int, float] = {}
        for term in terms:
            postings = get_term_postings(conn, term)
            for doc_id, tf in postings:
                scores[doc_id] = scores.get(doc_id, 0.0) + float(tf)
        
        # Применяем PageRank к результатам
        if pagerank:
            for doc_id in scores:
                scores[doc_id] += alpha * pagerank.get(doc_id, 0.0)
    finally:
        conn.close()
    
    return sorted(scores.items(), key=lambda x: x[1], reverse=True)


def pretty_print_results(results: List[Tuple[int, float]], limit: int = 10) -> None:
    conn = get_connection()
    for doc_id, score in results[:limit]:
        doc = get_document(conn, doc_id)
        if not doc:
            continue
        _, url, title, _ = doc
        print(f"[{score:.2f}] {title} ({url})")
    conn.close()


if __name__ == "__main__":
    build_inverted_index()
    q = "search engine ranking"
    terms = tokenize(q)
    print(f"Запрос: {q}")

    print("\nПоиск по документам:")
    res_doc = search_by_documents(terms)
    pretty_print_results(res_doc)

    print("\nПоиск по термам:")
    res_term = search_by_terms(terms)
    pretty_print_results(res_term)



