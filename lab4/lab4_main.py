import sys
from pathlib import Path

# Добавляем текущую директорию в путь для импортов
sys.path.insert(0, str(Path(__file__).parent))

from storage import get_connection, init_db, get_all_documents, get_links
from load_dataset import load_dataset_to_db
from parsing import crawl_wikipedia
from pagerank_mapreduce import pagerank_mapreduce
from pagerank_pregel import pagerank_pregel
from search_engine import (
    build_inverted_index,
    search_by_documents,
    search_by_terms,
    tokenize,
    pretty_print_results
)
from typing import Dict


def download_dataset():
    json_file = Path(__file__).parent / "wikipedia_dataset.json"

    if json_file.exists():
        print(f"Датасет уже существует: {json_file}")
        return

    seed_titles = [
        "Information retrieval",
        "PageRank",
        "Search engine",
        "Web search engine",
        "Inverted index",
        "Document retrieval",
        "Text mining",
        "Natural language processing"
    ]

    print(f"Seed статьи: {', '.join(seed_titles)}")
    crawl_wikipedia(seed_titles, max_pages=50, output_file="wikipedia_dataset.json")


def load_to_db():
    conn = get_connection()
    init_db(conn)

    # Проверяем, есть ли уже данные
    docs = get_all_documents(conn)
    if docs:
        # Очищаем базу
        conn.execute("DELETE FROM postings")
        conn.execute("DELETE FROM terms")
        conn.execute("DELETE FROM links")
        conn.execute("DELETE FROM documents")
        conn.commit()
        print("База очищена. Загружаем данные заново...")
        load_dataset_to_db()

    else:
        load_dataset_to_db()

    conn.close()


def show_statistics():
    """Шаг 3: Показ статистики по данным."""
    print("Статистика по данным")

    conn = get_connection()
    docs = get_all_documents(conn)
    links = get_links(conn)

    print(f"Всего документов: {len(docs)}")
    print(f"Всего ссылок между документами: {len(links)}")

    # Подсчитываем входящие и исходящие ссылки
    outgoing_count = {}
    incoming_count = {}

    for from_id, to_id in links:
        outgoing_count[from_id] = outgoing_count.get(from_id, 0) + 1
        incoming_count[to_id] = incoming_count.get(to_id, 0) + 1

    print(f"\nДокументы с наибольшим количеством исходящих ссылок:")
    top_outgoing = sorted(outgoing_count.items(), key=lambda x: x[1], reverse=True)[:5]
    for doc_id, count in top_outgoing:
        doc = next((d for d in docs if d[0] == doc_id), None)
        if doc:
            print(f"  - {doc[2]} (id={doc_id}): {count} ссылок")

    print(f"\nДокументы с наибольшим количеством входящих ссылок:")
    top_incoming = sorted(incoming_count.items(), key=lambda x: x[1], reverse=True)[:5]
    for doc_id, count in top_incoming:
        doc = next((d for d in docs if d[0] == doc_id), None)
        if doc:
            print(f"  - {doc[2]} (id={doc_id}): {count} ссылок")

    conn.close()


def run_pagerank_mapreduce():
    """Шаг 4: PageRank через MapReduce."""
    print("PageRank через MapReduce")

    print("Вычисляем PageRank используя подход MapReduce")
    ranks = pagerank_mapreduce(iterations=20, damping=0.85)

    if not ranks:
        print("Ошибка: не удалось вычислить PageRank. Проверьте наличие данных в базе.")
        return

    print(f"\nТоп-10 документов по PageRank (MapReduce):")
    conn = get_connection()
    sorted_ranks = sorted(ranks.items(), key=lambda x: x[1], reverse=True)[:10]

    for i, (doc_id, score) in enumerate(sorted_ranks, 1):
        from storage import get_document
        doc = get_document(conn, doc_id)
        if doc:
            _, url, title, _ = doc
            print(f"{i:2d}. [{score:.6f}] {title}")
        else:
            print(f"{i:2d}. [{score:.6f}] doc_id={doc_id}")

    conn.close()
    return ranks


def run_pagerank_pregel():
    print("PageRank через Pregel")

    ranks = pagerank_pregel(iterations=20, damping=0.85)

    if not ranks:
        print("Ошибка: не удалось вычислить PageRank. Проверьте наличие данных в базе.")
        return

        print(f"\nТоп-10 документов по PageRank (Pregel):")
    conn = get_connection()
    sorted_ranks = sorted(ranks.items(), key=lambda x: x[1], reverse=True)[:10]

    for i, (doc_id, score) in enumerate(sorted_ranks, 1):
        from storage import get_document
        doc = get_document(conn, doc_id)
        if doc:
            _, url, title, _ = doc
            print(f"{i:2d}. [{score:.6f}] {title}")
        else:
            print(f"{i:2d}. [{score:.6f}] doc_id={doc_id}")

    conn.close()
    return ranks


def build_index():
    build_inverted_index()

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM terms")
    term_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM postings")
    posting_count = cur.fetchone()[0]
    conn.close()


def search_document_at_a_time(query: str, pagerank: Dict[int, float] = None):
    print("Полнотекстовый поиск (Поиск по документам)")

    print(f"Запрос: '{query}'")
    terms = tokenize(query)
    print(f"Термы запроса: {terms}")
    if not terms:
        print("Ошибка: запрос не содержит термов.")
        return []

    results = search_by_documents(terms, pagerank=pagerank, alpha=0.1)

    print(f"\nНайдено документов: {len(results)}")
    if results:
        print("\nТоп-10 результатов (Поиск по документам):")
        pretty_print_results(results, limit=10)

    return results


def search_term_at_a_time(query: str, pagerank: Dict[int, float] = None):
    print("Полнотекстовый поиск (Поиск по термам)")

    print(f"Запрос: '{query}'")
    terms = tokenize(query)
    print(f"Термы запроса: {terms}")

    if not terms:
        print("Ошибка: запрос не содержит термов.")
        return []

    if pagerank:
        print("Используется PageRank для улучшения ранжирования (alpha=0.1)")

    results = search_by_terms(terms, pagerank=pagerank, alpha=0.1)

    print(f"\nНайдено документов: {len(results)}")
    if results:
        print("\nТоп-10 результатов (Поиск по термам):")
        pretty_print_results(results, limit=10)

    return results


def main():
    try:
        # Скачивание датасета
        download_dataset()

        # Загрузка в БД
        load_to_db()

        # Статистика
        show_statistics()

        # PageRank MapReduce
        ranks_mr = run_pagerank_mapreduce()

        # PageRank Pregel
        ranks_pregel = run_pagerank_pregel()

        # Экспорт графа в файлы (опционально, для демонстрации)
        try:
            from graph_export import export_graph_to_files
            print("ЭКСПОРТ ГРАФА")
            export_graph_to_files()
        except ImportError:
            pass

        # Построение индекса
        build_index()

        # Поиск
        test_queries = [
            "search engine",
            "information retrieval",
        ]

        # Используем PageRank для улучшения поиска

        for query in test_queries:
            print(f"\n\nТестовый запрос: '{query}'")

            # Поиск с PageRank
            results_doc = search_document_at_a_time(query, ranks_mr)
            results_term = search_term_at_a_time(query, ranks_mr)

            # Сравнение результатов
            if results_doc and results_term:
                print(f"\nСравнение результатов для запроса '{query}':")
                print(f"  Поиск по документам: {len(results_doc)} документов")
                print(f"  Поиск по термам: {len(results_term)} документов")

                # Проверяем совпадение топ-5
                top5_doc = [r[0] for r in results_doc[:5]]
                top5_term = [r[0] for r in results_term[:5]]
                common = set(top5_doc) & set(top5_term)
                print(f"  Общих документов в топ-5: {len(common)}")
                print('\n'*2)


    except KeyboardInterrupt:
        print("\n\nПрервано пользователем.")
    except Exception as e:
        print(f"\n\nОшибка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
