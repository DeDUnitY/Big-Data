import sqlite3
from pathlib import Path
from typing import Iterable, Tuple, List, Optional


DB_PATH = Path(__file__).with_name("search.db")


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS documents (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            url     TEXT UNIQUE NOT NULL,
            title   TEXT,
            content TEXT
        );

        CREATE TABLE IF NOT EXISTS links (
            from_id INTEGER NOT NULL,
            to_id   INTEGER NOT NULL,
            FOREIGN KEY(from_id) REFERENCES documents(id) ON DELETE CASCADE,
            FOREIGN KEY(to_id)   REFERENCES documents(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS terms (
            id    INTEGER PRIMARY KEY AUTOINCREMENT,
            term  TEXT NOT NULL
        );

        CREATE UNIQUE INDEX IF NOT EXISTS idx_terms_term ON terms(term);

        CREATE TABLE IF NOT EXISTS postings (
            term_id INTEGER NOT NULL,
            doc_id  INTEGER NOT NULL,
            tf      INTEGER NOT NULL,
            FOREIGN KEY(term_id) REFERENCES terms(id) ON DELETE CASCADE,
            FOREIGN KEY(doc_id)  REFERENCES documents(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_postings_term ON postings(term_id);
        CREATE INDEX IF NOT EXISTS idx_postings_doc  ON postings(doc_id);
        """
    )

    conn.commit()


def upsert_document(conn: sqlite3.Connection, url: str, title: str, content: str) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO documents(url, title, content)
        VALUES (?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            title = excluded.title,
            content = excluded.content
        """,
        (url, title, content),
    )
    conn.commit()
    cur.execute("SELECT id FROM documents WHERE url = ?", (url,))
    row = cur.fetchone()
    return int(row[0])


def insert_links(conn: sqlite3.Connection, from_id: int, to_urls: Iterable[str]) -> None:
    """
    Добавляет ссылки. Целевые документы создаются «пустышками», если ещё не существуют.
    """
    cur = conn.cursor()

    for url in to_urls:
        url = url.strip()
        if not url:
            continue

        cur.execute("SELECT id FROM documents WHERE url = ?", (url,))
        row = cur.fetchone()
        if row:
            to_id = int(row[0])
        else:
            cur.execute(
                "INSERT INTO documents(url, title, content) VALUES (?, '', '')",
                (url,),
            )
            to_id = cur.lastrowid

        cur.execute(
            "INSERT INTO links(from_id, to_id) VALUES (?, ?)",
            (from_id, to_id),
        )

    conn.commit()


def get_all_documents(conn: sqlite3.Connection) -> List[Tuple[int, str, str, str]]:
    cur = conn.cursor()
    cur.execute("SELECT id, url, title, content FROM documents")
    return [(int(r[0]), r[1], r[2], r[3]) for r in cur.fetchall()]


def get_links(conn: sqlite3.Connection) -> List[Tuple[int, int]]:
    cur = conn.cursor()
    cur.execute("SELECT from_id, to_id FROM links")
    return [(int(r[0]), int(r[1])) for r in cur.fetchall()]


def clear_index(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    cur.executescript(
        """
        DELETE FROM postings;
        DELETE FROM terms;
        """
    )
    conn.commit()


def get_or_create_term_id(conn: sqlite3.Connection, term: str) -> int:
    cur = conn.cursor()
    cur.execute("SELECT id FROM terms WHERE term = ?", (term,))
    row = cur.fetchone()
    if row:
        return int(row[0])

    cur.execute("INSERT INTO terms(term) VALUES (?)", (term,))
    conn.commit()
    return int(cur.lastrowid)


def insert_postings(
    conn: sqlite3.Connection,
    doc_id: int,
    term_freqs: Iterable[Tuple[str, int]],
) -> None:
    cur = conn.cursor()

    for term, tf in term_freqs:
        term_id = get_or_create_term_id(conn, term)
        cur.execute(
            "INSERT INTO postings(term_id, doc_id, tf) VALUES (?, ?, ?)",
            (term_id, doc_id, tf),
        )

    conn.commit()


def get_term_postings(
    conn: sqlite3.Connection, term: str
) -> List[Tuple[int, int]]:
    """
    Возвращает список (doc_id, tf) для заданного терма.
    """
    cur = conn.cursor()
    cur.execute("SELECT id FROM terms WHERE term = ?", (term,))
    row = cur.fetchone()
    if not row:
        return []

    term_id = int(row[0])
    cur.execute(
        "SELECT doc_id, tf FROM postings WHERE term_id = ? ORDER BY doc_id",
        (term_id,),
    )
    return [(int(r[0]), int(r[1])) for r in cur.fetchall()]


def get_document(conn: sqlite3.Connection, doc_id: int) -> Optional[Tuple[int, str, str, str]]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, url, title, content FROM documents WHERE id = ?",
        (doc_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return int(row[0]), row[1], row[2], row[3]




