"""
Загружает датасет из JSON файла в базу данных.
"""
import json
from pathlib import Path
from storage import get_connection, init_db, upsert_document, insert_links


def load_dataset_to_db(json_file: str = "wikipedia_dataset.json") -> None:
    """Загружает датасет из JSON в базу данных."""
    json_path = Path(__file__).parent / json_file
    
    if not json_path.exists():
        print(f"Файл {json_path} не найден. Сначала запустите download_dataset.py")
        return
    
    with open(json_path, "r", encoding="utf-8") as f:
        documents = json.load(f)
    
    conn = get_connection()
    init_db(conn)
    
    # Сначала создаём все документы
    doc_id_map = {}  # title -> doc_id
    for title, doc_data in documents.items():
        url = doc_data["url"]
        title_text = doc_data["title"]
        content = doc_data["content"]
        
        doc_id = upsert_document(conn, url, title_text, content)
        doc_id_map[title] = doc_id
    
    # Затем добавляем ссылки
    total_links = 0
    for title, doc_data in documents.items():
        from_id = doc_id_map[title]
        links = doc_data.get("links", [])
        
        # Преобразуем названия статей в URL
        link_urls = []
        for link_title in links:
            if link_title in doc_id_map:
                # Находим URL для этой статьи
                for other_title, other_data in documents.items():
                    if other_title == link_title:
                        link_urls.append(other_data["url"])
                        break
        
        if link_urls:
            insert_links(conn, from_id, link_urls)
            total_links += len(link_urls)
    
    conn.close()


if __name__ == "__main__":
    load_dataset_to_db()

