"""
Скрипт для скачивания датасета Wikipedia с документами и ссылками.
Использует прямой парсинг HTML страниц Wikipedia.
"""
import json
import re
import time
from pathlib import Path
from typing import List, Dict, Set
from urllib.parse import quote, unquote
import requests
from bs4 import BeautifulSoup


WIKI_BASE_URL = "https://en.wikipedia.org/wiki"

# User-Agent для вежливого краулинга
HEADERS = {
    "User-Agent": "MiniSearchEngine/1.0 (Educational project; contact: student@university.edu)"
}


def is_valid_wiki_link(href: str) -> bool:
    """Проверяет, является ли ссылка валидной ссылкой на статью Wikipedia."""
    if not href:
        return False
    if not href.startswith("/wiki/"):
        return False
    # Исключаем служебные страницы
    if any(prefix in href for prefix in [
        ":", "#", "Main_Page", "/wiki/File:", "/wiki/Category:",
        "/wiki/Template:", "/wiki/Help:", "/wiki/Wikipedia:",
        "/wiki/Special:", "/wiki/Talk:", "/wiki/User:"
    ]):
        return False
    return True


def article_name_from_href(href: str) -> str:
    """Извлекает название статьи из href."""
    return unquote(href.split("/wiki/")[1].split("#")[0])


def get_wiki_page_content(title: str) -> Dict:
    """Получает содержимое страницы через прямой запрос HTML."""
    # Кодируем название статьи для URL
    encoded_title = title.replace(" ", "_")
    url = f"{WIKI_BASE_URL}/{encoded_title}"
    
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        return {"html": resp.text, "title": title, "url": url}
    except Exception as e:
        print(f"Ошибка при получении {title}: {e}")
        return None


def extract_links_from_wiki_html(html: str, base_title: str) -> List[str]:
    """Извлекает ссылки на другие статьи Wikipedia из HTML."""
    soup = BeautifulSoup(html, "html.parser")
    links = set()
    
    # Ищем основной контент статьи
    content = soup.find("div", {"id": "mw-content-text"})
    if not content:
        # Если не нашли основной контент, ищем во всем документе
        content = soup
    
    # Ищем все ссылки на другие статьи Wikipedia
    for a in content.find_all("a", href=True):
        href = a.get("href", "")
        if is_valid_wiki_link(href):
            title = article_name_from_href(href)
            # Нормализуем: заменяем подчеркивания на пробелы
            title = title.replace("_", " ")
            if title and title != base_title:
                links.add(title)
    
    return list(links)


def extract_text_from_wiki_html(html: str) -> str:
    """Извлекает чистый текст из HTML Wikipedia."""
    soup = BeautifulSoup(html, "html.parser")
    
    # Удаляем служебные элементы
    for tag in soup.find_all(["script", "style", "nav", "header", "footer", "aside"]):
        if tag:
            tag.decompose()
    
    # Удаляем инфобоксы и другие служебные блоки
    for tag in soup.find_all("div", class_=re.compile("infobox|navbox|sidebar|mw-editsection")):
        if tag:
            tag.decompose()
    
    # Ищем основной контент статьи
    content = soup.find("div", {"id": "mw-content-text"})
    if content:
        # Удаляем таблицы и другие служебные элементы из контента
        for tag in content.find_all(["table", "div", "span"], class_=re.compile("reference|navbox|infobox")):
            if tag:
                tag.decompose()
        
        # Извлекаем текст из параграфов
        paragraphs = content.find_all("p")
        text_parts = [p.get_text(separator=" ", strip=True) for p in paragraphs if p]
        text = " ".join(text_parts)
    else:
        # Если не нашли основной контент, берем весь текст
        text = soup.get_text(separator=" ", strip=True)
    
    # Нормализуем пробелы
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def crawl_wikipedia(
    seed_titles: List[str],
    max_pages: int = 50,
    output_file: str = "wikipedia_dataset.json"
) -> None:
    """
    Краулит Wikipedia статьи, начиная с seed_titles.
    Сохраняет результат в JSON файл.
    """
    visited: Set[str] = set()
    queue: List[str] = list(seed_titles)
    documents: Dict[str, Dict] = {}
    
    print(f"Начинаю краулинг Wikipedia. Максимум страниц: {max_pages}")
    
    while queue and len(visited) < max_pages:
        title = queue.pop(0)
        
        if title in visited:
            continue
        
        print(f"[{len(visited) + 1}/{max_pages}] Обрабатываю: {title}")
        
        page_data = get_wiki_page_content(title)
        if not page_data:
            visited.add(title)
            continue
        
        html = page_data["html"]
        url = page_data["url"]
        text = extract_text_from_wiki_html(html)
        links = extract_links_from_wiki_html(html, title)
        
        documents[title] = {
            "url": url,
            "title": title,
            "content": text,
            "links": links
        }
        
        visited.add(title)
        
        # Добавляем новые страницы в очередь
        for link_title in links:
            if link_title not in visited and link_title not in queue:
                if len(visited) + len(queue) < max_pages * 2:
                    queue.append(link_title)
        
        # Задержка для вежливого краулинга (Wikipedia рекомендует минимум 1 секунду)
        time.sleep(1.0)
    
    # Сохраняем результат
    output_path = Path(__file__).parent / output_file
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(documents, f, ensure_ascii=False, indent=2)
    
    print(f"\nСкачано {len(documents)} страниц. Сохранено в {output_path}")
    print(f"Всего ссылок между документами: {sum(len(doc['links']) for doc in documents.values())}")


if __name__ == "__main__":
    # Seed статьи
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
    
    crawl_wikipedia(seed_titles, max_pages=150, output_file="wikipedia_dataset.json")

