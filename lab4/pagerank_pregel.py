from typing import Dict, List
from collections import defaultdict

from storage import get_connection, get_links, get_all_documents


class Vertex:
    """
    Вершина графа в модели Pregel.
    Каждая вершина знает свой ID, исходящие ссылки и текущий ранг.
    """
    def __init__(self, vid: int, out_links: List[int], num_vertices: int):
        self.id = vid
        self.out_links = out_links
        self.rank = 1.0 / num_vertices  # Начальный ранг

    def calculate(self, messages: List[float], num_vertices: int, damping: float):
        incoming_sum = sum(messages)
        self.rank = (1.0 - damping) / num_vertices + damping * incoming_sum

    def send_messages(self) -> List[tuple]:
        if not self.out_links:
            return []  # Нет исходящих ссылок - не отправляем сообщения
        
        share = self.rank / len(self.out_links)
        return [(dst, share) for dst in self.out_links]


def build_graph() -> tuple[List[int], Dict[int, List[int]]]:
    """
    Строит граф из базы данных.
    Возвращает список вершин и словарь исходящих ссылок.
    """
    conn = get_connection()
    docs = get_all_documents(conn)
    links = get_links(conn)
    conn.close()

    nodes = [doc_id for doc_id, _, _, _ in docs]
    outgoing: Dict[int, List[int]] = {n: [] for n in nodes}
    
    for from_id, to_id in links:
        if from_id in outgoing:
            outgoing[from_id].append(to_id)

    return nodes, outgoing


def pagerank_pregel(
    iterations: int = 10,
    damping: float = 0.85,
) -> Dict[int, float]:
    nodes, outgoing = build_graph()
    if not nodes:
        return {}

    num_vertices = len(nodes)
    
    # Создаем вершины графа
    vertices: Dict[int, Vertex] = {
        node: Vertex(node, outgoing.get(node, []), num_vertices)
        for node in nodes
    }

    # Выполняем supersteps (итерации Pregel)
    for _ in range(iterations):
        # Фаза отправки сообщений
        messages = defaultdict(list)
        for v in vertices.values():
            for dst, value in v.send_messages():
                messages[dst].append(value)
        
        # Обработка dangling nodes (вершины без исходящих ссылок)
        dangling_rank = sum(v.rank for v in vertices.values() if not v.out_links)
        dangling_share = damping * dangling_rank / num_vertices
        
        # Фаза вычисления (compute)
        for v in vertices.values():
            msgs = messages.get(v.id, [])
            v.calculate(msgs, num_vertices, damping)
            # Добавляем вклад от dangling nodes
            v.rank += dangling_share

    # Возвращаем ранги
    return {v.id: v.rank for v in vertices.values()}


if __name__ == "__main__":
    ranks = pagerank_pregel()
    if not ranks:
        print("В базе нет документов. Сначала запустите crawler.py")
    else:
        print("PageRank (Pregel):")
        for doc_id, score in sorted(ranks.items(), key=lambda x: x[1], reverse=True):
            print(f"doc_id={doc_id}: {score:.4f}")