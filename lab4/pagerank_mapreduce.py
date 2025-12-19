from collections import defaultdict
from typing import Dict, List, Tuple

from storage import get_connection, get_links, get_all_documents


def build_graph() -> Tuple[List[int], Dict[int, List[int]]]:
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


# MAP: распределяет вес по исходящим рёбрам.
def map_phase(ranks: Dict[int, float], nodes: List[int], outgoing: Dict[int, List[int]], N: int, ) -> Dict[int, float]:
    contributions: Dict[int, float] = defaultdict(float)

    for node in nodes:
        rank = ranks[node]
        neighbors = outgoing.get(node, [])
        if neighbors:
            share = rank / len(neighbors)
            for nb in neighbors:
                contributions[nb] += share
        else:
            # dangling-node: распределяем по всем
            share = rank / N
            for nb in nodes:
                contributions[nb] += share

    return contributions


def reduce_phase(contributions: Dict[int, float], nodes: List[int], N: int, damping: float) -> Dict[int, float]:
    new_ranks: Dict[int, float] = {}
    for node in nodes:
        new_ranks[node] = (1.0 - damping) / N + damping * contributions[node]

    return new_ranks


def pagerank_mapreduce(iterations: int = 10, damping: float = 0.85) -> Dict[int, float]:
    nodes, outgoing = build_graph()
    if not nodes:
        return {}

    N = len(nodes)
    ranks: Dict[int, float] = {n: 1.0 / N for n in nodes}

    for _ in range(iterations):
        contributions = map_phase(ranks, nodes, outgoing, N)
        ranks = reduce_phase(contributions, nodes, N, damping)

    return ranks


if __name__ == "__main__":
    ranks = pagerank_mapreduce()
    if not ranks:
        print("В базе нет документов. Сначала запустите crawler.py")
    else:
        print("PageRank (MapReduce-стиль):")
        for doc_id, score in sorted(ranks.items(), key=lambda x: x[1], reverse=True):
            print(f"doc_id={doc_id}: {score:.4f}")
