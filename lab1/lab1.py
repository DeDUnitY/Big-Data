import csv
import multiprocessing as mp
from collections import defaultdict

INPUT_FILE = "amazon_reviews_us_Electronics_v1_00.tsv"
OUTPUT_FILE = "result.csv"
MIN_REVIEWS = 50

# Какие товары в категории электроники имеют наибольший средний рейтинг, при условии, что колличество отзывов больше 50.

# Берём список строк, возвращаем кортежи с полной информацией
def map_chunk(lines):
    mapped = []
    reader = csv.DictReader(lines, delimiter="\t")
    for row in reader:
        try:
            pid = row["product_id"]
            rating = float(row["star_rating"])
            title = row["product_title"]
            category = row.get("product_category", "")
            parent = row.get("product_parent", "")

            mapped.append((pid, rating, title, category, parent))
        except:
            continue
    return mapped

# Группировка по product_id
def shuffle(mapped_data):
    shuffled = defaultdict(list)
    for pid, rating, title, category, parent in mapped_data:
        shuffled[pid].append((rating, title, category, parent))
    return shuffled


# Суммируем рейтинги и собираем информацию
def reduce_pairs(pairs):
    agg = defaultdict(lambda: [0.0, 0, "", "", ""])  # sum, count, title, category, parent

    for pid, rating, title, category, parent in pairs:
        agg[pid][0] += rating
        agg[pid][1] += 1

        # Сохраняем метаданные
        if agg[pid][2] == "":
            agg[pid][2] = title
        if agg[pid][3] == "":
            agg[pid][3] = category
        if agg[pid][4] == "":
            agg[pid][4] = parent

    # фильтруем по MIN_REVIEWS
    result = [(pid, s / c, c, title, category, parent)
              for pid, (s, c, title, category, parent) in agg.items()
              if c >= MIN_REVIEWS]

    # сортировка по avg rating
    result.sort(key=lambda x: x[1], reverse=True)
    return result


# Вспомогательная функция для multiprocessing
def process_chunk(start, end, lines):
    chunk = lines[start:end]
    return map_chunk(chunk)


if __name__ == "__main__":
    with open(INPUT_FILE, encoding="utf-8") as f:
        all_lines = f.readlines()

    headers = all_lines[0]
    num_processes = 4
    chunk_size = (len(all_lines) - 1) // num_processes

    chunks = []
    for i in range(num_processes):
        start = 1 + i * chunk_size
        end = 1 + (i + 1) * chunk_size if i < num_processes - 1 else len(all_lines)
        chunk_with_header = [headers] + all_lines[start:end]
        chunks.append((0, len(chunk_with_header), chunk_with_header))

    with mp.Pool(num_processes) as pool:
        mapped_results = pool.starmap(process_chunk, chunks)

    all_mapped = [item for sublist in mapped_results for item in sublist]

    shuffled_data = shuffle(all_mapped)

    final_result = reduce_pairs(shuffled_data)

    # Запись в CSV с полной информацией
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "avg_rating", "review_count",
                         "product_title", "category", "product_parent"])

        for pid, avg, count, title, category, parent in final_result[:20]:
            writer.writerow([pid, f"{avg:.2f}", count, title, category, parent])

    print(f"\nРезультаты сохранены в {OUTPUT_FILE}")
