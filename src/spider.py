import time
import json
import requests
from bs4 import BeautifulSoup

WIKI_YEAR_URL = "https://en.wikipedia.org/wiki/List_of_American_films_of_{year}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (course-project; +https://github.com/zzrr208/web-course-project)"
}

def fetch_html(url: str, timeout: int = 15) -> str:
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return r.text

def parse_films_from_year_page(html: str, limit: int = 10) -> list[dict]:
    """
    从“List of American films of {year}”页面提取电影条目（尽量提取 title + url）。
    Wikipedia 页面通常有多张 wikitable（按季度/月份分区），我们把所有表格都扫一遍。
    """
    soup = BeautifulSoup(html, "html.parser")
    results = []

    # 页面上有很多 wikitable（包括 box office 表、季度表等）
    for table in soup.select("table.wikitable"):
        # 大多数电影表格行里会有链接 <a href="/wiki/xxx">Title</a>
        for row in table.select("tr"):
            a = row.select_one("i a[href^='/wiki/'], a[href^='/wiki/']")
            if not a:
                continue

            title = a.get_text(strip=True)
            href = a.get("href", "")
            if not title or not href:
                continue

            # 过滤一些明显不是电影条目的链接（可按需增删）
            if title.lower() in {"edit", "cite this page"}:
                continue

            results.append({
                "title": title,
                "url": "https://en.wikipedia.org" + href
            })

            if len(results) >= limit:
                return results

    return results[:limit]

def crawl_wikipedia_films(year: int = 2024, limit: int = 10, sleep_sec: float = 1.0) -> list[dict]:
    url = WIKI_YEAR_URL.format(year=year)
    html = fetch_html(url)
    films = parse_films_from_year_page(html, limit=limit)
    time.sleep(sleep_sec)  # 友好一点，避免频繁请求
    return films

def save_json(items: list[dict], path: str = "sample.json") -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(items, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    films = crawl_wikipedia_films(year=2024, limit=10)
    print(f"Fetched: {len(films)}")
    for i, item in enumerate(films, 1):
        print(i, item["title"], item["url"])

    # 可选：保存样例数据文件（方便写测试/写文档）
    save_json(films, "sample.json")
    print("Saved sample.json")
