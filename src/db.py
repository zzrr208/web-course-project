import json
import sqlite3
from pathlib import Path
from typing import Iterable, Dict, Any, Tuple

DB_PATH = "films.db"
JSON_PATH = "films_2000_2024.json"


def connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS films (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            url TEXT NOT NULL UNIQUE,
            year INTEGER,
            source TEXT
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_films_year ON films(year);")
    conn.commit()


def load_json(json_path: str = JSON_PATH) -> list[Dict[str, Any]]:
    p = Path(json_path)
    if not p.exists():
        raise FileNotFoundError(
            f"找不到 {json_path}。请先运行 spider.py 生成 films_2000_2024.json，或把文件放在同目录。"
        )
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("JSON 格式不正确：期望是 list。")
    return data


def normalize_items(items: Iterable[Dict[str, Any]]) -> Iterable[Tuple[str, str, int, str]]:
    """
    将 dict 转成可插入的 tuple，并做最基本清洗。
    """
    for it in items:
        title = (it.get("title") or "").strip()
        url = (it.get("url") or "").strip()
        year = it.get("year")
        source = (it.get("source") or "").strip() or None

        if not title or not url:
            continue

        try:
            year_int = int(year) if year is not None else None
        except Exception:
            year_int = None

        yield (title, url, year_int, source)


def upsert_films(conn: sqlite3.Connection, items: Iterable[Tuple[str, str, int, str]]) -> int:
    """
    使用 UNIQUE(url) 去重：同 url 重复插入会被忽略。
    返回成功插入的行数（不含忽略的重复）。
    """
    cur = conn.cursor()
    before = cur.execute("SELECT COUNT(*) FROM films;").fetchone()[0]

    cur.executemany(
        """
        INSERT OR IGNORE INTO films (title, url, year, source)
        VALUES (?, ?, ?, ?);
        """,
        list(items),
    )
    conn.commit()

    after = cur.execute("SELECT COUNT(*) FROM films;").fetchone()[0]
    return after - before


def query_stats(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    total = cur.execute("SELECT COUNT(*) FROM films;").fetchone()[0]
    print(f"Total rows in DB: {total}")

    # 年份范围
    minmax = cur.execute("SELECT MIN(year), MAX(year) FROM films;").fetchone()
    print(f"Year range: {minmax[0]} - {minmax[1]}")

    # 每年数量（Top 10 年份）
    print("\nTop years by film count:")
    rows = cur.execute(
        """
        SELECT year, COUNT(*) AS cnt
        FROM films
        GROUP BY year
        ORDER BY cnt DESC
        LIMIT 10;
        """
    ).fetchall()
    for year, cnt in rows:
        print(year, cnt)

    # 抽样展示 5 条
    print("\nSample rows:")
    sample = cur.execute(
        """
        SELECT title, year, url
        FROM films
        ORDER BY RANDOM()
        LIMIT 5;
        """
    ).fetchall()
    for title, year, url in sample:
        print(f"- {title} ({year}) {url}")


def main() -> None:
    conn = connect(DB_PATH)
    init_db(conn)

    data = load_json(JSON_PATH)
    inserted = upsert_films(conn, normalize_items(data))
    print(f"Inserted new rows: {inserted}")

    query_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()


