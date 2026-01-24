import sqlite3

DB_PATH = "films.db"

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def query_by_year(conn: sqlite3.Connection, year: int, limit: int = 20) -> None:
    cur = conn.cursor()

    count = cur.execute(
        "SELECT COUNT(*) AS cnt FROM films WHERE year = ?",
        (year,),
    ).fetchone()["cnt"]

    print(f"\nYear: {year}, Count: {count}")

    rows = cur.execute(
        """
        SELECT title, url
        FROM films
        WHERE year = ?
        ORDER BY title ASC
        LIMIT ?
        """,
        (year, limit),
    ).fetchall()

    if not rows:
        print("No results.")
        return

    for i, r in enumerate(rows, 1):
        print(f"{i}. {r['title']} - {r['url']}")


def main():
    print("=== 电影信息查询系统（SQLite） ===")

    while True:
        year_input = input("请输入查询年份（如 2024，输入 q 退出）：").strip()
        if year_input.lower() == "q":
            print("退出查询。")
            break

        if not year_input.isdigit():
            print("请输入合法的年份（数字）。")
            continue

        year = int(year_input)

        conn = connect(DB_PATH)
        try:
            query_by_year(conn, year, limit=20)
        finally:
            conn.close()


if __name__ == "__main__":
    main()

