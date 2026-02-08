import sqlite3


def test_films_table_has_data():
    conn = sqlite3.connect("films.db")
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM films;")
    count = cur.fetchone()[0]

    conn.close()

    assert count > 1000, f"电影数据数量不足，当前为 {count}"
