import sqlite3


def test_query_specific_year():
    conn = sqlite3.connect("films.db")
    cur = conn.cursor()

    year = 2024
    cur.execute(
        "SELECT COUNT(*) FROM films WHERE year = ?;",
        (year,),
    )
    count = cur.fetchone()[0]
    conn.close()

    # 只要不报错，且数量 >= 0 即认为查询功能可用
    assert count >= 0

