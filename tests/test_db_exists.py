import os
import sqlite3


def test_database_file_exists():
    assert os.path.exists("films.db"), "films.db 数据库文件不存在"


def test_films_table_exists():
    conn = sqlite3.connect("films.db")
    cur = conn.cursor()

    cur.execute(
        """
        SELECT name FROM sqlite_master
        WHERE type='table' AND name='films';
        """
    )
    table = cur.fetchone()
    conn.close()

    assert table is not None, "films 表不存在"

