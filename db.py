"""
数据库模块
"""
import sqlite3

def connect_db():
    return sqlite3.connect("data.db")
