import sqlite3
import os
import hashlib

DB_PATH = './backend/metadata.db'

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT,
            upload_time TEXT,
            row_count INTEGER,
            parquet_path TEXT,
            status TEXT,
            user_email TEXT
        )
    ''')
    c.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            full_name TEXT,
            email TEXT UNIQUE,
            password_hash TEXT
        )
    ''')
    conn.commit()
    conn.close()

def add_file_metadata(file_name, upload_time, row_count, parquet_path, status, user_email):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        INSERT INTO files (file_name, upload_time, row_count, parquet_path, status, user_email)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (file_name, upload_time, row_count, parquet_path, status, user_email))
    conn.commit()
    conn.close()

def add_user(full_name, email, password):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    try:
        c.execute('INSERT INTO users (full_name, email, password_hash) VALUES (?, ?, ?)', (full_name, email, password_hash))
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()

def get_user(username):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT username, email, password_hash FROM users WHERE username = ?', (username,))
    user = c.fetchone()
    conn.close()
    return user

def get_user_by_email(email):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT full_name, email, password_hash FROM users WHERE email = ?', (email,))
    user = c.fetchone()
    conn.close()
    return user

def get_all_files(user_email):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('SELECT file_name, upload_time, row_count, parquet_path, status FROM files WHERE user_email = ? ORDER BY id DESC', (user_email,))
    rows = c.fetchall()
    conn.close()
    files = [
        {
            'file_name': row[0],
            'upload_time': row[1],
            'row_count': int(row[2]),
            'parquet_path': row[3],
            'status': row[4]
        }
        for row in rows
    ]
    return files 