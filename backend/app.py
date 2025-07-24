from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Header, Request, Body # type: ignore
from fastapi.middleware.cors import CORSMiddleware # type: ignore
from pydantic import BaseModel # type: ignore
import os
from typing import Optional
from backend.database import init_db, add_file_metadata, get_all_files, add_user
from backend.auth import authenticate_user, get_current_token
from backend.utils import save_upload_file, convert_csv_to_parquet, get_row_count
from datetime import datetime
import logging
import shutil

UPLOAD_DIR = './backend/uploads/'
PARQUET_DIR = './backend/parquet/'

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(PARQUET_DIR, exist_ok=True)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()

class LoginRequest(BaseModel):
    email: str
    password: str

@app.post("/login")
def login(data: LoginRequest):
    print(f"Login attempt: email={data.email}, password={data.password}")
    full_name = authenticate_user(data.email, data.password)
    print(f"authenticate_user returned: {full_name}")
    if full_name:
        return {"token": "abc123", "full_name": full_name}
    raise HTTPException(status_code=401, detail="Invalid credentials")

class RegisterRequest(BaseModel):
    full_name: str
    email: str
    password: str

@app.post("/register")
def register(data: RegisterRequest):
    # Check for duplicate email
    import sqlite3
    conn = sqlite3.connect('./backend/metadata.db')
    c = conn.cursor()
    c.execute('SELECT email FROM users WHERE email = ?', (data.email,))
    if c.fetchone():
        conn.close()
        raise HTTPException(status_code=400, detail="Email already exists")
    conn.close()
    if add_user(data.full_name, data.email, data.password):
        return {"message": "User registered successfully"}
    raise HTTPException(status_code=500, detail="Registration failed")

# Helper to extract user email from token (for demo, just use a header for now)
def get_user_email_from_token(token: str = Depends(get_current_token)):
    # In a real app, decode JWT and extract email
    # For now, just get from a custom header for demo
    return Header(..., alias="X-User-Email")

@app.post("/upload")
def upload_file(request: Request, file: UploadFile = File(...), token: str = Depends(get_current_token)):
    user_email = request.headers.get("X-User-Email")
    if not user_email:
        raise HTTPException(status_code=400, detail="User email header missing")
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")
    try:
        upload_path = save_upload_file(file, UPLOAD_DIR)
    except Exception as e:
        logging.error(f"Failed to save uploaded file: {e}")
        raise HTTPException(status_code=500, detail="Failed to save file.")
    parquet_path = os.path.join(PARQUET_DIR, file.filename.replace('.csv', '.parquet'))
    status = "Processing"
    row_count = 0
    try:
        row_count = convert_csv_to_parquet(upload_path, parquet_path)
        status = "Done" if row_count > 0 else "Error"
    except Exception as e:
        logging.error(f"Failed to convert CSV to Parquet: {e}")
        status = "Error"
    add_file_metadata(
        file_name=file.filename,
        upload_time=datetime.utcnow().isoformat(),
        row_count=row_count,
        parquet_path=parquet_path,
        status=status,
        user_email=user_email
    )
    return {
        "message": "File uploaded",
        "file": {
            "file_name": file.filename,
            "upload_time": datetime.utcnow().isoformat(),
            "row_count": row_count,
            "parquet_path": parquet_path,
            "status": status
        }
    }

@app.post("/clear")
def clear_memory(token: str = Depends(get_current_token)):
    # Delete all files in uploads and parquet
    for folder in [UPLOAD_DIR, PARQUET_DIR]:
        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
            except Exception as e:
                pass
    # Clear the database
    import sqlite3
    conn = sqlite3.connect('./backend/metadata.db')
    c = conn.cursor()
    c.execute('DELETE FROM files')
    conn.commit()
    conn.close()
    return {"message": "All files and metadata cleared."}

@app.get("/files")
def list_files(request: Request, token: str = Depends(get_current_token)):
    user_email = request.headers.get("X-User-Email")
    print(f"/files endpoint: user_email received: {user_email}")
    if not user_email:
        raise HTTPException(status_code=400, detail="User email header missing")
    files = get_all_files(user_email)
    return files

@app.post("/delete_file")
def delete_file(request: Request, data: dict = Body(...), token: str = Depends(get_current_token)):
    user_email = request.headers.get("X-User-Email")
    file_name = data.get("file_name")
    if not user_email or not file_name:
        raise HTTPException(status_code=400, detail="User email or file name missing")
    # Delete files from disk
    csv_path = os.path.join(UPLOAD_DIR, file_name)
    parquet_path = os.path.join(PARQUET_DIR, file_name.replace('.csv', '.parquet'))
    for path in [csv_path, parquet_path]:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception as e:
            pass
    # Delete from database
    import sqlite3
    conn = sqlite3.connect('./backend/metadata.db')
    c = conn.cursor()
    c.execute('DELETE FROM files WHERE file_name = ? AND user_email = ?', (file_name, user_email))
    conn.commit()
    conn.close()
    return {"message": f"File {file_name} deleted."}

@app.post("/convert_file")
def convert_file(request: Request, data: dict = Body(...), token: str = Depends(get_current_token)):
    user_email = request.headers.get("X-User-Email")
    file_name = data.get("file_name")
    if not user_email or not file_name:
        raise HTTPException(status_code=400, detail="User email or file name missing")
    csv_path = os.path.join(UPLOAD_DIR, file_name)
    parquet_path = os.path.join(PARQUET_DIR, file_name.replace('.csv', '.parquet'))
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="CSV file not found")
    try:
        row_count = convert_csv_to_parquet(csv_path, parquet_path)
        status = "Done" if row_count > 0 else "Error"
    except Exception as e:
        status = "Error"
        row_count = 0
    # Update database
    import sqlite3
    conn = sqlite3.connect('./backend/metadata.db')
    c = conn.cursor()
    c.execute('''UPDATE files SET row_count=?, parquet_path=?, status=? WHERE file_name=? AND user_email=?''', (row_count, parquet_path, status, file_name, user_email))
    conn.commit()
    c.execute('SELECT file_name, upload_time, row_count, parquet_path, status FROM files WHERE file_name=? AND user_email=?', (file_name, user_email))
    row = c.fetchone()
    conn.close()
    if row:
        return {"file_name": row[0], "upload_time": row[1], "row_count": row[2], "parquet_path": row[3], "status": row[4]}
    else:
        raise HTTPException(status_code=404, detail="File metadata not found") 