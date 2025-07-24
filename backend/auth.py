from fastapi import Header, HTTPException, Depends # type: ignore
from backend.database import get_user_by_email
import hashlib

def authenticate_user(email: str, password: str):
    user = get_user_by_email(email)
    if user:
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        if user[2] == password_hash:
            return user[0]  # Return full_name
    return None

def get_current_token(Authorization: str = Header(None)):
    if not Authorization or not Authorization.startswith('Bearer '):
        raise HTTPException(status_code=401, detail='Invalid or missing token')
    token = Authorization.split(' ', 1)[1]
    if token != 'abc123':
        raise HTTPException(status_code=401, detail='Invalid or missing token')
    return token 