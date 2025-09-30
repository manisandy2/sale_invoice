# from datetime import datetime, timedelta
# from jose import JWTError, jwt
# from fastapi import Depends, HTTPException, status
#from fastapi.security import OAuth2PasswordBearer,OAuth2PasswordRequestForm,HTTPBearer, HTTPAuthorizationCredentials

# Secret key (in production, load from env)
# SECRET_KEY = "crm-secret-key"
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 60

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
# security = HTTPBearer()
# oauth2_scheme = HTTPBearer()
#
# def create_access_token(data: dict, expires_delta: timedelta | None = None):
#     to_encode = data.copy()
#     expire = datetime.utcnow() + (expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES))
#     to_encode.update({"exp": expire})
#     return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
#
# def verify_token(
#         credentials: HTTPAuthorizationCredentials = Depends(security)):
#     token = credentials.credentials
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         return payload  # you can also return user_id or username
#     except JWTError:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Invalid or expired token",
#             headers={"WWW-Authenticate": "Bearer"},
#         )
#
# print(create_access_token(data={"name":"mani","password":"admin@123"}))

# from datetime import datetime, timedelta, timezone
# from typing import Annotated
#
# import jwt
# from fastapi import Depends, FastAPI, HTTPException, status
# from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
# from jwt.exceptions import InvalidTokenError
# from passlib.context import CryptContext
# from pydantic import BaseModel
#
# # to get a string like this run:
# # openssl rand -hex 32
# SECRET_KEY = "09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7"
# ALGORITHM = "HS256"
# ACCESS_TOKEN_EXPIRE_MINUTES = 30
#
#
# fake_users_db = {
#     "user01": {
#         "username": "user01",
#         "hashed_password": "$2b$12$odPNfe15HCRNuOGuIFCu0.bLqLF/aBuf4/Qav5iGlJAu8jtQ2GmtK",
#         "disabled": False
#     },
#     "sandy":{
#         "username": "sandy",
#         "hashed_password": "$2b$12$odPNfe15HCRNuOGuIFCu0.bLqLF/aBuf4/Qav5iGlJAu8jtQ2GmtK",
#         "disabled": False
#     }
# }
#
#
# class Token(BaseModel):
#     access_token: str
#     token_type: str
#
#
# class TokenData(BaseModel):
#     username: str | None = None
#
#
# class User(BaseModel):
#     username: str
#     disabled: bool | None = None
#
#
#
# class UserInDB(User):
#     hashed_password: str
#
#
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
#
# # oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
# # oauth2_scheme = OAuth2PasswordRequestForm(username=fake_users_db["johndoe"]["username"],password=fake_users_db["johndoe"]["hashed_password"])
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
#
# app = FastAPI()
#
#
# def verify_password(plain_password, hashed_password):
#     return pwd_context.verify(plain_password, hashed_password)
#
#
# def get_password_hash(password):
#     return pwd_context.hash(password)
#
#
# def get_user(db, username: str):
#     if username in db:
#         user_dict = db[username]
#         return UserInDB(**user_dict)
#
#
# def authenticate_user(fake_db, username: str, password: str):
#     user = get_user(fake_db, username)
#     if not user:
#         return False
#     if not verify_password(password, user.hashed_password):
#         return False
#     return user
#
#
# def create_access_token(data: dict, expires_delta: timedelta | None = None):
#     to_encode = data.copy()
#     if expires_delta:
#         expire = datetime.now(timezone.utc) + expires_delta
#     else:
#         expire = datetime.now(timezone.utc) + timedelta(minutes=15)
#     to_encode.update({"exp": expire})
#     encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
#     return encoded_jwt
#
#
# async def get_current_user(token: Annotated[str, Depends(oauth2_scheme)]):
#     credentials_exception = HTTPException(
#         status_code=status.HTTP_401_UNAUTHORIZED,
#         detail="Could not validate credentials",
#         headers={"WWW-Authenticate": "Bearer"},
#     )
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         username = payload.get("sub")
#         if username is None:
#             raise credentials_exception
#         token_data = TokenData(username=username)
#     except InvalidTokenError:
#         raise credentials_exception
#     user = get_user(fake_users_db, username=token_data.username)
#     if user is None:
#         raise credentials_exception
#     return user
#
#
# async def get_current_active_user(
#     current_user: Annotated[User, Depends(get_current_user)],
# ):
#     if current_user.disabled:
#         raise HTTPException(status_code=400, detail="Inactive user")
#     return current_user


# @app.post("/token")
# async def login_for_access_token(
#     form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
# ) -> Token:
#     user = authenticate_user(fake_users_db, form_data.username, form_data.password)
#     if not user:
#         raise HTTPException(
#             status_code=status.HTTP_401_UNAUTHORIZED,
#             detail="Incorrect username or password",
#             headers={"WWW-Authenticate": "Bearer"},
#         )
#     access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
#     access_token = create_access_token(
#         data={"sub": user.username}, expires_delta=access_token_expires
#     )
#     return Token(access_token=access_token, token_type="bearer")
#
#
# @app.get("/users/me/", response_model=User)
# async def read_users_me(
#     current_user: Annotated[User, Depends(get_current_active_user)],
# ):
#     return current_user
#
#
# @app.get("/users/me/items/")
# async def read_own_items(
#     current_user: Annotated[User, Depends(get_current_active_user)],
# ):
#     return [{"item_id": "Foo", "owner": current_user.username}]
# from fastapi import FastAPI,  HTTPException
# from fastapi.security import HTTPBearer
# from datetime import datetime, timedelta
# from jose import jwt, JWTError


# app = FastAPI()

# SECRET_KEY = "your-secret-key"
# ALGORITHM = "HS256"
# TOKEN_EXPIRE_HOURS = 1

# security = HTTPBearer()

# # ---------------- JWT Utility ----------------
# def create_jwt(app_name: str):
#     # expire = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE_HOURS)
#     expire = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_HOURS)
#     payload = {
#         "app": app_name,
#         "exp": expire,
#         "iat": datetime.utcnow()
#     }
#     return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

# def verify_jwt(token: str):
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         if payload.get("app") != "CRM":
#             raise HTTPException(status_code=401, detail="Invalid appName in token")
#         return payload
#     except JWTError:
#         raise HTTPException(status_code=401, detail="Invalid or expired token")

# ---------------- Auth API ----------------
# @app.post("/auth/token")
# async def get_token(request: Request):
#     app_name = request.headers.get("appName")
#     if app_name != "CRM":
#         raise HTTPException(status_code=401, detail="Invalid appName header")
#     token = create_jwt(app_name)
#     return {"access_token": token, "token_type": "bearer", "expires_in": TOKEN_EXPIRE_HOURS * 3600}
#
# # ---------------- Protected APIs ----------------
# @app.post("/serial-number-request")
# async def create_serial_number_request(
#     credentials: HTTPAuthorizationCredentials = Depends(security)
# ):
#     verify_jwt(credentials.credentials)
#     return {"status": "success", "message": "Serial number request created"}
#
# @app.get("/serial-number-request")
# async def get_serial_number_request(
#     credentials: HTTPAuthorizationCredentials = Depends(security)
# ):
#     verify_jwt(credentials.credentials)
#     return {"status": "success", "data": ["SN-001", "SN-002"]}