import json
import decimal
import datetime
import re
import logging
from typing import Annotated

# from core.auth import (authenticate_user,fake_users_db,Token,create_access_token,ACCESS_TOKEN_EXPIRE_MINUTES,
#                        User,get_current_active_user)
from fastapi import Depends, FastAPI, HTTPException, status,Form
from datetime import datetime, timedelta
# from routers import (bucket, namespace, get_data, crm_application, table, json_data)
from routers import (bucket,namespace,invoice_data,table,branch_data)

logger = logging.getLogger(__name__)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="ignore")
        return super().default(obj)

app = FastAPI()
app.include_router(bucket.router)
app.include_router(namespace.router)
app.include_router(invoice_data.router)
app.include_router(branch_data.router)
app.include_router(table.router)

# app.include_router(crm_application.router)
# app.include_router(get_data.router)
# app.include_router(json_data.router)


ALLOWED_TABLES = ["Invoice",]

# @app.get("/")
# def root():
#     tables_name = ["CRM", ]
#
#     return {"message": "API is running",
#             "version": "1.0",
#             "Tables": tables_name
#             }

# @app.get("/sales-data")
# def get_sales_data():
#     return [
#         {"date": "2025-09-01", "product": "A", "sales": 120},
#         {"date": "2025-09-02", "product": "B", "sales": 150},
#         {"date": "2025-09-03", "product": "A", "sales": 100},
#     ]


class CustomOAuth2Form:
    def __init__(
        self,
        username: Annotated[str, Form(...)],
        password: Annotated[str, Form(...)]
    ):
        self.username = username
        self.password = password

# @app.post("/token")
# async def login_for_access_token()
#     # form_data: Annotated[CustomOAuth2Form, Depends()],
#     #     ) -> Token:
#     # user = authenticate_user(fake_users_db, form_data.username, form_data.password
#     #                          )
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


# @app.get("/users/me/", response_model=User)
# async def read_users_me(
#     current_user: Annotated[User, Depends(get_current_active_user)],
# ):
#     return current_user


# @app.get("/users/me/items/")
# async def read_own_items(
#     current_user: Annotated[User, Depends(get_current_active_user)],
# ):
#     return [{"item_id": "Foo", "owner": current_user.username}]


def convert_row(row, column_types):

    converted = []
    for value, col_type in zip(row, column_types):
        if col_type.startswith("decimal") and value is not None:
            converted.append(str(value))
        else:
            converted.append(value)
    return converted



def normalize_mysql_type(t):
    return re.sub(r"\(.*\)", "", t).strip().lower()


from fastapi import FastAPI, Depends, HTTPException, Request,Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from datetime import datetime, timedelta
from jose import jwt, JWTError
from core.auth import create_jwt,verify_jwt
from typing import Optional
from fastapi.responses import JSONResponse

security = HTTPBearer()

TOKEN_EXPIRE_HOURS = 1

@app.post("/auth/token")
async def get_token(
        request: Request,
        app_name: Optional[str] = Header(None)

):
    if not app_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing 'appName' header"
        )

    if app_name != "CRM":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid 'appName' header"
        )

    try:
        token = create_jwt(app_name)  # Your JWT creation logic
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Token generation failed: {str(e)}"
        )

    return JSONResponse(
        content={
            "access_token": token,
            "token_type": "bearer",
            "expires_in": TOKEN_EXPIRE_HOURS * 3600
        },
        status_code=status.HTTP_200_OK
    )

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

