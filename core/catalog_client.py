from dotenv import load_dotenv
import os
from pyiceberg.catalog.rest import RestCatalog
import logging
from fastapi import HTTPException
import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, MetaData, text
from sqlalchemy.exc import SQLAlchemyError
from fastapi import FastAPI,  HTTPException
from fastapi.security import HTTPBearer
from datetime import datetime, timedelta
from jose import jwt, JWTError

# import pyodbc

# print(pyodbc.drivers())
load_dotenv("config.env")

logger = logging.getLogger(__name__)

class Creds:
    def __init__(self):
        self.CATALOG_URI = os.getenv("CATALOG_URI")
        self.WAREHOUSE = os.getenv("WAREHOUSE")
        self.TOKEN = os.getenv("TOKEN")
        self.CATALOG_NAME = os.getenv("CATALOG_NAME")

    def catalog_valid(self):
        # print("CATALOG_URI:",self.CATALOG_URI)
        # print("CATALOG_URI:",self.WAREHOUSE)
        # print("CATALOG_URI:",self.TOKEN)
        if not all([self.CATALOG_URI, self.WAREHOUSE, self.TOKEN]):
            raise ValueError("Missing environment variables. Please check CATALOG_URI, WAREHOUSE, or TOKEN.")

        return RestCatalog(
            name=self.CATALOG_NAME,
            warehouse=self.WAREHOUSE,
            uri=self.CATALOG_URI,
            token=self.TOKEN
        )
def get_catalog_client():
    try:
        return Creds().catalog_valid()
    except Exception as e:
        logger.error(f"Failed to initialize Iceberg catalog client: {e}")
        raise HTTPException(status_code=500, detail="Cloudflare R2 client initialization failed")



class CloudflareR2Creds:
    def __init__(self):
        self.ACCOUNT_ID = os.getenv("ACCOUNT_ID")
        self.ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
        self.SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
        self.BUCKET_NAME = os.getenv("BUCKET_NAME")
        self.ENDPOINT = os.getenv("ENDPOINT")
        self.client = None


    def get_client(self):
        if not self.client:
            if not all([self.ACCESS_KEY_ID, self.SECRET_ACCESS_KEY, self.ENDPOINT]):
                raise ValueError("Missing Cloudflare R2 environment variables.")
        self.client = boto3.client(
            "s3",
            endpoint_url=self.ENDPOINT,
            aws_access_key_id=self.ACCESS_KEY_ID,
            aws_secret_access_key=self.SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto"
        )
        return self.client


def get_r2_client():
    try:
        return CloudflareR2Creds().get_client()
    except Exception as e:
        logger.error(f"Failed to initialize R2 client: {e}")
        raise HTTPException(status_code=500, detail="Cloudflare R2 client initialization failed")


def mssql_invoice(start_date: str, end_date: str):
    try:
        types_server = os.getenv("server_Type")
        host = os.getenv("Host")
        port = os.getenv("Port")
        db_name = os.getenv("DB_Name")
        username = os.getenv("UserName")
        password = os.getenv("Password")

        print("Database type:", types_server)

        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&Encrypt=no&TrustServerCertificate=yes"
        )

        engine = create_engine(connection_string)

        with engine.connect() as connection:
            query = text(
                """ 
                SELECT 
                    SAFDNO AS INVOICE_NO,
                    SADATE AS INVOICE_DATE,
                    PRPCOD AS ITEM_CODE,
                    PRDESC AS ITEM_NAME,
                    OMVALU AS DISCOUNT_PURPOSE,
                    H5DAMT AS DISCOUNT_AMOUNT 
                FROM  
                    DLSIDP,DLINVO,DLBOTT,DLPROD,DLOMAS 
                WHERE 
                    H5INVO = SAINVO 
                    AND OMOMAS = H5DPUR  
                    AND OMHARD = 'DPUR' 
                    AND SAINVO = BOINVO 
                    AND BOPROD = PRPROD 
                    AND BOPROD = H5PROD 
                    AND SADATE >= :start_date
                    AND SADATE <= :end_date

                """
            )
            result = connection.execute(query, {"start_date": start_date, "end_date": end_date})
            rows = result.fetchall()

            return [dict(row._mapping) for row in rows]

    except SQLAlchemyError as db_err:
        print("SQLAlchemy error occurred:", db_err)

    except Exception as err:
        print("Unexpected error occurred:", err)

def mssql_branch(start_date: str, end_date: str):
    try:
        types_server = os.getenv("server_Type")
        host = os.getenv("Host")
        port = os.getenv("Port")
        db_name = os.getenv("DB_Name")
        username = os.getenv("UserName")
        password = os.getenv("Password")

        if not all([host, db_name, username, password]):
            raise ValueError("Missing required database environment variables")

        print("Database type:", types_server)

        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server"
            "&Encrypt=no&TrustServerCertificate=yes"
        )

        engine = create_engine(connection_string)


        query = text(
            """ 
            SELECT 
                BRNAME AS Branch,
                PRPCOD AS ItemCode,
                PRDESC AS ItemName,
                SADATE as DocDate,
                SATIME as DocTime,
                SAFDNO  AS DocNo,
                SATOTA AS DocValue,
                SATYPE AS SALESTYPE,
                SLCODE AS SalespersonCode,
                SLNAME AS SALESPERSON,
                BOBQYY AS Qty,
                BOFQYY AS FreeQty,
                BOGROS AS RateUnit,(BOGROS*BOBQYY) AS Amount,
                BODSA1 AS DiscAmount1,BODSA2 AS DiscAmount2,
                BOTAXX AS TaxPerc,
                BOTAXA AS TaxAmount,
                CSNAME AS PartyName,
                CSRONE AS ContctNo,
                CSRAD1 AS ADDRESSLINE1,
                CSRAD2 AS ADDRESSLINE2,
                CSRAD3 AS ADDRESSLINE3,
                CSRPCO AS PINCODE,
                OMVALU AS CITY_NAME,
                SANOTE  AS NOTES 
            FROM 
                DLINVO,DLBOTT,DLBOFF,DLCONS,DLPROD,DLSLPR,DLOMAS 
            WHERE SAINVO = BOINVO 
                AND SABOFF = BRBOFF 
                AND SACONS = CSCONS 
                AND BOPROD = PRPROD 
                AND BOSLPR = SLSLPR 
                AND OMOMAS = CSRCIT 
                AND OMHARD ='CITY'
                and sadate >= :start_date 
                AND SADATE <= :end_date
            UNION ALL
            SELECT 
                BRNAME AS Branch,
                PRPCOD AS ItemCode,
                PRDESC AS ItemName,
                SADATE as DocDate,
                SATIME as DocTime,
                SAFDNO  AS DocNo,
                SATOTA AS DocValue,
                SATYPE AS SALESTYPE,
                SLCODE AS SalespersonCode,
                SLNAME AS SALESPERSON,
                BOBQYY AS Qty,BOFQYY AS FreeQty,
                BOGROS AS RateUnit,
                (BOGROS*BOBQYY) AS Amount,
                BODSA1 AS DiscAmount1,
                BODSA2 AS DiscAmount2,
                BOTAXX AS TaxPerc,
                BOTAXA AS TaxAmount,
                CUNAME AS PartyName,
                CUOONE AS ContctNo,
                CUOAD1 AS ADDRESSLINE1,
                CUOAD2 AS ADDRESSLINE2,
                CUOAD3 AS ADDRESSLINE3,
                CUOPCO AS PINCODE,
                OMVALU AS CITY_NAME,
                SANOTE  AS NOTES 
            FROM 
                DLINVO,DLBOTT,DLBOFF,DLCUST,DLPROD,DLSLPR,DLOMAS 
            WHERE 
                SAINVO = BOINVO
                AND SABOFF = BRBOFF 
                AND SACUST = CUCUST 
                AND BOPROD = PRPROD 
                AND BOSLPR = SLSLPR 
                AND OMOMAS = CUOCIT 
                AND OMHARD ='CITY'
                and sadate >= :start_date 
                AND SADATE <= :end_date

            """
        )
        with engine.connect() as connection:

            result = connection.execute(query, {"start_date": start_date, "end_date": end_date})
            rows = result.fetchall()

            print("Count:", len(rows))
            # print("Query Results:")
            # for row in rows:
            #     print(row)# Convert row to dict for readability
            #     print("*"*50)
            return [dict(row._mapping) for row in rows]

    except SQLAlchemyError as db_err:
        print("SQLAlchemy error occurred:", db_err)

    except Exception as err:
        print("Unexpected error occurred:", err)


SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = os.getenv("JWT_ALGORITHM")
TOKEN_EXPIRE_HOURS = os.getenv("JWT_TOKEN_EXPIRE_HOURS")

security = HTTPBearer()

# ---------------- JWT Utility ----------------
def create_jwt(app_name: str):
    # expire = datetime.utcnow() + timedelta(hours=TOKEN_EXPIRE_HOURS)
    expire = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_HOURS)
    payload = {
        "app": app_name,
        "exp": expire,
        "iat": datetime.utcnow()
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_jwt(token: str):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        if payload.get("app") != "Invoice":
            raise HTTPException(status_code=401, detail="Invalid appName in token")
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")