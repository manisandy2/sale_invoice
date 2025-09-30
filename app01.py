import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, MetaData,text
from sqlalchemy.exc import SQLAlchemyError
import pyodbc

print(pyodbc.drivers())
load_dotenv()

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
        result = connection.execute(text(
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
                AND SADATE >= 20250802 
                AND SADATE <= 20250802
            
            """
        ))
        rows = result.fetchall()
        for row in rows:
            print(row)
            print("*"*50)
        print("Count:",len(rows))
        print("Query Results:")
        # for row in rows:
        #     print(dict(row._mapping))# Convert row to dict for readability

except SQLAlchemyError as db_err:
    print("SQLAlchemy error occurred:", db_err)

except Exception as err:
    print("Unexpected error occurred:", err)


except SQLAlchemyError as db_err:
    print("SQLAlchemy error occurred:", db_err)

except Exception as err:
    print("Unexpected error occurred:", err)