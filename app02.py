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
                and sadate >= 20250701 
                AND SADATE <= 20250701
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
                and sadate >= 20250701 
                AND SADATE <= 20250701
            
            """
        ))
        rows = result.fetchall()
        print("Count:",len(rows))
        print("Query Results:")
        # for row in rows:
        #     print(row)# Convert row to dict for readability
        #     print("*"*50)

except SQLAlchemyError as db_err:
    print("SQLAlchemy error occurred:", db_err)

except Exception as err:
    print("Unexpected error occurred:", err)


except SQLAlchemyError as db_err:
    print("SQLAlchemy error occurred:", db_err)

except Exception as err:
    print("Unexpected error occurred:", err)