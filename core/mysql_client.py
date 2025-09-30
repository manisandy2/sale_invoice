# # import mysql.connector
# # from dotenv import load_dotenv
# # import os
# # from mysql.connector import Error
# # # import mysql
# #
# # load_dotenv()
#
# # def mysql_connect():
# #     print("Connecting to MySQL database...")
# #     print(os.getenv("HOST"))
# #     print(os.getenv("MYSQL_USER"))
# #     # print(os.getenv("PASSWORD"))
# #     # print(os.getenv("DATABASE"))
# #     try:
# #         conn = mysql.connector.connect(
# #             host=os.getenv("HOST"),
# #             user=os.getenv("MYSQL_USER"),
# #             password=os.getenv("PASSWORD"),
# #             database=os.getenv("DATABASE"),
# #             port=3306
# #         )
# #         if conn.is_connected():
# #             print("✅ MySQL connection established")
# #             return conn
# #         else:
# #             raise ConnectionError("❌ MySQL connection could not be established")
# #     except Error as e:
# #         print(f"❌ Error connecting to MySQL: {e}")
# #         return None
# #
# # ALLOWED_TABLES = ["Transaction", "employees","POS_Transactions"]
# #
# # class MysqlCatalog:
# #     def __init__(self):
# #         self.conn = mysql_connect()
# #         # self.cursor = self.conn.cursor()
# #         self.cursor = self.conn.cursor(dictionary=True)
# #         # self.table_name = "employees"
# #         # self.table_name = "Transaction"
# #
# #     def _validate_table(self, table_name: str):
# #         if table_name not in ALLOWED_TABLES:
# #             raise ValueError(f"Invalid table name: {table_name}")
# #
# #     def get_all_value(self,table_name):
# #         self._validate_table(table_name)
# #         self.cursor.execute(f"SELECT * FROM {table_name}")
# #         return self.cursor.fetchall()
# #
# #     def get_count(self,table_name:str):
# #         self._validate_table(table_name)
# #         self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
# #         # print("Count :",self.cursor.fetchone()["COUNT(*)"])
# #         # print(self.cursor.fetchone()["COUNT(*)"])
# #         return self.cursor.fetchone()["COUNT(*)"]
# #
# #     def get_describe(self,table_name:str):
# #         self._validate_table(table_name)
# #         self.cursor.execute(f"DESCRIBE {table_name}")
# #         # print("Describe:",self.cursor.fetchall())
# #         # return self.cursor.fetchall()
# #         result = self.cursor.fetchall()
# #         # print("Describe:", result)
# #         return result
# #
# #     def get_range(self,table_name:str, start: int, end: int):
# #         self._validate_table(table_name)
# #         self.cursor.execute(f"SELECT * FROM {table_name} LIMIT {start}, {end - start}")
# #         return self.cursor.fetchall()
# #
# #     def close(self):
# #         if self.cursor:
# #             self.cursor.close()
# #         if self.conn:
# #             self.conn.close()
#
#
# import os
# from dotenv import load_dotenv
# # from duckdb.duckdb import query
# from sqlalchemy import create_engine, inspect, Table, Column, Integer, String, MetaData, text
# from sqlalchemy.exc import SQLAlchemyError
# import pyodbc
#
# print(pyodbc.drivers())
# load_dotenv("config.env")
#
#
# # def mysql_connect():
# def mysql_invoice(start_date: str, end_date: str):
#     try:
#         types_server = os.getenv("server_Type")
#         host = os.getenv("Host")
#         port = os.getenv("Port")
#         db_name = os.getenv("DB_Name")
#         username = os.getenv("UserName")
#         password = os.getenv("Password")
#
#         print("Database type:", types_server)
#
#         connection_string = (
#             f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db_name}"
#             "?driver=ODBC+Driver+17+for+SQL+Server"
#             "&Encrypt=no&TrustServerCertificate=yes"
#         )
#
#         engine = create_engine(connection_string)
#
#         with engine.connect() as connection:
#             query  = text(
#                 """
#                 SELECT
#                     SAFDNO AS INVOICE_NO,
#                     SADATE AS INVOICE_DATE,
#                     PRPCOD AS ITEM_CODE,
#                     PRDESC AS ITEM_NAME,
#                     OMVALU AS DISCOUNT_PURPOSE,
#                     H5DAMT AS DISCOUNT_AMOUNT
#                 FROM
#                     DLSIDP,DLINVO,DLBOTT,DLPROD,DLOMAS
#                 WHERE
#                     H5INVO = SAINVO
#                     AND OMOMAS = H5DPUR
#                     AND OMHARD = 'DPUR'
#                     AND SAINVO = BOINVO
#                     AND BOPROD = PRPROD
#                     AND BOPROD = H5PROD
#                     AND SADATE >= :start_date
#                     AND SADATE <= :end_date
#
#                 """
#             )
#             result = connection.execute(query, {"start_date": start_date, "end_date": end_date})
#             rows = result.fetchall()
#
#             return [dict(row._mapping) for row in rows]
#
#     except SQLAlchemyError as db_err:
#         print("SQLAlchemy error occurred:", db_err)
#
#     except Exception as err:
#         print("Unexpected error occurred:", err)
#
#
#
# def mssql_branch(start_date: str, end_date: str):
#     try:
#         types_server = os.getenv("server_Type")
#         host = os.getenv("Host")
#         port = os.getenv("Port")
#         db_name = os.getenv("DB_Name")
#         username = os.getenv("UserName")
#         password = os.getenv("Password")
#
#         if not all([host, db_name, username, password]):
#             raise ValueError("Missing required database environment variables")
#
#         print("Database type:", types_server)
#
#         connection_string = (
#             f"mssql+pyodbc://{username}:{password}@{host}:{port}/{db_name}"
#             "?driver=ODBC+Driver+17+for+SQL+Server"
#             "&Encrypt=no&TrustServerCertificate=yes"
#         )
#
#         engine = create_engine(connection_string)
#
#
#         query = text(
#             """
#             SELECT
#                 BRNAME AS Branch,
#                 PRPCOD AS ItemCode,
#                 PRDESC AS ItemName,
#                 SADATE as DocDate,
#                 SATIME as DocTime,
#                 SAFDNO  AS DocNo,
#                 SATOTA AS DocValue,
#                 SATYPE AS SALESTYPE,
#                 SLCODE AS SalespersonCode,
#                 SLNAME AS SALESPERSON,
#                 BOBQYY AS Qty,
#                 BOFQYY AS FreeQty,
#                 BOGROS AS RateUnit,(BOGROS*BOBQYY) AS Amount,
#                 BODSA1 AS DiscAmount1,BODSA2 AS DiscAmount2,
#                 BOTAXX AS TaxPerc,
#                 BOTAXA AS TaxAmount,
#                 CSNAME AS PartyName,
#                 CSRONE AS ContctNo,
#                 CSRAD1 AS ADDRESSLINE1,
#                 CSRAD2 AS ADDRESSLINE2,
#                 CSRAD3 AS ADDRESSLINE3,
#                 CSRPCO AS PINCODE,
#                 OMVALU AS CITY_NAME,
#                 SANOTE  AS NOTES
#             FROM
#                 DLINVO,DLBOTT,DLBOFF,DLCONS,DLPROD,DLSLPR,DLOMAS
#             WHERE SAINVO = BOINVO
#                 AND SABOFF = BRBOFF
#                 AND SACONS = CSCONS
#                 AND BOPROD = PRPROD
#                 AND BOSLPR = SLSLPR
#                 AND OMOMAS = CSRCIT
#                 AND OMHARD ='CITY'
#                 and sadate >= :start_date
#                 AND SADATE <= :end_date
#             UNION ALL
#             SELECT
#                 BRNAME AS Branch,
#                 PRPCOD AS ItemCode,
#                 PRDESC AS ItemName,
#                 SADATE as DocDate,
#                 SATIME as DocTime,
#                 SAFDNO  AS DocNo,
#                 SATOTA AS DocValue,
#                 SATYPE AS SALESTYPE,
#                 SLCODE AS SalespersonCode,
#                 SLNAME AS SALESPERSON,
#                 BOBQYY AS Qty,BOFQYY AS FreeQty,
#                 BOGROS AS RateUnit,
#                 (BOGROS*BOBQYY) AS Amount,
#                 BODSA1 AS DiscAmount1,
#                 BODSA2 AS DiscAmount2,
#                 BOTAXX AS TaxPerc,
#                 BOTAXA AS TaxAmount,
#                 CUNAME AS PartyName,
#                 CUOONE AS ContctNo,
#                 CUOAD1 AS ADDRESSLINE1,
#                 CUOAD2 AS ADDRESSLINE2,
#                 CUOAD3 AS ADDRESSLINE3,
#                 CUOPCO AS PINCODE,
#                 OMVALU AS CITY_NAME,
#                 SANOTE  AS NOTES
#             FROM
#                 DLINVO,DLBOTT,DLBOFF,DLCUST,DLPROD,DLSLPR,DLOMAS
#             WHERE
#                 SAINVO = BOINVO
#                 AND SABOFF = BRBOFF
#                 AND SACUST = CUCUST
#                 AND BOPROD = PRPROD
#                 AND BOSLPR = SLSLPR
#                 AND OMOMAS = CUOCIT
#                 AND OMHARD ='CITY'
#                 and sadate >= :start_date
#                 AND SADATE <= :end_date
#
#             """
#         )
#         with engine.connect() as connection:
#
#             result = connection.execute(query, {"start_date": start_date, "end_date": end_date})
#             rows = result.fetchall()
#
#             print("Count:", len(rows))
#             # print("Query Results:")
#             # for row in rows:
#             #     print(row)# Convert row to dict for readability
#             #     print("*"*50)
#             return [dict(row._mapping) for row in rows]
#
#     except SQLAlchemyError as db_err:
#         print("SQLAlchemy error occurred:", db_err)
#
#     except Exception as err:
#         print("Unexpected error occurred:", err)
