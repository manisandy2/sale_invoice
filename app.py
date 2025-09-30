import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect
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

    # Try connecting and inspecting
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print("Tables in database:", tables)

except SQLAlchemyError as db_err:
    print("SQLAlchemy error occurred:", db_err)

except Exception as err:
    print("Unexpected error occurred:", err)