from mapping import *
from pyiceberg import types
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from fastapi import APIRouter, Query, Body, HTTPException,Depends
from datetime import datetime
import uuid, json, time
import pyarrow as pa
from pyiceberg.expressions import EqualTo,And,GreaterThan,GreaterThanOrEqual,LessThan,LessThanOrEqual
from core.catalog_client import get_catalog_client,get_r2_client,mssql_invoice,security,verify_jwt
import pandas as pd
from fastapi import Request
from fastapi.security import HTTPAuthorizationCredentials


router = APIRouter(prefix="/invoice", tags=["Invoice"])

# @router.post("/create")
# def invoice(
#     namespace: str = Query(..., description="invoice (e.g. 'invoice')"),
#     table_name: str = Query(..., description="data-test pos (e.g. 'data-test pos')"),
#     start_date: str = Query("2025-08-01", description="Start Date YYYY-MM-DD"),
#     end_date: str = Query("2025-08-01", description="End Date YYYY-MM-DD"),
#     # dbname:str = Query(..., description="Database name"),
#     # metadata: Optional[Dict[str, str]] = Body(None, description="Custom metadata key/value pairs")
# ):
#     start_time = time.time()
#
#     start_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
#     end_fmt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")
#
#     rows = mysql_connect(int(start_fmt), int(end_fmt))
#     print("Data count:",len(rows))
#     # print("Mysql:",rows)
#
#     iceberg_schema, arrow_schema = build_schemas_from_mysql(rows, type_mapping, arrow_mapping)
#
#     iceberg_fields = []
#     arrow_fields = []
#
#     if rows:
#         first_row = rows[0]
#         for idx, (name, value) in enumerate(first_row.items(), start=1):
#             # Default mapping
#             py_type = type(value).__name__.lower()
#
#             # Special handling for INVOICE_DATE
#             if name == "INVOICE_DATE" and isinstance(value, int):
#                 # convert all rows to proper date strings (once)
#                 for r in rows:
#                     raw = str(r["INVOICE_DATE"])
#                     r["INVOICE_DATE"] = datetime.strptime(raw, "%Y%m%d").date()
#
#                 ice_type = DateType()
#                 arrow_type = pa.date32()
#             else:
#                 ice_type = type_mapping.get(py_type, StringType())
#                 arrow_type = arrow_mapping.get(py_type, pa.string())
#
#             iceberg_fields.append(NestedField(field_id=idx, name=name, field_type=ice_type, required=False))
#             arrow_fields.append(pa.field(name, arrow_type, nullable=True))
#
#     # finally
#     iceberg_schema = Schema(*iceberg_fields)
#     arrow_schema = pa.schema(arrow_fields)
#     # print("Iceberg:",iceberg_schema)
#     # print("#"*100)
#     # print("Arrow:",arrow_schema)
#
#     arrow_table = pa.Table.from_pylist(rows, schema=arrow_schema)
#
#     catalog = get_catalog_client()
#
#
#     table_identifier = "{}.{}".format(namespace, table_name)
#     tbl = get_or_create_table(catalog, table_identifier, iceberg_schema,[1, 2, 3])
#
#     tbl.append(arrow_table)
#     # tbl.append(arrow_table)
#
#     elapsed = time.time() - start_time
#     return {
#         "status": "success",
#         # "action": action,
#         "namespace": namespace,
#         "table": table_name,
#         "rows_written": arrow_table.num_rows,
#         "elapsed_seconds": round(elapsed, 2),
#         "schema": [f.name for f in iceberg_schema.columns],
#         # "metadata": metadata or {},
#         "table_properties": tbl.properties if hasattr(tbl, "properties") else {}
#     }

@router.post("/create")
def invoice(
    namespace: str = Query(..., description="Namespace (e.g. 'invoice')"),
    table_name: str = Query(..., description="Table (e.g. 'data-test pos')"),
    start_date: str = Query("2025-08-01", description="Start Date YYYY-MM-DD"),
    end_date: str = Query("2025-08-01", description="End Date YYYY-MM-DD"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    verify_jwt(credentials.credentials)
    start_time = time.time()

    start_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y%m%d")
    end_fmt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%Y%m%d")

    # Fetch data from MySQL
    rows = mssql_invoice(int(start_fmt), int(end_fmt))
    print(f"Data count: {len(rows)}")

    r2_client = get_r2_client()

    if not rows:
        raise HTTPException(status_code=404, detail="No rows found for given date range")

    # Convert INVOICE_DATE if needed
    uploaded_files = []
    for r in rows:

        if isinstance(r.get("INVOICE_DATE"), int):
            raw = str(r["INVOICE_DATE"])
            r["INVOICE_DATE"] = datetime.strptime(raw, "%Y%m%d").date()

        row_dict = dict(r)
        invoice_date = r["INVOICE_DATE"]
        # Format YYYY/MM/DD
        year = invoice_date.strftime("%Y")
        month = invoice_date.strftime("%m")
        day = invoice_date.strftime("%d")


        invoice_no = r["INVOICE_NO"]
        safe_invoice_no = invoice_no.replace("/", "_")
        # r2_key = f"{r["INVOICE_DATE"]}/{invoice_no}.json"
        ################## bucket ################################################################
        r2_key = f"{year}/{month}/{day}/{safe_invoice_no}.json"

        # r2_client.put_object(
        #     Bucket=namespace,
        #     Key=r2_key,
        #     Body=json.dumps(row_dict, indent=2, cls=CustomJSONEncoder).encode("utf-8"),
        #     ContentType="application/json",
        # )
        # uploaded_files.append(r2_key)


    # Infer schemas
    iceberg_fields = []
    arrow_fields = []
    first_row = rows[0]

    for idx, (name, value) in enumerate(first_row.items(), start=1):
        py_type = type(value).__name__.lower()
        ice_type = type_mapping.get(py_type, StringType())
        arrow_type = arrow_mapping.get(py_type, pa.string())
        if name == "INVOICE_DATE":
            ice_type = DateType()
            arrow_type = pa.date32()
        iceberg_fields.append(NestedField(field_id=idx, name=name, field_type=ice_type, required=False))
        arrow_fields.append(pa.field(name, arrow_type, nullable=True))

    iceberg_schema = Schema(*iceberg_fields)
    arrow_schema = pa.schema(arrow_fields)

    # Connect to Iceberg
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    tbl = get_or_create_table(catalog, table_identifier, iceberg_schema)

    # 🛑 Duplicate prevention
    # Composite key columns (adjust names as per your data)
    key_cols = ["INVOICE_NO", "INVOICE_DATE", "ITEM_CODE"]

    # Get existing keys from table
    existing_keys = set()
    try:
        existing_df = tbl.scan().to_arrow(columns=key_cols).to_pandas()
        for _, row in existing_df.iterrows():
            existing_keys.add(tuple(row[c] for c in key_cols))
    except Exception as e:
        print(f"No existing data or could not read existing keys: {e}")

    # Filter new rows
    unique_rows = []
    for r in rows:
        key = (r["INVOICE_NO"], r["INVOICE_DATE"], r["ITEM_CODE"])
        if key not in existing_keys:
            unique_rows.append(r)

    if not unique_rows:
        return {
            "status": "skipped",
            "message": "All rows already exist. Nothing inserted.",
            "uploaded_files": uploaded_files
        }

    # Append only new rows
    arrow_table = pa.Table.from_pylist(unique_rows, schema=arrow_schema)
    # tbl.append(arrow_table)
    tbl.append(arrow_table)

    elapsed = time.time() - start_time
    return {
        "status": "success",
        "inserted_rows": arrow_table.num_rows,
        "skipped_rows": len(rows) - len(unique_rows),
        "elapsed_seconds": round(elapsed, 2),
        "schema": [f.name for f in iceberg_schema.columns]
    }

# @router.delete("/delete")
# def delete_invoices(
#     namespace: str = Query(..., description="Namespace (e.g. 'invoice')"),
#     table_name: str = Query(..., description="Table (e.g. 'data-test pos')"),
#     start_date: str = Query(..., description="Start Date YYYY-MM-DD"),
#     end_date: str = Query(..., description="End Date YYYY-MM-DD")
# ):
#     start_time = time.time()
#
#     # Convert to proper format
#     start_fmt = datetime.strptime(start_date, "%Y-%m-%d").date()
#     end_fmt = datetime.strptime(end_date, "%Y-%m-%d").date()
#
#     # Connect to Iceberg
#     catalog = get_catalog_client()
#     table_identifier = f"{namespace}.{table_name}"
#     tbl = catalog.load_table(table_identifier)
#
#     # Delete condition (Iceberg supports row-level deletes with predicates)
#     # try:
#     #     (
#     #         tbl.delete_where(
#     #             (pl.col("INVOICE_DATE") >= pl.lit(start_fmt)) &
#     #             (pl.col("INVOICE_DATE") <= pl.lit(end_fmt))
#     #         )
#     #     )
#     # except Exception as e:
#     #     raise HTTPException(status_code=500, detail=f"Delete failed: {str(e)}")
#     #
#     # elapsed = time.time() - start_time
#     # return {
#     #     "status": "success",
#     #     "message": f"Deleted rows between {start_date} and {end_date}",
#     #     "elapsed_seconds": round(elapsed, 2)
#     # }
#
#     df = tbl.scan().to_arrow().to_pandas()
#
#     if "INVOICE_DATE" not in df.columns:
#         raise HTTPException(status_code=400, detail="Column INVOICE_DATE not found in table")
#
#     before_count = len(df)
#
#     # Keep rows outside the delete range
#     mask = ~((df["INVOICE_DATE"] >= pd.to_datetime(start_fmt)) & (df["INVOICE_DATE"] <= pd.to_datetime(end_fmt)))
#     filtered_df = df[mask]
#
#     deleted_count = before_count - len(filtered_df)
#
#     if deleted_count == 0:
#         return {
#             "status": "skipped",
#             "message": "No rows matched the delete filter"
#         }
#
#     # Convert back to Arrow
#     arrow_table = pa.Table.from_pandas(filtered_df)
#
#     # Overwrite the table with filtered data
#     tbl.overwrite(arrow_table)
#
#     elapsed = time.time() - start_time
#     return {
#         "status": "success",
#         "deleted_rows": deleted_count,
#         "remaining_rows": len(filtered_df),
#         "elapsed_seconds": round(elapsed, 2)
#     }

@router.delete("/delete")
def delete_invoices(
    namespace: str = Query(..., description="Namespace (e.g. 'invoice')"),
    table_name: str = Query(..., description="Table (e.g. 'data-test pos')"),
    start_date: str = Query(..., description="Start Date YYYY-MM-DD"),
    end_date: str = Query(None, description="End Date YYYY-MM-DD (optional)"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    verify_jwt(credentials.credentials)
    start_time = time.time()

    # Parse dates
    start_fmt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_fmt = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else start_fmt

    # Load Iceberg table
    catalog = get_catalog_client()
    table_identifier = f"{namespace}.{table_name}"
    tbl = catalog.load_table(table_identifier)

    # Read data into Pandas
    df = tbl.scan().to_arrow().to_pandas()

    if "INVOICE_DATE" not in df.columns:
        raise HTTPException(status_code=400, detail="Column INVOICE_DATE not found in table")

    before_count = len(df)

    # ✅ Ensure INVOICE_DATE is comparable
    if pd.api.types.is_datetime64_any_dtype(df["INVOICE_DATE"]):
        df["INVOICE_DATE"] = df["INVOICE_DATE"].dt.date

    # Keep only rows outside the delete range
    mask = ~(
        (df["INVOICE_DATE"] >= start_fmt) &
        (df["INVOICE_DATE"] <= end_fmt)
    )
    filtered_df = df[mask]

    deleted_count = before_count - len(filtered_df)
    print(f"Deleted {deleted_count} rows")
    if deleted_count == 0:
        return {
            "status": "skipped",
            "message": "No rows matched the delete filter"
        }

    # Convert back to Arrow
    # arrow_table = pa.Table.from_pandas(filtered_df)
    arrow_table = pa.Table.from_pandas(filtered_df.reset_index(drop=True), preserve_index=False)
    print("Data",arrow_table)


    # Overwrite Iceberg table
    tbl.overwrite(arrow_table)

    elapsed = time.time() - start_time
    return {
        "status": "success",
        "deleted_rows": deleted_count,
        "remaining_rows": len(filtered_df),
        "elapsed_seconds": round(elapsed, 2)
    }

# @router.post("/Normal/Create")
# def create_table_json_store(
#
#         bucket_name: str = Query(..., title="Bucket Name"),
#         dbname:str = Query(..., description="Database name"),
#         bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
#         start_range: int = Query(0, description="Start row (e.g. 0)"),
#         end_rage: int = Query(100, description="End row (e.g. 100)")
# ):
#         start_time = time.time()
#         mysql_catalog = MysqlCatalog()
#         rows = mysql_catalog.get_range(dbname,start= start_range, end=end_rage)
#         r2_client = get_r2_client()
#
#         uploaded_files = []
#
#         for row in rows:
#             row_dict = dict(row)
#             if "pri_id" not in row_dict:
#                 continue
#
#             pri_id_str = str(row_dict["pri_id"])
#             row_dict["pri_id"] = pri_id_str
#
#             # r2_key = f"iceberg_json/{namespace}_{table_name}/model_{pri_id_str}.json"
#             r2_key = f"{bucket_path}/{pri_id_str}.json"
#
#             r2_client.put_object(
#                 Bucket=bucket_name,
#                 Key=r2_key,
#                 Body=json.dumps(row_dict, indent=2, cls=CustomJSONEncoder).encode("utf-8")
#             )
#             uploaded_files.append(r2_key)
#             elapsed = time.time() - start_time
#             minutes = int(elapsed // 60)
#             seconds = int(elapsed % 60)
#
#             print("message", f"{len(uploaded_files)} JSON files uploaded to R2")
#             print("files", "uploaded_files")
#             print("Elapsed time", f"{minutes} minutes {seconds} seconds")
#
#
#         elapsed = time.time() - start_time
#         minutes = int(elapsed // 60)
#         seconds = int(elapsed % 60)
#         return {
#             "message": f"{len(uploaded_files)} JSON files uploaded to R2",
#             "files": uploaded_files,
#             "Elapsed time": f"{minutes} minutes {seconds} seconds"
#         }



@router.get("/Inspect")
def table_inspect(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    verify_jwt(credentials.credentials)
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))

        snapshots = list(table.snapshots())

        snapshot_data = []
        for s in snapshots:
            snapshot_data.append({
                "snapshot_id": getattr(s, "snapshot_id", None),
                "parent_snapshot_id": getattr(s, "parent_snapshot_id", None),
                "timestamp_ms": getattr(s, "timestamp_ms", None),
                "manifest_list": getattr(s, "manifest_list", None),
                "summary": getattr(s, "summary", {})
            })

        return {
            "namespace": namespace,
            "table_name": table.name,
            "records_count": len(snapshots),
            "snapshots": snapshot_data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to inspect table: {str(e)}")

@router.get("/filter")
def get_data(
    namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
    table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        catalog = get_catalog_client()
        table = catalog.load_table((namespace, table_name))
    except Exception as e:

        raise HTTPException(status_code=500, detail="Error loading table from catalog")

    try:
        scan = table.scan()
        files = [task.file.file_path for task in scan.plan_files()]
    except Exception as e:

        raise HTTPException(status_code=500, detail="Error scanning table files")

    try:
        snapshots = list(table.snapshots())
        snapshot_data = []
        for s in snapshots:
            ts = getattr(s, "timestamp_ms", None)
            snapshot_data.append({
                "snapshot_id": getattr(s, "snapshot_id", None),
                "parent_snapshot_id": getattr(s, "parent_snapshot_id", None),
                "timestamp_ms": ts,
                "manifest_list": getattr(s, "manifest_list", None),
                "date_time": datetime.fromtimestamp(ts / 1000.0).strftime("%Y-%m-%d %H:%M:%S") if ts else None,
                "summary": getattr(s, "summary", {})
            })
    except Exception as e:

        raise HTTPException(status_code=500, detail="Error retrieving snapshot data")

    return {
        "namespace": namespace,
        "table_name": f"{namespace}.{table_name}",
        "records_count": len(snapshots),
        "snapshots": snapshot_data,
        "parquet_files": files
    }

# @router.get("/filter-data")
# def get_filtered_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'CRM_Application')"),
#     table_name: str = Query(..., description="Table name (e.g. 'Serial')"),
#     columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
#     values: list[str] = Query(..., description="Values to match (in same order as columns)")
# ):
#     start_time = time.time()
#     print(columns)
#     print(values)
#     try:
#         if len(columns) != len(values):
#             return {
#                 "status": "error",
#                 "error_code": "MISMATCHED_COLUMNS_VALUES",
#                 "message": "Number of columns and values must match",
#                 "data": []
#             }
#
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         schema_fields = {f.name for f in table.schema().fields}
#         for col in columns:
#             if col not in schema_fields:
#                 return {
#                     "status": "error",
#                     "error_code": "COLUMN_NOT_FOUND",
#                     "message": f"Column '{col}' not found in schema",
#                     "data": []
#                 }
#
#         filter_expr = None
#         for col, val in zip(columns, values):
#             expr = EqualTo(col, val)
#             filter_expr = expr if filter_expr is None else And (filter_expr, expr)
#
#
#         # Apply filter
#         scan = table.scan(row_filter=filter_expr)
#         # print(scan.to_arrow())
#
#         rows = []
#         arrow_table = scan.to_arrow()
#
#         for batch in arrow_table.to_batches():
#             rows.extend(batch.to_pylist())
#
#         elapsed = round(time.time() - start_time, 2)
#         print("Data len:",len(rows))
#
#         try:
#             if len(rows) == 0:
#                 return {"status": "error", "error_code": "NO_DATA", "message": "No data found"}
#         except Exception as e:
#             return {"status": "error", "error_code": "NO_DATA", "message": str(e)}
#
#
#         return {
#             "status": "success",
#             # "metadata": metadata,
#             "count": len(scan.to_arrow()),
#             "data": scan.to_arrow().to_pylist()[:5],
#             "status_Code": 200,
#         }
#
#
#     except Exception as e:
#         return {
#             "status": "error",
#             "error_code": "INTERNAL_SERVER_ERROR",
#             "message": f"Error filtering data: {str(e)}",
#             "data": []
#         }

# @router.get("/filter-data")
# def get_filtered_data(
#     namespace: str = Query(..., description="Namespace (e.g. 'crm')"),
#     table_name: str = Query(..., description="Table name (e.g. 'serial_number_requests')"),
#     columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
#     values: list[str] = Query(..., description="Values to match (in same order as columns)")
# ):
#     # print(columns)
#     # print(values)
#     start_time = time.time()
#
#     # Handle comma-separated inputs
#     if len(columns) == 1 and ',' in columns[0]:
#         columns = [c.strip() for c in columns[0].split(',')]
#     if len(values) == 1 and ',' in values[0] and len(columns) > 1:
#         values = [v.strip() for v in values[0].split(',')]
#
#     try:
#         # Check lengths
#         if len(columns) != len(values):
#             raise HTTPException(
#                 status_code=400,
#                 detail={
#                     "status": "error",
#                     "error_code": "MISMATCHED_COLUMNS_VALUES",
#                     "message": "Number of columns and values must match",
#                     "data": [],
#                     "status_code": 400
#                 }
#             )
#
#         # Load table
#         # catalog = get_catalog_client()
#         # table = catalog.load_table((namespace, table_name))
#         try:
#             catalog = get_catalog_client()
#         except Exception as e:
#             raise HTTPException(
#                 status_code=500,
#                 detail={
#                     "status": "error",
#                     "error_code": "CATALOG_CONNECTION_FAILED",
#                     "message": f"Failed to connect to catalog: {str(e)}",
#                     "data": [],
#                     "status_code": 500
#                 }
#             )
#         try:
#             table = catalog.load_table((namespace, table_name))
#         except Exception as e:
#             raise HTTPException(
#                 status_code=404,
#                 detail={
#                     "status": "error",
#                     "error_code": "TABLE_NOT_FOUND",
#                     "message": f"Table '{namespace}.{table_name}' not found or could not be loaded: {str(e)}",
#                     "data": [],
#                     "status_code": 404
#                 }
#             )
#
#         # Validate columns
#         schema_fields = {f.name for f in table.schema().fields}
#         for col in columns:
#             if col not in schema_fields:
#                 raise HTTPException(
#                     status_code=404,
#                     detail={
#                         "status": "error",
#                         "error_code": "COLUMN_NOT_FOUND",
#                         "message": f"Column '{col}' not found in schema",
#                         "data": [],
#                         "status_code": 404
#                     }
#                 )
#         filters = None
#         for col, val in zip(columns, values):
#             if col == "INVOICE_DATE" and ',' in val:
#                 start_date, end_date = [v.strip() for v in val.split(',')]
#                 condition = And(
#                     GreaterThanOrEqual(col, start_date),
#                     LessThanOrEqual(col, end_date)
#                 )
#             else:
#                 condition = EqualTo(col, val)
#             filters = condition if filters is None else And(filters, condition)
#
#         # # Apply filter
#         scan = table.scan(row_filter=filters)
#         arrow_table = scan.to_arrow()
#
#         rows = []
#         for batch in arrow_table.to_batches():
#             rows.extend(batch.to_pylist())
#
#         # elapsed = round(time.time() - start_time, 2)
#
#         if len(rows) == 0:
#             raise HTTPException(
#                 status_code=404,
#                 detail={
#                     "status": "error",
#                     "error_code": "NO_DATA",
#                     "message": "No data found",
#                     "data": [],
#                     "status_code": 404
#                 }
#             )
#         rows = arrow_table.to_pylist()
#         elapsed = round(time.time() - start_time, 2)
#
#         return {
#             "status": "success",
#             "status_code": 200,
#             "count": arrow_table.num_rows,
#             # "data": rows[:50],
#             "data": rows,
#             "execution_time_seconds": elapsed
#         }
#
#     except HTTPException as http_err:
#         # Re-raise known HTTP errors
#         raise http_err
#
#     except Exception as e:
#         # Catch any unknown errors
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "status": "error",
#                 "error_code": "INTERNAL_ERROR",
#                 "message": str(e),
#                 "data": [],
#                 "status_code": 500
#             }
#         )
@router.get("/filter-data")
def get_filtered_data(
    request: Request,
    namespace: str = Query(..., description="Namespace (e.g. 'invoice')"),
    table_name: str = Query(..., description="Table name (e.g. 'invoice-data')"),
    columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
    values: list[str] = Query(..., description="Values to match (in same order as columns)"),
    page: int = Query(1, ge=1, description="Page number (starting from 1)"),
    page_size: int = Query(50, ge=1, le=500, description="Number of records per page"),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    verify_jwt(credentials.credentials)
    start_time = time.time()

    # Handle comma-separated inputs
    if len(columns) == 1 and ',' in columns[0]:
        columns = [c.strip() for c in columns[0].split(',')]
    if len(values) == 1 and ',' in values[0] and len(columns) > 1:
        values = [v.strip() for v in values[0].split(',')]

    try:
        if len(columns) != len(values):
            raise HTTPException(
                status_code=400,
                detail={
                    "status": "error",
                    "error_code": "MISMATCHED_COLUMNS_VALUES",
                    "message": "Number of columns and values must match",
                    "data": [],
                    "status_code": 400
                }
            )

        try:
            catalog = get_catalog_client()
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail={
                    "status": "error",
                    "error_code": "CATALOG_CONNECTION_FAILED",
                    "message": f"Failed to connect to catalog: {str(e)}",
                    "data": [],
                    "status_code": 500
                }
            )
        try:
            table = catalog.load_table((namespace, table_name))
        except Exception as e:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "error_code": "TABLE_NOT_FOUND",
                    "message": f"Table '{namespace}.{table_name}' not found or could not be loaded: {str(e)}",
                    "data": [],
                    "status_code": 404
                }
            )

        schema_fields = {f.name for f in table.schema().fields}
        for col in columns:
            if col not in schema_fields:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "status": "error",
                        "error_code": "COLUMN_NOT_FOUND",
                        "message": f"Column '{col}' not found in schema",
                        "data": [],
                        "status_code": 404
                    }
                )

        filters = None
        for col, val in zip(columns, values):
            if col == "INVOICE_DATE" and ',' in val:
                start_date, end_date = [v.strip() for v in val.split(',')]
                condition = And(
                    GreaterThanOrEqual(col, start_date),
                    LessThanOrEqual(col, end_date)
                )
            else:
                condition = EqualTo(col, val)
            filters = condition if filters is None else And(filters, condition)

        scan = table.scan(row_filter=filters)
        arrow_table = scan.to_arrow()

        rows = arrow_table.to_pylist()

        if len(rows) == 0:
            raise HTTPException(
                status_code=404,
                detail={
                    "status": "error",
                    "error_code": "NO_DATA",
                    "message": "No data found",
                    "data": [],
                    "status_code": 404
                }
            )

        # ✅ Pagination logic
        total_count = len(rows)
        total_pages = (total_count + page_size - 1)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_rows = rows[start_index:end_index]

        elapsed = round(time.time() - start_time, 2)

        # ✅ Build URLs
        base_url = str(request.url).split("?")[0]  # endpoint path only
        query_params = dict(request.query_params)

        query_params["page_size"] = str(page_size)

        next_page_url = None
        prev_page_url = None

        if page < total_pages:
            query_params["page"] = str(page + 1)
            next_page_url = f"{base_url}?{query_params}"

        if page > 1:
            query_params["page"] = str(page - 1)
            prev_page_url = f"{base_url}?{query_params}"

        return {
            "status": "success",
            "status_code": 200,
            "count": len(paginated_rows),
            "total_count": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "next_page_url": next_page_url,
            "prev_page_url": prev_page_url,
            "data": paginated_rows,
            "execution_time_seconds": elapsed
        }

    except HTTPException as http_err:
        raise http_err
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "error_code": "INTERNAL_ERROR",
                "message": str(e),
                "data": [],
                "status_code": 500
            }
        )


#
# from pydantic import BaseModel
#
# class DeleteSnapshotRequest(BaseModel):
#     snapshot_id: str
#
# @router.get("/Inspect")
# def table_inspect(
#     namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
#     table_name: str = Query(..., description="Table name (e.g. 'Table name')")
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         snapshots = list(table.snapshots())
#
#         snapshot_data = []
#         for s in snapshots:
#             snapshot_data.append({
#                 "snapshot_id": getattr(s, "snapshot_id", None),
#                 "parent_snapshot_id": getattr(s, "parent_snapshot_id", None),
#                 "timestamp_ms": getattr(s, "timestamp_ms", None),
#                 "manifest_list": getattr(s, "manifest_list", None),
#                 "summary": getattr(s, "summary", {})
#             })
#
#         return {
#             "namespace": namespace,
#             "table_name": table.name,
#             "records_count": len(snapshots),
#             "snapshots": snapshot_data
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to inspect table: {str(e)}")
#
#
# @router.delete("/DeleteSnapshot")
# def delete_snapshot(
#     namespace: str = Query(..., description="Namespace (e.g. 'Namespace')"),
#     table_name: str = Query(..., description="Table name (e.g. 'Table name')"),
#     req: DeleteSnapshotRequest = Body(...)
# ):
#     try:
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         # Rollback to the snapshot (remove all snapshots after it)
#         ops = table.manage_snapshots()
#         ops.rollback_to(req.snapshot_id)
#         ops.commit()
#
#         return {
#             "message": f"Rollback to snapshot {req.snapshot_id} completed successfully"
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to rollback: {str(e)}")
#
# @router.get("/filter-data")
# def get_filtered_datas(
#     namespace: str = Query(..., description="Namespace (e.g. 'crm')"),
#     table_name: str = Query(..., description="Table name (e.g. 'serial_number_requests')"),
#     columns: list[str] = Query(..., description="Columns to filter on (comma-separated or multiple query params)"),
#     values: list[str] = Query(..., description="Values to match (in same order as columns)")
# ):
#     start_time = time.time()
#
#     # Handle comma-separated inputs
#     if len(columns) == 1 and ',' in columns[0]:
#         columns = [c.strip() for c in columns[0].split(',')]
#     if len(values) == 1 and ',' in values[0]:
#         values = [v.strip() for v in values[0].split(',')]
#
#     try:
#         # Check lengths
#         if len(columns) != len(values):
#             raise HTTPException(
#                 status_code=400,
#                 detail={
#                     "status": "error",
#                     "error_code": "MISMATCHED_COLUMNS_VALUES",
#                     "message": "Number of columns and values must match",
#                     "data": []
#                 }
#             )
#
#         # Load table
#         catalog = get_catalog_client()
#         table = catalog.load_table((namespace, table_name))
#
#         # Validate columns
#         schema_fields = {f.name for f in table.schema().fields}
#         for col in columns:
#             if col not in schema_fields:
#                 raise HTTPException(
#                     status_code=404,
#                     detail={
#                         "status": "error",
#                         "error_code": "COLUMN_NOT_FOUND",
#                         "message": f"Column '{col}' not found in schema",
#                         "data": []
#                     }
#                 )
#
#         # Build filter expression
#         filters = None
#         for col, val in zip(columns, values):
#             condition = EqualTo(col, val)
#             filters = condition if filters is None else And(filters, condition)
#
#         # Apply filter
#         scan = table.scan(row_filter=filters)
#         arrow_table = scan.to_arrow()
#
#         rows = []
#         for batch in arrow_table.to_batches():
#             rows.extend(batch.to_pylist())
#
#         elapsed = round(time.time() - start_time, 2)
#
#         if len(rows) == 0:
#             raise HTTPException(
#                 status_code=404,
#                 detail={
#                     "status": "error",
#                     "error_code": "NO_DATA",
#                     "message": "No data found",
#                     "data": []
#                 }
#             )
#
#         return {
#             "status": "success",
#             "status_code": 200,
#             "count": len(scan.to_arrow()),
#             "data": scan.to_arrow().to_pylist(),
#             "execution_time_seconds": elapsed
#         }
#
#     except HTTPException as http_err:
#         # Re-raise known HTTP errors
#         raise http_err
#
#     except Exception as e:
#         # Catch any unknown errors
#         raise HTTPException(
#             status_code=500,
#             detail={
#                 "status": "error",
#                 "error_code": "INTERNAL_ERROR",
#                 "message": str(e),
#                 "data": []
#             }
#         )


# @router.get("/GetOne")
# def get_one(
#     namespace: str = Query(..., description="Namespace (bucket name in R2)"),
#     table_name: str = Query(..., description="Table name"),
#     year: Optional[int] = Query(None, description="Year (YYYY)"),
#     month: int = Query(...),
#     day: int = Query(...),
#     ticket_id: str = Query(..., description="Ticket ID"),
# ):
#     r2_client = get_r2_client()
#     key = f"{namespace}/{table_name}/{year}/{month}/{day}/{ticket_id}.json"
#
#     try:
#         obj = r2_client.get_object(Bucket=namespace, Key=key)
#         body = obj["Body"].read().decode("utf-8")
#         return json.loads(body)
#     except Exception as e:
#         raise HTTPException(status_code=404, detail=f"Object not found: {str(e)}")
# from typing import Optional, List
# from pydantic import BaseModel
#
# class RecordModel(BaseModel):
#     ticketId: str
#     createdAt: str
#     mobileNo: Optional[str] = None
#     warehouseCode: Optional[str] = None
#     productName: Optional[str] = None
#     itemCode: Optional[str] = None
#     branchName: Optional[str] = None
#
# @router.get("/GetRecords", response_model=List[RecordModel])
# def get_records(
#     namespace: str = Query(..., description="Namespace (bucket name in R2)"),
#     table_name: str = Query(..., description="Table name"),
#     year: Optional[int] = Query(None, description="Year (YYYY)"),
#     month: Optional[int] = Query(None, description="Month (MM)"),
#     day: Optional[int] = Query(None, description="Day (DD)"),
#     ticket_id: Optional[str] = Query(None, description="Ticket ID"),
#     mobile_no: Optional[str] = Query(None, description="Mobile Number"),
#     warehouse_code: Optional[str] = Query(None, description="Warehouse Code"),
#     product_name: Optional[str] = Query(None, description="Product Name"),
#     item_code: Optional[str] = Query(None, description="Item Code"),
#     branch_name: Optional[str] = Query(None, description="Branch Name"),
# ):
#     r2_client = get_r2_client()
#
#     # Prefix to narrow search (e.g. /namespace/table/year/month/day/)
#     prefix_parts = [table_name]
#     if year: prefix_parts.append(str(year))
#     if month: prefix_parts.append(str(month))
#     if day: prefix_parts.append(str(day))
#
#     prefix = "/".join(prefix_parts)
#     print(ticket_id)
#
#     try:
#         response = r2_client.list_objects_v2(Bucket=namespace, Prefix=prefix)
#         print(response)
#         records = []
#
#         if "Contents" not in response:
#             return []
#
#         for obj in response["Contents"]:
#             key = obj["Key"]
#             file_obj = r2_client.get_object(Bucket=namespace, Key=key)
#             body = file_obj["Body"].read().decode("utf-8")
#             record = json.loads(body)
#
#             # Apply filters
#             if ticket_id and record.get("ticketId") != ticket_id:
#                 continue
#             if mobile_no and record.get("mobileNo") != mobile_no:
#                 continue
#             if warehouse_code and record.get("warehouseCode") != warehouse_code:
#                 continue
#             if product_name and record.get("productName") != product_name:
#                 continue
#             if item_code and record.get("itemCode") != item_code:
#                 continue
#             if branch_name and record.get("branchName") != branch_name:
#                 continue
#
#             records.append(RecordModel(**record))
#             print("data:",records)
#
#         return records
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error fetching records: {str(e)}")

@router.get("/list")
def get_bucket_list(
    bucket_name: str = Query(..., title="Bucket Name"),
    bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
    credentials: HTTPAuthorizationCredentials = Depends(security)

):
    verify_jwt(credentials.credentials)
    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"

    try:
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        print(response)

        files = []
        if "Contents" in response:
            files = [obj["Key"] for obj in response["Contents"]]

        return {

            "total_files": len(files),
            "files": files
        }

    except Exception as e:
        return {"error": str(e)}

@router.delete("/delete-files")
def delete_files(
        bucket_name: str = Query(..., title="Bucket Name"),
        bucket_path: str = Query("iceberg_json", description="Folder path in R2 (default: iceberg_json)"),
        prefix_only: bool = Query(True, description="Delete all files under prefix (True) or specific file (False)"),
        file_name: str = Query(None, description="Specific file name (e.g. 'batch_0.json') if prefix_only=False"),
        credentials: HTTPAuthorizationCredentials = Depends(security)
):
    verify_jwt(credentials.credentials)
    r2_client = get_r2_client()
    prefix = f"{bucket_path}/"
    print("Deleting files")
    print(f"{prefix}")
    try:
        deleted_files = []

        if prefix_only:
            response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    print(obj["Key"])
                    r2_client.delete_object(Bucket=bucket_name, Key=obj["Key"])
                    deleted_files.append(obj["Key"])
        else:
            if not file_name:
                return {"error": "file_name is required if prefix_only=False"}

            file_key = prefix + file_name
            r2_client.delete_object(Bucket=bucket_name, Key=file_key)
            deleted_files.append(file_key)

        return {
            "message": f"{len(deleted_files)} file(s) deleted",
            "deleted_files": deleted_files
        }

    except Exception as e:
        return {"error": str(e)}