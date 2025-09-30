from fastapi import APIRouter, Query, HTTPException,Depends
from botocore.exceptions import ClientError
import logging
from core.catalog_client import get_r2_client,security,verify_jwt
# from core.auth import verify_token
# from typing import Annotated
# from core.auth import (User,get_current_active_user)
from fastapi.security import  HTTPAuthorizationCredentials


logger = logging.getLogger(__name__)
router = APIRouter(prefix="", tags=["R2 Bucket"])

@router.get("/list-buckets")
def list_buckets(
        # token_data: dict = Depends(verify_token)
    # current_user: Annotated[User, Depends(get_current_active_user)]
    # current_user: Annotated[User, Depends(get_current_active_user)]
    credentials: HTTPAuthorizationCredentials = Depends(security)

):
    # print("Token=",credentials.credentials)
    verify_jwt(credentials.credentials)
    r2_client = get_r2_client()
    try:
        response = r2_client.list_buckets()
        buckets = [b["Name"] for b in response["Buckets"]]
        return {
            "buckets": buckets,
            "token": credentials.credentials,
            # "user": current_user.username,
            # "token_data": token_data
        }
    except ClientError as e:
        return {"error": str(e)}

@router.post("/create-bucket")
def create_bucket(
        bucket_name: str = Query(..., description="Name of the new R2 bucket"),
        credentials: HTTPAuthorizationCredentials = Depends(security)
):
    verify_jwt(credentials.credentials)
    r2_client = get_r2_client()
    try:
        r2_client.create_bucket(Bucket=bucket_name)
        return {"message": f"✅ Bucket '{bucket_name}' created successfully"}
    except ClientError as e:
        return {"error": str(e)}

# @router.post("/upload-object")
# async def upload_object(
#     bucket_name: str = Query(..., description="Bucket name"),
#     object_key: str = Query(..., description="Object key (filename in bucket)"),
#     file: UploadFile = File(...)
# ):
#     """Upload or update (overwrite) an object"""
#     r2_client = get_r2_client()
#     try:
#         file_content = await file.read()
#         r2_client.put_object(Bucket=bucket_name, Key=object_key, Body=file_content)
#         return {"message": f"✅ Object '{object_key}' uploaded to bucket '{bucket_name}'"}
#     except ClientError as e:
#         logger.error(f"Upload failed: {e}")
#         raise HTTPException(status_code=400, detail=f"Upload failed: {e.response['Error']['Message']}")


@router.delete("/delete-folder")
def delete_folder(
        bucket_name: str = Query(..., description="Bucket name"),
        folder_path: str = Query(..., description="Folder path to delete (will remove all objects inside)"),
        credentials: HTTPAuthorizationCredentials = Depends(security)
):
    """Delete a folder (all objects with that prefix)"""
    verify_jwt(credentials.credentials)
    r2_client = get_r2_client()
    try:
        response = r2_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)
        if "Contents" not in response:
            return {"message": f"⚠️ No objects found in folder '{folder_path}'"}

        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        r2_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objects_to_delete})
        return {"message": f" Folder '{folder_path}' deleted with {len(objects_to_delete)} objects"}
    except ClientError as e:
        logger.error(f"Delete folder failed: {e}")
        raise HTTPException(status_code=400, detail=f"Delete folder failed: {e.response['Error']['Message']}")


@router.delete("/delete")
def delete_bucket(bucket_name: str = Query(..., description="Bucket name"),
                  force: bool = Query(False, description="Force delete non-empty bucket"),
                credentials: HTTPAuthorizationCredentials = Depends(security)
                  ):
    verify_jwt(credentials.credentials)
    r2_client = get_r2_client()
    try:
        if force:
            # Delete all objects inside before removing bucket
            objects = r2_client.list_objects_v2(Bucket=bucket_name)
            if "Contents" in objects:
                to_delete = [{"Key": obj["Key"]} for obj in objects["Contents"]]
                r2_client.delete_objects(Bucket=bucket_name, Delete={"Objects": to_delete})

        # Try deleting the bucket
        r2_client.delete_bucket(Bucket=bucket_name)
        return {"message": f"🗑️ Bucket '{bucket_name}' deleted successfully"}
    except ClientError as e:
        logger.error(f"Bucket deletion failed: {e}")
        raise HTTPException(status_code=400, detail=e.response["Error"]["Message"])


