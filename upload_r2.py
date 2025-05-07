import os
import json
import hashlib
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

R2_ACCESS_KEY_ID = os.getenv('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.getenv('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT_URL = os.getenv('R2_ENDPOINT_URL')
R2_BUCKET_NAME = os.getenv('R2_BUCKET_NAME')
CACHE_FILE = '.r2-cache.json'


def sha256sum(file_path: str) -> str:
    h = hashlib.sha256()
    with open(file_path, 'rb') as f:
        while chunk := f.read(8192):
            h.update(chunk)
    return h.hexdigest()


def load_cache() -> dict:
    return json.load(open(CACHE_FILE)) if os.path.exists(CACHE_FILE) else {}


def save_cache(cache: dict):
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f)


def upload_folder_to_r2(folder_path: str):
    cache = load_cache()
    updated_cache = {}

    session = boto3.session.Session()
    s3 = session.client('s3',
                        endpoint_url=R2_ENDPOINT_URL,
                        aws_access_key_id=R2_ACCESS_KEY_ID,
                        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
                        config=Config(signature_version='s3v4'),
                        region_name='auto'
                        )

    for root, _, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_path, folder_path)
            file_hash = sha256sum(local_path)

            if cache.get(s3_key) == file_hash:
                print(f'Skipping (no change): {s3_key}')
                updated_cache[s3_key] = file_hash
                continue

            print(f'Uploading: {s3_key}')
            s3.upload_file(local_path, R2_BUCKET_NAME, s3_key)
            updated_cache[s3_key] = file_hash

    save_cache(updated_cache)


if __name__ == '__main__':
    upload_folder_to_r2('./public')
