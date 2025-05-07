import os
import json
import hashlib
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

DO_SPACES_KEY = os.getenv('DO_SPACES_KEY')
DO_SPACES_SECRET = os.getenv('DO_SPACES_SECRET')
DO_SPACES_ENDPOINT = os.getenv('DO_SPACES_ENDPOINT')
DO_SPACES_BUCKET = os.getenv('DO_SPACES_BUCKET')
CACHE_FILE = '.spaces-cache.json'


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


def upload_folder_to_spaces(folder_path: str):
    cache = load_cache()
    updated_cache = {}

    session = boto3.session.Session()
    s3 = session.client('s3',
                        endpoint_url=DO_SPACES_ENDPOINT,
                        aws_access_key_id=DO_SPACES_KEY,
                        aws_secret_access_key=DO_SPACES_SECRET,
                        config=Config(signature_version='s3v4'),
                        region_name='us-east-1'  # DigitalOcean default
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
            s3.upload_file(local_path, DO_SPACES_BUCKET, s3_key,
                           ExtraArgs={'ACL': 'public-read'})
            updated_cache[s3_key] = file_hash

    save_cache(updated_cache)


if __name__ == '__main__':
    upload_folder_to_spaces('./public')
