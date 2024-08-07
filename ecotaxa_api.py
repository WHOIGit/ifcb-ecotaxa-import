import sys
import requests
import os

import dotenv

dotenv.load_dotenv()

ENDPOINT = os.getenv('ENDPOINT', 'https://ecotaxa.obs-vlfr.fr/api/')

API_TOKEN = os.getenv('API_TOKEN')

PROJECT_ID = int(os.getenv('PROJECT_ID'))

def get_auth_headers():
    return {
        'Authorization': f'Bearer {API_TOKEN}',
    }

def login(username, password):
    url = f'{ENDPOINT}login'
    data = {
        'username': username,
        'password': password
    }
    response = requests.post(url, json=data)
    response.raise_for_status()
    return response.json()

def upload_zip():
    url = f'{ENDPOINT}my_files/'
    headers = get_auth_headers()
    # make a multipart form upload
    # local_path = os.path.abspath('output/D20190402T193949_IFCB010.zip')
    local_path = os.path.abspath('foo.zip')
    # construct files, indicate that they are of type application/zip
    with open(local_path, 'rb') as f:
        files = {
            'file': (os.path.basename(local_path), f, 'application/zip')
        }
        params = {
            'tag': 'upload_test',
        }
        response = requests.post(url, files=files, data=params, headers=headers)
    return response.json()

def import_file(source_path):
    url = f'{ENDPOINT}file_import/{PROJECT_ID}'
    headers = get_auth_headers()
    data = {
        'source_path': source_path,
        'skip_existing_objects': 'true',
        'update_mode': 'Yes',
    }
    response = requests.post(url, json=data, headers=headers)
    return response.json()

def get_files(tag):
    url = f'{ENDPOINT}my_files/{tag}'
    headers = get_auth_headers()
    response = requests.get(url, headers=headers)
    return response.json()

def get_job(job_id):
    url = f'{ENDPOINT}jobs/{job_id}/'
    headers = get_auth_headers()
    response = requests.get(url, headers=headers)
    return response.json()

def main():
    source_path = upload_zip()
    print(source_path)
    print(get_files('upload_test'))
    resp = import_file(source_path)
    job_id = resp['job_id']
    print(f'job_id: {job_id}')
    print(get_job(job_id))

if __name__ == '__main__':
    main()