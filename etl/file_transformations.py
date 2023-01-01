import os
import re
from io import StringIO
from zipfile import ZipFile

def return_zipfile(data_dir: str) -> str:
    adidas_file = ''; 
    adidas_file = [os.path.join(data_dir, file) for file in os.listdir(data_dir) if file.endswith('.zip')][0]
    return adidas_file

def return_target_member(zo: object) -> str: 
    file_member = [file for file in zo.namelist() if (file.startswith('archive') and file.endswith('.csv'))][0]
    return file_member

def unzip_in_memory(file: str) -> list:
    file_in_memory = StringIO()
    zip_archive_obj = ZipFile(file)
    target_file_member = return_target_member(zip_archive_obj)
    file_in_memory = zip_archive_obj.open(target_file_member, mode='r')
    data = [(lines.decode()) for lines in file_in_memory.readlines()]
    return data

def format_columns(columns: list) -> list: 
    clean_columns = [re.sub('(\s+)', '_', header.strip()) for header in columns]
    return clean_columns

def clean_data_and_columns(data: list) -> tuple:
    columns = format_columns(data[:1])
    data = [row.strip().split('\n') for row in data[1:]]
    return data, columns