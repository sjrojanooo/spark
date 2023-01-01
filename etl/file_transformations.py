import os
import re
from io import StringIO, BytesIO
from zipfile import ZipFile

def return_zipfile(data_dir: str) -> str:
    adidas_file = ''; 
    adidas_file = [os.path.join(data_dir, file) for file in os.listdir(data_dir) if file.endswith('.zip')][0]
    return adidas_file

def return_target_member(zo: object) -> str: 
    file_member = [file for file in zo.namelist() if (file.startswith('archive') and file.endswith('.csv'))][0]
    return file_member

def unzip_in_memory(file: str) -> list:
    in_memory_data = StringIO()
    with ZipFile(file) as fo: 
        target_file_member = return_target_member(fo)
        with fo.open(target_file_member, mode='r') as file_contents:
            decode_contents = '\n'.join([x.decode() for x in file_contents.readlines()])
            in_memory_data.write(decode_contents)
    return in_memory_data.getvalue()

def format_columns(columns: list) -> list: 
    clean_columns = [re.sub('(\s+)', '_', header.strip()) for header in columns]
    return clean_columns

def clean_data_and_columns(in_memory_data: str) -> tuple:
    columns = format_columns(in_memory_data.split('\r\n')[:1])
    data = [row.strip().split('\n') for row in in_memory_data[1:].split('\r\n')]
    return data, columns