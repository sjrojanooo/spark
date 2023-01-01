import os
import re
import csv
from io import StringIO, TextIOWrapper
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
    writer = csv.writer(in_memory_data, delimiter='|')
    with ZipFile(file) as fo: 
        target_file_member = return_target_member(fo)
        with fo.open(target_file_member, mode='r') as file_contents:
            reader = csv.reader(TextIOWrapper(file_contents, 'utf-8'))
            writer.writerows(reader)
    return in_memory_data.getvalue()

def clean_data_and_columns(in_memory_data: str) -> tuple:
    split_data = in_memory_data.split('\n')
    columns = [re.sub('(\s{1})','_', x.strip()).split('|') for x in split_data[:1]][0]
    data = [re.sub('\r', '', x).split('|') for x in split_data[1:]]
    return data, columns