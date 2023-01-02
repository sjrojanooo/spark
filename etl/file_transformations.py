import os
from zipfile import ZipFile
import shutil

# locate the zip file by walking through the projects directory. 
def extract_zipfile(current_directory: str) -> str:
    for root, dir, path in os.walk(current_directory, topdown=True):
        if root.endswith('data'):
            zip_file = [os.path.join(root, file) for file in path if file.endswith('zip')][0]
            return zip_file

# zip file object hold files that are called members, we want to locate csv file inside of the compressed file 
# without unzipping. 
def return_target_member(zo: object) -> str: 
    file_member = [file for file in zo.namelist() if (file.startswith('archive') and file.endswith('.csv'))][0]
    return file_member

# extracting the target file into the its own directory and return the target csv file located inside the zipped file
def extract_and_return_target_file(file: str) -> str:
    with ZipFile(file) as fo: 
        target_file_member = return_target_member(fo)
        fo.extract(target_file_member, './data')
    return target_file_member

# copy over the file into a new directory, rename the file and remove the extracted archive directory
def move_and_rename_file(target_file_member: str) -> None:
    csv_file = os.path.basename(target_file_member)
    if not os.path.exists('./data/sales'):
        os.mkdir('./data/sales')
        shutil.move(f'.data/{target_file_member}', './data/sales')
        os.rename(f'./data/sales/{csv_file}', 'adidas_us_retail_sales_data.csv') 
    shutil.rmtree('./data/archive')