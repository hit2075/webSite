import zipfile
import os

def list_zip_contents(zip_path):
    print(f'Contents of {zip_path}:')
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        for file in zip_ref.namelist():
            print(f'- {file}')

if __name__ == '__main__':
    zip_path = os.path.join(os.getcwd(), '2298_DESKTOP-QTCL99K_rp.zip')
    list_zip_contents(zip_path)