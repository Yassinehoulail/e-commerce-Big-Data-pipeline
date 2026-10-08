import zipfile
import os

zip_path = 'c:/Users/YASSINE/Desktop/ecommerce-bigdata.zip'
project_path = 'c:/Users/YASSINE/CascadeProjects/ecommerce-bigdata'

z = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED)
count = 0
skipped = 0

for root, dirs, files in os.walk(project_path):
    for f in files:
        file_path = os.path.join(root, f)
        arcname = os.path.relpath(file_path, project_path)
        try:
            z.write(file_path, arcname)
            count += 1
        except (OSError, PermissionError) as e:
            print(f"Skipping {file_path}: {e}")
            skipped += 1

z.close()
print(f'Zip created. Files added: {count}, Skipped (permission denied): {skipped}')
