import os
import urllib.request

# Directory in Unity Catalog Volume to store files
dest_dir = "/Volumes/workspace/default/scada_3w_sample/3w_parquet/sample"
os.makedirs(dest_dir, exist_ok=True)

# Direct URLs to sample 3W Parquet files from GitHub
sample_files = [
    "https://github.com/petrobras/3W/raw/refs/heads/main/dataset/0/WELL-00001_20170201010207.parquet",
    "https://github.com/petrobras/3W/raw/refs/heads/main/dataset/3/WELL-00014_20170917140000.parquet"
]

for url in sample_files:
    filename = url.split("/")[-1]
    local_path = os.path.join(dest_dir, filename)
    print(f"Downloading {filename}...")
    try:
        urllib.request.urlretrieve(url, local_path)
        print(f"Saved to {local_path} ({os.path.getsize(local_path)} bytes)")
    except Exception as e:
        print(f"Download failed: {e}")

print("Done! Files available in DBFS.")