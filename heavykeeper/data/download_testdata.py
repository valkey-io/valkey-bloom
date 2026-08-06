import requests
import os
from tqdm import tqdm  # For progress bar

def download_file(file_num):
    base_url = "https://raw.githubusercontent.com/ZeBraHack0/DHS/main/data"
    filename = f"{file_num}.dat"
    url = f"{base_url}/{filename}"
    
    try:
        # Send GET request with stream=True for progress bar
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        # Get file size for progress bar
        total_size = int(response.headers.get('content-length', 0))
        
        # Save file with progress bar
        output_path = os.path.join(".", filename)
        with open(output_path, "wb") as f, tqdm(
            desc=filename,
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as pbar:
            for data in response.iter_content(chunk_size=1024):
                size = f.write(data)
                pbar.update(size)
            
        print(f"Successfully downloaded {filename}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {filename}: {e}")

def main():
    # Download files 0.dat through 10.dat
    print("Downloading CAIDA trace files...")
    for file_num in range(11):  
        download_file(file_num)
    print("\nAll files downloaded!")

if __name__ == "__main__":
    main()
