import zipfile
import os

# Function to screen uploaded files (.ttl and .csv)
def screen_uploaded_files(file_names):
    ttl_files = [f for f in file_names if f.endswith('.ttl')]
    csv_files = [f for f in file_names if f.endswith('.csv')]

    # Screening logic for valid file combinations
    if (len(ttl_files) == 1 and len(csv_files) <= 1) or (len(ttl_files) == 2 and len(csv_files) <= 1):
        return True, f"Uploaded files: {', '.join(file_names)}"
    else:
        return False, "Error: Please upload 1 or 2 .ttl files and at most 1 .csv file."

# Function to screen zip file for .pkl files
def screen_zip_file(zip_file_path):
    if not os.path.exists(zip_file_path):
        return False, f"Error: File not found at {zip_file_path}."

    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            pkl_files = [f for f in zip_ref.namelist() if f.endswith('.pkl')]

        if not pkl_files:
            return False, "Error: No .pkl files found."
        else:
            return True, f"{len(pkl_files)} .pkl file(s) found."
    except Exception as e:
        return False, f"Error: Could not read zip file. {str(e)}"
