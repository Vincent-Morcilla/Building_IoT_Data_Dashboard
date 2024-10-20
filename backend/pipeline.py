import time
import os
import base64
import zipfile

# Define the pipeline progress tracker
class PipelineProgress:
    def __init__(self):
        self.stage = "Not Started"
        self.percentage = 0
        self.analytics = None  # Store analytics here
        self.error = None  # Store error messages

    def update_progress(self, stage, percentage):
        self.stage = stage
        self.percentage = percentage

    def get_progress(self):
        return {"stage": self.stage, "percentage": self.percentage, "error": self.error}

    def set_analytics(self, analytics):
        self.analytics = analytics

    def get_analytics(self):
        return self.analytics

    def set_error(self, error_message):
        self.error = error_message

    def get_error(self):
        return self.error

    def reset_progress(self):
        self.__init__()

# Single instance of PipelineProgress
pipeline_progress = PipelineProgress()

def get_pipeline_progress():
    return pipeline_progress

# Data ingestion step
def data_ingestion(uploaded_files, zip_file_path):
    ttl_files = []
    csv_file = None
    zip_file_contents = []

    # Process uploaded files
    for file_data in uploaded_files:
        filename = file_data['filename']
        content_type, content_string = file_data['content'].split(',')
        decoded = base64.b64decode(content_string)

        if filename.endswith('.ttl'):
            # Process .ttl file
            print(f"Ingesting TTL file: {filename}")
            ttl_files.append({'filename': filename, 'content': decoded})
        elif filename.endswith('.csv'):
            # Process .csv file
            print(f"Ingesting CSV file: {filename}")
            csv_file = {'filename': filename, 'content': decoded}

    # Process the zip file and extract contents
    if zip_file_path:
        print(f"Processing Zip File: {zip_file_path}")
        if os.path.exists(zip_file_path):
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_file_contents = zip_ref.namelist()  # List of files in the zip
                # Modify filenames or process them as needed here
        else:
            raise FileNotFoundError(f"Zip file not found at {zip_file_path}")

    # Return the base names for display
    file_names = [f['filename'] for f in ttl_files]
    if csv_file:
        file_names.append(csv_file['filename'])
    if zip_file_contents:
        file_names.extend(zip_file_contents)
    return file_names

# Data processing step (modify filenames)
def process_files(file_names, dataset_name):
    # Simulate processing delay
    time.sleep(2)
    modified_file_names = [f"{dataset_name}_{name}_processed" for name in file_names]
    print("File Processing Completed")
    return modified_file_names

# Data cleaning step
def data_cleaning():
    time.sleep(2)  # Simulate data cleaning delay
    print("Data Cleaning Completed")

# Exploratory Data Analysis (EDA) step
def eda():
    time.sleep(2)  # Simulate EDA delay
    print("EDA Completed")

# Define the complete pipeline
def data_pipeline(data):
    pipeline_progress.reset_progress()  # Reset progress for new pipeline run
    try:
        # Extract data from the dictionary
        dataset_name = data.get('dataset_name')
        uploaded_files = data.get('uploaded_files', [])
        zip_file_path = data.get('zip_file_path')

        # Store the dataset name and uploaded filenames in the progress object
        uploaded_file_names = [file['filename'] for file in uploaded_files]
        pipeline_progress.set_analytics({
            'uploaded_files': uploaded_file_names,
            'dataset_name': dataset_name
        })

        # Data Ingestion (20% completed)
        pipeline_progress.update_progress("Ingesting Data...", 20)
        file_names = data_ingestion(uploaded_files, zip_file_path)

        # Process Files (40% completed)
        pipeline_progress.update_progress("Processing Files...", 40)
        modified_file_names = process_files(file_names, dataset_name)

        # Data Cleaning and Pre-processing (60% completed)
        pipeline_progress.update_progress("Cleaning Data...", 60)
        data_cleaning()

        # Exploratory Data Analysis (80% completed)
        pipeline_progress.update_progress("Performing EDA...", 80)
        eda()

        # Completion (100% completed)
        pipeline_progress.update_progress("Complete", 100)
        # Store the modified file names in analytics along with the original uploaded filenames
        pipeline_progress.set_analytics({
            'file_names': modified_file_names,
            'uploaded_files': uploaded_file_names,
            'dataset_name': dataset_name
        })

    except Exception as e:
        # Update the progress with error
        pipeline_progress.set_error(str(e))
        # Log the error
        print(f"Pipeline encountered an error: {str(e)}")