import os
import json
import pandas as pd
import logging
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Set the folder path
folder_path = r"C:\Users\Kokoabassplayer\Downloads\all_rows-20241008T131045Z-001\all_rows"

logging.info(f"Processing files in folder: {folder_path}")

# Initialize an empty list to store all data
all_data = []

# Iterate through all files in the folder
json_files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
logging.info(f"Found {len(json_files)} JSON files")

for filename in tqdm(json_files, desc="Processing JSON files"):
    file_path = os.path.join(folder_path, filename)
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Load JSON data from each file
            json_data = json.load(file)
            # Append the data to the list
            if isinstance(json_data, list):
                all_data.extend(json_data)
            else:
                all_data.append(json_data)
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON in file: {filename}")
    except Exception as e:
        logging.error(f"Error processing file {filename}: {str(e)}")

logging.info(f"Total records processed: {len(all_data)}")

if not all_data:
    logging.error("No data was processed. Check if JSON files are empty or if there were errors reading them.")
else:
    # Convert the combined data to a DataFrame
    df = pd.DataFrame(all_data)
    logging.info(f"Created DataFrame with shape: {df.shape}")

    # Define the output CSV file path
    output_csv = os.path.join(folder_path, "combined_output.csv")

    # Save the DataFrame to a CSV file
    try:
        df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        file_size = os.path.getsize(output_csv)
        logging.info(f"All JSON files have been combined and saved to {output_csv}")
        logging.info(f"Output file size: {file_size} bytes")
        
        if file_size == 0:
            logging.error("Output file is empty. Check if DataFrame was empty before saving.")
    except Exception as e:
        logging.error(f"Error saving CSV file: {str(e)}")

print("Script execution completed. Please check the logs for details.")
