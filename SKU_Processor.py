import pandas as pd
from tqdm import tqdm
from collections import defaultdict

csv_file_path = input("Enter the path to the CSV file: ") or 'input.csv'

# Load the CSV file
df = pd.read_csv(csv_file_path)

# Enable progress bar for the 'apply' operation
tqdm.pandas()

# Define the characteristic categories in the desired order
priority_order = [
    'Farbe', 'Größe' # and so on...
]

# Function to reorder characteristics and handle multiple values for each SKU
def reorder_and_handle_multiple(group):
    characteristic_dict = defaultdict(list)

    # Process each 'Combined' value, split by semicolon, and store in the dictionary
    for entry in group['Combined']:
        if ';' in entry:
            key, value = entry.split(';', 1)  # Split by the first semicolon (only once)
            characteristic_dict[key.strip()].append(value.strip())  # Strip spaces and append the value to the list

    # Create a new dictionary following the order from 'priority_order'
    reordered_combined = {}
    for characteristic in priority_order:
        if characteristic in characteristic_dict:
            # If multiple values, create multiple columns for each characteristic
            for idx, value in enumerate(characteristic_dict[characteristic], start=1):
                reordered_combined[f"{characteristic}_{idx}"] = value
        else:
            # If the characteristic is missing, add a placeholder value
            reordered_combined[f"{characteristic}_1"] = '000'

    return pd.Series(reordered_combined)

# Group by the 'SKU' column
df_grouped = df.groupby(['SKU']).agg({'Combined': list}).reset_index()

# Apply the function to handle multiple characteristics and reorder them into columns
expanded_df = df_grouped.progress_apply(reorder_and_handle_multiple, axis=1)

# Concatenate the original grouped data with the expanded characteristics columns
final_df = pd.concat([df_grouped[['SKU']], expanded_df], axis=1)

# Save the resulting DataFrame to a new CSV file
final_df.to_csv('output.csv', index=False, encoding='utf-8')

print("File saved as output.csv")