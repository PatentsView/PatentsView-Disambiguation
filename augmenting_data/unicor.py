import pandas as pd
import os


def consolidate_unicor_data(csv_directory, csv_files):
    # Initialize an empty DataFrame to store the consolidated data
    consolidated_df = pd.DataFrame()
    # Iterate through each CSV file and append its contents to the consolidated DataFrame
    for csv_file in csv_files:
        file_path = os.path.join(csv_directory, csv_file)
        df = pd.read_csv(file_path)
        df["correction"] = csv_file.replace(".csv", "")
        consolidated_df = pd.concat([consolidated_df, df], axis=0)
    print(consolidated_df.head())
    print(consolidated_df.shape)
    consolidated_df.to_csv("combined_unicor_data.csv")
    breakpoint()

if __name__ == '__main__':
    # Specify the directory where your CSV files are located
    csv_directory = '/Users/bcard/Downloads/Unicor'
    # Get a list of all CSV files in the specified directory
    csv_files = [file for file in os.listdir(csv_directory) if file.endswith('.csv')]
    consolidate_unicor_data(csv_directory, csv_files)

