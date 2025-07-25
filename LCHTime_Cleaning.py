import pandas as pd
import os
import sys

# === Helper: Convert seconds to HH:MM:SS ===
def convert_seconds_to_hms(seconds):
    try:
        seconds = float(seconds)
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02}:{minutes:02}:{secs:02}"
    except:
        return "00:00:00"

def main(input_file_1, input_file_2):
    # === Output File Name ===
    file_root, file_ext = os.path.splitext(input_file_1)
    output_file = f"{file_root}_merged_cleaned{file_ext}"

    # === Step 1: Load and Clean First File ===
    df1 = pd.read_csv(input_file_1)
    df1 = df1[['environment', 'lifecycle_hook_name', 'workload_deployment', 'Value']]
    df1 = df1.drop_duplicates(subset=['environment', 'lifecycle_hook_name', 'workload_deployment'], keep='first')
    df1['Duration'] = df1['Value'].apply(convert_seconds_to_hms)
    df1 = df1.drop(columns=['Value'])  # drop original Value column

    # === Step 2: Load and Clean Second File ===
    df2 = pd.read_csv(input_file_2)
    df2 = df2[['environment', 'lifecycle_hook_name', 'workload_deployment', 'Value']]
    df2 = df2.drop_duplicates(subset=['environment', 'lifecycle_hook_name', 'workload_deployment'], keep='first')

    # Convert 'Value' in second file to integer Count
    df2['Count'] = pd.to_numeric(df2['Value'], errors='coerce').fillna(0).astype(int)
    df2 = df2.drop(columns=['Value'])

    # === Step 3: Merge on 3 keys ===
    merged = pd.merge(
        df1,
        df2,
        on=['environment', 'lifecycle_hook_name', 'workload_deployment'],
        how='inner'
    )

    # === Step 4: Save the final result ===
    merged.to_csv(output_file, index=False)
    print(f"Merged & cleaned CSV saved as: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python merge_duration_count.py <PHX_LCH_Duration.csv> <PHX_LCH_Count.csv>")
    else:
        main(sys.argv[1], sys.argv[2])
