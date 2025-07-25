# EndTimeCorrection.py

import pandas as pd
import os
import sys

def normalize_service(val):
    if pd.isnull(val):
        return val
    val = val.split(":")[-1]  # Remove prefix like 'SP3:'
    replacements = {
        'cxservice-core': 'cxservice',
        'risk-cloud': 'risk',
        'saas-batch': 'batch',
        'saas-messaging': 'messaging',
        'rwdinfra': 'apps-infra',
        'fusion-common': 'setup',
        'procurement-core': 'procurement',
        'scm-core': 'scm'
    }
    return replacements.get(val, val)

def convert_seconds_to_hms(seconds):
    try:
        seconds = float(seconds)
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        return f"{hours:02}:{minutes:02}:{secs:02}"
    except:
        return "00:00:00"

def main(end_time_file, start_time_file):
    allowed_spans = [
        "erp", "sales-common", "messaging", "risk", "rwdtools", "batch", "student-management",
        "service-health", "hr-core", "apps-infra", "knowledge-management", "boss",
        "cxservice", "fusion-ai", "field-service-common", "procurement", "authz", "setup", "scm",
        "ui-infrastructure"
    ]

    # Load and clean End Time file
    df = pd.read_csv(end_time_file, sep=None, engine="python", header=1)
    df.columns = df.columns.str.strip()
    df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
    df = df.dropna(subset=['Value'])
    df = df[df['namespace'].isin(allowed_spans)]
    max_df = df.groupby(['namespace', 'prd_env'])['Value'].max().reset_index()
    max_df['End Time'] = pd.to_datetime(max_df['Value'], unit='s')
    df1 = max_df[['namespace', 'prd_env', 'End Time']]

    # Load and clean Start Time file
    df2 = pd.read_csv(start_time_file)
    df2["SERVICE_INSTANCE_TYPE"] = df2["SERVICE_INSTANCE_TYPE"].apply(normalize_service)
    df2.rename(columns={"SPECTRA_START_TIME(spectra platform)": "SERVICE_START_TIME"}, inplace=True)
    df1["End Time"] = pd.to_datetime(df1["End Time"], errors='coerce')
    df2["SERVICE_START_TIME"] = pd.to_datetime(df2["SERVICE_START_TIME"], errors='coerce')
    df2["POD"] = df2["POD"].str.lower()

    # Merge
    merged = pd.merge(
        df1,
        df2,
        how='inner',
        left_on=["namespace", "prd_env"],
        right_on=["SERVICE_INSTANCE_TYPE", "POD"]
    )

    # Calculate durations
    merged["Duration (seconds)"] = (merged["End Time"] - merged["SERVICE_START_TIME"]).dt.total_seconds()
    merged["Duration"] = merged["Duration (seconds)"].apply(convert_seconds_to_hms)

    final_df = merged[[
        "namespace",
        "prd_env",
        "SERVICE_START_TIME",
        "End Time",
        "Duration"
    ]]

    file_root, file_ext = os.path.splitext(end_time_file)
    output_file = f"{file_root}_CorrectedEndTime{file_ext}"
    final_df.to_csv(output_file, index=False)
    print(f"Saved merged output to: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python EndTimeCorrection.py <CorrectEndTimeFileName.csv> <StartTimeFileName.csv>")
        sys.exit(1)

    end_time_file = sys.argv[1]
    start_time_file = sys.argv[2]
    main(end_time_file, start_time_file)
