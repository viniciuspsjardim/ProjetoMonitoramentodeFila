import pandas as pd
import os

# Define paths
base_dir = "./data_local/gold/predictions"
output_csv = "./data_local/predictions_preview.csv"
output_md = "./data_local/predictions_preview.md"

# Find the parquet file (UUID name)
files = [f for f in os.listdir(base_dir) if f.endswith('.parquet')]
if not files:
    print("No predictions found.")
    exit(1)

# Read Parquet
df = pd.read_parquet(os.path.join(base_dir, files[0]))

# Format
df['prediction_ts'] = df['prediction_ts'].astype(str)
df = df.round(2)

# Export to CSV
df.to_csv(output_csv, index=False)
print(f"Saved CSV to {output_csv}")

# Export to Markdown for Artifact
with open(output_md, "w") as f:
    f.write("# Prediction Results Preview\n\n")
    f.write(df.to_markdown(index=False))
print(f"Saved MD to {output_md}")
