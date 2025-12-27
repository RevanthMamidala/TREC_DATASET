import geopandas as gpd
import pandas as pd
import os
import sys
from io import StringIO

# === Usage ===
if len(sys.argv) != 5:
    print("Usage: python Add_datafrom_outputs_to_shp.py <txt_file> <variable> <output_filename.shp>")
    sys.exit(1)

txt_path = sys.argv[1]
variable = sys.argv[2]
output_name = sys.argv[3]
project_dir = sys.argv[4] 

# === Step 1: Find most recent shapefile from outputs ===
outputs_dir = os.path.join(project_dir, "outputs")
shapefile_path = os.path.join(outputs_dir, f"{os.path.splitext(output_name)[0]}.shp")

shapefiles = [f for f in os.listdir(outputs_dir) if f.endswith(".shp")]
if not shapefiles:
    print(f"❌ No shapefiles found in {outputs_dir}")
    sys.exit(1)

# Assume latest file by modified time
shapefiles.sort(key=lambda f: os.path.getmtime(os.path.join(outputs_dir, f)), reverse=True)
shapefile_path = os.path.join(outputs_dir, shapefiles[0])
print(f"📂 Using shapefile: {shapefile_path}")

gdf = gpd.read_file(shapefile_path)
gdf['HRU_ID'] = gdf['HRU_ID'].astype(int)

# === Step 2: Read water balance text file ===
with open(txt_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

columns = lines[1].strip().split()
data_lines = lines[3:]
df = pd.read_csv(StringIO(''.join(data_lines)), sep=r'\s+', engine='python', names=columns)
df['gis_id'] = df['gis_id'].astype(int)

# === Step 3: Merge specified variable ===
if variable not in df.columns:
    print(f"❌ Variable '{variable}' not found in water balance file.")
    sys.exit(1)

merged = gdf.merge(df[['gis_id', variable]], left_on='HRU_ID', right_on='gis_id', how='left')
merged = merged.drop(columns=['gis_id'])

# === Step 4: Save updated shapefile ===
output_path = os.path.join(outputs_dir, output_name)
merged.to_file(output_path)
print(f"✅ Added column '{variable}' and saved to: {output_path}")
