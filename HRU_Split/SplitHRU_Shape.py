import sys
import os
import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import box, Polygon, MultiPolygon
from joblib import Parallel, delayed
import re
import multiprocessing

# === ARGUMENT PARSING ===
# Usage: python split_hru_polygons.py <project_folder> <output_basename>
if len(sys.argv) != 3:
    print("Usage: python split_hru_polygons.py <project_folder> <output_basename>")
    sys.exit(1)

project_folder = sys.argv[1]
output_basename = sys.argv[2]

# === Construct input paths ===
shapefile_path = os.path.join(project_folder, "Watershed", "Shapes", "hrus2.shp")
hru_txt_path   = os.path.join(project_folder, "Watershed", "Text", "HruLanduseSoilSlopeRepSwat.txt")

# === Construct output paths ===
output_dir = os.path.join(project_folder, "outputs")
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, f"{output_basename}.shp")
debug_log_path = os.path.join(output_dir, f"{output_basename}_log.txt")

# === CONFIG ===
NUM_WORKERS = max(1, int(multiprocessing.cpu_count() * 0.6))
np.random.seed(42)

print("🚀 Inputs are valid and Starting HRU Split Script...")
print(f"📂 Project folder: {project_folder}")
print(f"🗺️  Shapefile: {shapefile_path}")
print(f"📄 HRU Text: {hru_txt_path}")
print(f"💾 Output will be saved to: {output_path}")
print("")

# === STEP 1: Read HRU text file with robust parsing ===
with open(hru_txt_path, 'r', encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()

hru_data = []
collecting = False
channel = None
lsu = None
pattern = re.compile(r'^(\d+)\s*([A-Z/_0-9a-z\-]+)')

for line in lines:
    line = line.rstrip()
    if line.startswith("Channel"):
        collecting = True
        match = re.match(r"Channel\s+(\d+)\s+\(LSU\s+(\d+)\)", line)
        if match:
            channel = int(match.group(1))
            lsu = int(match.group(2))
        continue
    if collecting and line.strip():
        try:
            m = pattern.match(line)
            if not m:
                continue
            hru_id = m.group(1)
            hru_desc = m.group(2)
            remainder = line[m.end():]
            area = remainder[0:30].strip()
            pct_ws = remainder[30:55].strip()
            pct_sub = remainder[55:80].strip()
            pct_lsu = remainder[80:105].strip()
            hru_data.append({
                "Channel": channel,
                "LSU": lsu,
                "HRU_ID": hru_id,
                "HRU_Desc": hru_desc,
                "Area_ha": float(area) if area else None,
                "%Watershed": float(pct_ws) if pct_ws else None,
                "%Subbasin": float(pct_sub) if pct_sub else None,
                "%LSU": float(pct_lsu) if pct_lsu else None
            })
        except:
            continue

df_hrus = pd.DataFrame(hru_data).dropna(subset=["HRU_ID", "Area_ha"])

# === STEP 2: Recursive polygon slicing ===
def recursive_split(polygon, area_list_ha, orientation='vertical'):
    if len(area_list_ha) == 1:
        return [polygon]
    total_area = polygon.area / 10000  # m² to ha
    target_area = area_list_ha[0]
    fraction = target_area / total_area
    minx, miny, maxx, maxy = polygon.bounds

    if orientation == 'vertical':
        split_x = minx + (maxx - minx) * fraction
        band1 = box(minx, miny, split_x, maxy)
        band2 = box(split_x, miny, maxx, maxy)
    else:
        split_y = miny + (maxy - miny) * fraction
        band1 = box(minx, miny, maxx, split_y)
        band2 = box(minx, split_y, maxx, maxy)

    try:
        part1 = polygon.intersection(band1)
        part2 = polygon.intersection(band2)
        return [part1] + recursive_split(part2, area_list_ha[1:], orientation)
    except:
        return []

# === STEP 3: Process row and return features + debug log ===
def process_row(row, df_hrus):
    debug_line = None
    try:
        base_geom = row.geometry
        hru_ids = [x.strip() for x in str(row['HRUS']).split(',')]
        matched = df_hrus[df_hrus['HRU_ID'].isin(hru_ids)].copy()
        if matched.empty:
            return [], f"Polygon_ID: {row.name} — ❌ No HRUs matched"

        area_list_ha = matched["Area_ha"].astype(float).tolist()
        total_requested_ha = sum(area_list_ha)
        if total_requested_ha == 0:
            return [], f"Polygon_ID: {row.name} — ❌ Total Area_ha is zero"

        ratios = [a / total_requested_ha for a in area_list_ha]
        geom_ha = base_geom.area / 10000
        allocated_areas = [r * geom_ha for r in ratios]

        debug_line = (
            f"Polygon_ID: {row.name}, HRUS: {hru_ids}, "
            f"geom_ha: {geom_ha:.4f}, total_ha: {total_requested_ha:.4f}, "
            f"ratios: {[round(r, 4) for r in ratios]}, "
            f"allocated: {[round(a, 4) for a in allocated_areas]}"
        )

        # Try vertical split
        sub_geoms = recursive_split(base_geom, allocated_areas, orientation='vertical')
        if len(sub_geoms) != len(matched):
            sub_geoms = recursive_split(base_geom, allocated_areas, orientation='horizontal')
        if len(sub_geoms) != len(matched):
            hru_row = matched.iloc[0]
            combined_attr = row.drop(['geometry', 'HRUS']).to_dict()
            for col in hru_row.index:
                combined_attr[col] = hru_row[col]
            combined_attr['geometry'] = base_geom
            combined_attr['SliceMeth'] = 'fallback'
            combined_attr['geom_ha'] = geom_ha
            combined_attr['total_ha'] = total_requested_ha
            return [combined_attr], debug_line + " — ⚠️ Used fallback"

        features = []
        for geom, (_, hru_row) in zip(sub_geoms, matched.iterrows()):
            if geom.is_empty or not isinstance(geom, (Polygon, MultiPolygon)):
                continue
            combined_attr = row.drop(['geometry', 'HRUS']).to_dict()
            for col in hru_row.index:
                combined_attr[col] = hru_row[col]
            combined_attr['geometry'] = geom
            combined_attr['SliceMeth'] = 'recursive'
            combined_attr['geom_ha'] = geom_ha
            combined_attr['total_ha'] = total_requested_ha
            features.append(combined_attr)

        return features, debug_line

    except Exception as e:
        return [], f"Polygon_ID: {row.name} — ❌ Exception: {str(e)}"

# === STEP 4: Load shapefile and run in parallel ===
gdf = gpd.read_file(shapefile_path)
gdf = gdf[gdf['HRUS'].notna()].copy()

print(f"🔄 Processing {len(gdf)} polygons using {NUM_WORKERS} workers...")

results = Parallel(n_jobs=NUM_WORKERS)(
    delayed(process_row)(row, df_hrus) for _, row in gdf.iterrows()
)

print("✅ Finished parallel processing.")

# Separate output and debug lines
flattened = []
debug_lines = []
for features, debug in results:
    flattened.extend(features)
    if debug:
        debug_lines.append(debug)

# === STEP 5: Save GeoDataFrame ===
split_gdf = gpd.GeoDataFrame(flattened, crs=gdf.crs)
split_gdf.rename(columns=lambda x: x[:10], inplace=True)
split_gdf.to_file(output_path)

##=== STEP 6: Save debug log to file ===
# with open(debug_log_path, "w", encoding="utf-8") as f:
#     for line in debug_lines:
#         f.write(line + "\n")


print(f"✅ Done! Saved {len(split_gdf)} features to shapefile.")
print(f"📝 Debug log written to: {debug_log_path}")
