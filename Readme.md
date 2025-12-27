# Tile and Rotation Enhanced Dataset

## GENERAL INFORMATION

### DATA TITLE: Tile Drainage and Rotation-Enhanced Cropland Data layer 

### PROJECT TITLE: *Enhancing Agricultural Impact Assessment with Tile Drainage and Rotation-Enhanced Cropland Data layer* 
---
### DATA ABSTRACT:

The Tile and Rotation Enhanced Cropland (TREC) dataset provides a high-resolution (30 m) spatial representation of land use in the Conterminous United States (CONUS), specifically designed to improve hydrological modeling in agricultural systems by enhanced spatial accuracy. This dataset consists of a single-band tiff image (`Tile_Rotation_Enhanced_CDL2017.tif`) with values ranging from 1 to 1522, representing various land use classes and tile drainage conditions as mapped in the csv file (`TREC_Datset_Code_Names.csv`). This dataset contains spatially explicit (pixel-wise) data on croplands, categorized by tile drainage status (tiled vs. non-tiled) and cropping system (continuous vs. discontinuous rotation). The TREC dataset is built upon the 2017 Cropland Data Layer (CDL); therefore, the values for non-tiled and discontinuous croplands in the TREC dataset developed in this study align with those in the 2017 CDL. This forms the basis for naming the output file as `Tile_Rotation_Enhanced_CDL2017.tif` (TREC2017). The TREC dataset supports reliable hydrologic modeling, enabling informed decision-making for watershed conservation, nutrient management, and agricultural policy development. This dataset is a vital resource for researchers and policymakers focused on sustainable agricultural practices and water resource management.

---

### AUTHORS

- **Revanth Mamidala**  
  ORCID: [https://orcid.org/0000-0002-1854-2495](https://orcid.org/0000-0002-1854-2495)  
  Institution: Iowa State University  
  Email: [mrevanth@iastate.edu](mailto:mrevanth@iastate.edu)

- **Lu Liu** *(Corresponding Author)*  
  ORCID: [https://orcid.org/0000-0002-4939-5432](https://orcid.org/0000-0002-4939-5432)  
  Institution: Iowa State University  
  Email: [luliu@iastate.edu](mailto:luliu@iastate.edu)

---
### ASSOCIATED PUBLICATIONS

Enhancing Agricultural Impact Assessment with Tile Drainage and Rotation-Enhanced Cropland Data Layer: Application in Uncalibrated SWAT+ Simulations

---

### COLLECTION INFORMATION

**Time Period:**  
The dataset integrates information from multiple sources:
- **Crop Data Layer (CDL):** Records from **2008 to 2024**, with the **2017 CDL** used as the base layer for classification.
- **Tile Drainage Data:** Derived from the **2017 USDA Agricultural Census**.

**Spatial Extent:**  
Conterminous United States (**CONUS**)

---

## FILE DIRECTORY

```
├── GEE Script and Input Data
│   ├── Google Earth Engine JavaScript
│   └── Input Data - GEE Assets
│       ├── 2017_30m_cdls
│       └── Crop_Frequency_2008-2024
├── TREC Dataset
├── Lookup Tables and SWAT Plant Database
└── HRU_Split
```

### Folders and files

1. **GEE Script and Input Data/**<br>
	This folder contains scripts and input datasets used for processing in Google Earth Engine (GEE).
   - `Google Earth Engine JavaScript/TREC2017_20250519.js`: Main GEE script used for generating the TREC dataset.<br>
   		See "METHODS AND MATERIALS - Steps to Generate TREC Dataset" for detailed instructions.
   - `Input Data - GEE Assets/`: <br>
	   Raster files downloaded from USDA NASS Research and Science: 
			`https://www.nass.usda.gov/Research_and_Science/Cropland/Release/index.php`
	   - `2017_30m_cdls/`: The Cropland Data Layer (CDL) dataset, base layer used for classification (GeoTIFF format with metadata)."2017 National CDL (1.7 GB)"
	   - `Crop_Frequency_2008-2024/`: Raster files showing crop frequency layers for corn, soybeans, wheat, and cotton (2008–2024).  "2024 National Frequency Layer 30-meter (2.0 GB)"
   
2. **TREC Dataset/**<br>
   This folder contains the output raster generated from GEE and a style file.
   - Raster file 
	   - `Tile_Rotation_Enhanced_CDL2017.tif`: Raster file showing classified land use with tile rotation separation.
	   - Associated metadata and auxiliary files (`.tfw`, `.aux.xml`, `.vat.dbf`, etc.) for GIS compatibility.
   - Layer file
   		- `Tile_Rotation_Enhanced_CDL_2017.lyrx`: ESRI layer file containing TREC land use raster with **symbology**.

3. **Lookup Tables and SWAT Plant Database/**<br>
   This folder contains supporting files for mapping CDL codes and linking SWAT+ plant parameters.
   - `Landuse_lookup_SWAT.csv`: Lookup table linking land use codes to SWAT+ inputs used in the study.
   - `plant_Database_SWAT.csv`: Crop database used in the study.
   - `TREC_Datset_Code_Names.csv`: Lookup table mapping crop type and modified land use code.

4. **HRU_Split/**<br>
   This folder contains scripts and tools for splitting the **Hydrologic Response Units** (HRUs) for visualization purposes and linking SWAT+ model outputs with shapefiles.
   - `HRUSplit.bat`: Batch file to automate HRU splitting workflow.
   - `SplitHRU_Shape.py`: Python script for splitting HRU polygons based on percentage allocation.
   - `Add_datafrom_outputs_to_shp.py`: Python script for adding SWAT+ output variables (e.g., tile flow) to the split HRU shapefile.

---

## CODEBOOK

**Number of Variables/Columns:**  
1 (Single-band raster with categorical integer codes)

**Number of Cases/Rows (Pixels):**  
- **Columns:** 153,812  
- **Rows:** 96,524  
- **Total Pixels:** 14,859,430,688 (≈ 14.86 billion pixels at 30 m resolution)  
- **Pixel Size:** 30 m × 30 m (900 m²)

**Missing Data Codes:**  
- `65535` — Represents NoData / background (value ignored in statistics)

**Pixel Data Type:**  
- Unsigned 16-bit integer (`unsigned short`)

**Valid Range of Values:**  
- **Minimum:** 1  
- **Maximum:** 1522  
- **Mean:** 93.11  
- **Standard Deviation:** 144.31  

**Coordinate System:**  
- **Projected Coordinate System:** NAD 1983 Contiguous USA Albers (EPSG: 5070)  
  - Linear Unit: Meters  
  - Central Meridian: -96.0  
  - Standard Parallels: 29.5, 45.5  
  - Latitude of Origin: 23.0  
- **Geographic Coordinate System:** NAD 1983 (EPSG: 4269)  
  - Angular Unit: Degree  
  - Prime Meridian: Greenwich  
  - Datum: D North American 1983

**Extent:**  
- **Top:** 3,172,620.000000 m  
- **Bottom:** 276,900.000000 m  
- **Left:** -2,356,110.000000 m  
- **Right:** 2,258,250.000000 m

### Columns in the `TREC_Datset_Code_Names.csv`

| Column           | Description                                           | Allowed format|
|------------------|-------------------------------------------------------|---------------|
| Codes            | Pixel Value in the TREC Raster                        |    Integer    |
| Class_Names      | Land Use Class associated with respective Pixel Value |     Text      |

### Columns in the `plant_database_SWAT.csv`

| Column Name   | Description                                                                            | Allowed values or format |
| ------------- | -------------------------------------------------------------------------------------- | ----------- |
| id            | Unique identifier for each plant entry                                                 | Integer     |
| name          | Short name or code used to identify the plant                                          | Text        |
| plnt\_typ     | Type of plant based on life cycle or seasonality (e.g., perennial, cold\_annual)       | Text        |
| gro\_trig     | Trigger that initiates plant growth (e.g., temperature-based)                          | Text        |
| nfix\_co      | Nitrogen fixation coefficient (0–1) indicating how much N the plant fixes biologically | Float       |
| days\_mat     | Number of days until maturity for annual plants                                        | Integer     |
| bm\_e         | Plant biomass-energy ratio (kg/ha)                                                     | Float       |
| harv\_idx     | Harvest index, ratio of economic yield to total above-ground biomass                   | Float       |
| lai\_pot      | Potential leaf area index (LAI) under optimal conditions                               | Float       |
| frac\_hu1     | Fraction of heat units when LAI reaches first max                                      | Float       |
| lai\_max1     | Maximum LAI at the first growth phase                                                  | Float       |
| frac\_hu2     | Fraction of heat units when LAI reaches second max                                     | Float       |
| lai\_max2     | Maximum LAI at the second growth phase                                                 | Float       |
| hu\_lai\_decl | Heat units at which LAI starts to decline                                              | Float       |
| dlai\_rate    | Rate of decline of LAI (per day)                                                       | Float       |
| can\_ht\_max  | Maximum canopy height (m)                                                              | Float       |
| rt\_dp\_max   | Maximum rooting depth (m)                                                              | Float       |
| tmp\_opt      | Optimum temperature for plant growth (°C)                                              | Float       |
| tmp\_base     | Base temperature for plant growth (°C)                                                 | Float       |
| frac\_n\_yld  | Fraction of plant nitrogen content in yield                                            | Float       |
| frac\_p\_yld  | Fraction of plant phosphorus content in yield                                          | Float       |
| frac\_n\_em   | Fraction of total nitrogen uptake at emergence                                         | Float       |
| frac\_n\_50   | Fraction of nitrogen uptake at 50% maturity                                            | Float       |
| frac\_n\_mat  | Fraction of nitrogen uptake at full maturity                                           | Float       |
| frac\_p\_em   | Fraction of total phosphorus uptake at emergence                                       | Float       |
| frac\_p\_50   | Fraction of phosphorus uptake at 50% maturity                                          | Float       |
| frac\_p\_mat  | Fraction of phosphorus uptake at full maturity                                         | Float       |
| harv\_idx\_ws | Harvest index during water stress conditions                                           | Float       |
| usle\_c\_min  | Minimum USLE C-factor (soil erosion factor)                                            | Float       |
| stcon\_max    | Maximum stomatal conductance                                                           | Float       |
| vpd           | Vapor pressure deficit threshold (kPa)                                                 | Float       |
| frac\_stcon   | Fraction of stomatal conductance used for transpiration                                | Float       |
| ru\_vpd       | Relative uptake as function of vapor pressure deficit                                  | Float       |
| co2\_hi       | CO₂ enrichment harvest index modifier                                                  | Float       |
| bm\_e\_hi     | CO₂ enrichment biomass-energy modifier                                                 | Float       |
| plnt\_decomp  | Decomposition rate of plant residue (fraction/day)                                     | Float       |
| lai\_min      | Minimum LAI for active growth                                                          | Float       |
| bm\_tree\_acc | Annual biomass accumulation for trees (kg/ha)                                          | Float       |
| yrs\_mat      | Years to maturity for perennials/trees                                                 | Integer     |
| bm\_tree\_max | Maximum biomass of tree species (kg/ha)                                                | Float       |
| ext\_co       | Extinction coefficient for solar radiation                                             | Float       |
| leaf\_tov\_mn | Minimum day of year for leaf turnover                                                  | Integer     |
| leaf\_tov\_mx | Maximum day of year for leaf turnover                                                  | Integer     |
| bm\_dieoff    | Fraction of biomass lost during die-off                                                | Float       |
| rt\_st\_beg   | Fraction of root mass at beginning of season                                           | Float       |
| rt\_st\_end   | Fraction of root mass at end of season                                                 | Float       |
| plnt\_pop1    | Plant population for first growth phase (plants/m²)                                    | Integer     |
| frac\_lai1    | Fraction of LAI at first population                                                    | Float       |
| plnt\_pop2    | Plant population for second growth phase (plants/m²)                                   | Integer     |
| frac\_lai2    | Fraction of LAI at second population                                                   | Float       |
| frac\_sw\_gro | Fraction of soil water required for growth                                             | Float       |
| aeration      | Soil aeration threshold for plant growth                                               | Float       |
| rsd\_pctcov   | Percent cover by residue                                                               | Float       |
| rsd\_covfac   | Residue cover factor for erosion control                                               | Float       |
| description   | Label for plant type                                                                   | Text        |

## METHODS AND MATERIALS

### DATA PROCESSING METHODS 

The TREC dataset was generated using a geospatial methodology implemented through a custom JavaScript script (`TREC2017_20250519.js`) in Google Earth Engine (GEE). This dataset integrates information from the 2017 Crop Data Layer (CDL) (USDA NASS 2017), 2024 crop frequency data covering the years 2008 to 2024 (USDA NASS 2024), and tile drainage data from Ag-Tile US 2017 developed by [Valayamkunnath et al., 2020](https://doi.org/10.1038/s41597-020-00596-x). The image generated in GEE is exported as a single-band GeoTIFF file (`Tile_Rotation_Enhanced_CDL2017.tif`) with Unsigned 16-bit integer (**Uint16**) pixel values ranging from 1 to 1522, representing various land use classes and tile drainage conditions. The dataset is accompanied by a CSV file (`TREC_Datset_Code_Names.csv`) that maps each unique pixel values to their corresponding land use class and tile drainage condition.

The performance of the TREC dataset against traditional CDL dataset was validated using its application in eco-hydrologic modeling of two watersheds: Boone and Minnesota in the state of the art Soil and Water Assessment Tool (**SWAT+**) (Bieger et al., 2017a). The dataset performance was evaluated by comparing the tile drainage area in Landscape Units (LSUs) while the simulated hydrologic response of SWAT+ model is compared against observed streamflow data at various USGS measuring stations as in `Flow_cms.csv` files present in respective watershed folders. Additionally, SWAT+ split landuse option does not physically split the HRUs and hence, the python model in `HRU_Split` folder is used to split the HRUs based on the percentage of landuse in each HRU and visualize the tile drainage flow from each split HRU as saved in the shapefiles (`SWAT+ Model Folders\Outputs\HRU2_Split_Recursive_qtile.shp`). The TREC dataset was found to be more spatially accurate against randomized or averaged tile drainage area estimates applied on CDL dataset in the SWAT+ model, and it improved the hydrologic response of the model compared to the traditional CDL dataset.

**References:**
- Valayamkunnath, P., Barlage, M., Chen, F. et al. Mapping of 30-meter resolution tile-drained croplands using a geospatial modeling approach. Sci Data 7, 257 (2020). https://doi.org/10.1038/s41597-020-00596-x
- USDA National Agricultural Statistics Service Cropland Data Layer. 2017. Published crop-specific data layer [Online]. Available at http://nassgeodata.gmu.edu/CropScape/ (accessed [04/13/2025]; verified [04/13/2025]). USDA-NASS, Washington, DC.
- USDA National Agricultural Statistics Service. 2024. Crop Frequency Layer (2008–2024) [Online]. Available at https://nassgeodata.gmu.edu/CropScape/ (accessed [04/13/2025]; verified [04/13/2025]). USDA-NASS, Washington, DC.
- Bieger, K., Arnold, J. G., Rathjens, H., White, M. J., Bosch, D. D., Allen, P. M., Volk, M., & Srinivasan, R. (2017a). Introduction to SWAT+, A Completely Restructured Version of the Soil and Water Assessment Tool. Journal of the American Water Resources Association, 53(1), 115–130. https://doi.org/10.1111/1752-1688.12482

---

### Steps to Generate TREC Dataset:
**Step1:** Upload the input data files to GEE assets. <br>
**Step2:** Open the GEE Code Editor and paste the script `TREC2017_20250519.js` into the editor.<br>
**Step3:** Edit the script to the requirements of your analysis.<br>
- Replace the asset paths in the script with your uploaded files <br>
- Select the appropriate region of interest (ROI) for processing if needed or leave it as is for the entire CONUS region.<br>
- Change the thresholds for continuous crop frequency if required. <br>
- Select the appropriate output file name and export parameters in the script.<br>

**Step4:** Run the script to generate the TREC dataset.<br>
**Step5:** Append the attributes table of the resulting TREC dataset to the `TREC_Datset_Code_Names.csv` file to map pixel values to land use classes and tile drainage conditions.
 
---

### **NOTE:** GEE Tile Error - Image Size Exceeds Limit

When attempting to visualize the `Tiledrain_rotation_separated_CDL2017` image in **Google Earth Engine (GEE)**, you may encounter the following errors:

> Tile error: Number of pixels requested from Image.reproject exceeds the maximum allowed (2^31).
Tile error: Reprojection output too large (49397x37735 pixels).


These errors occur because the **ROI** is too large. The free version of GEE has a processing limit of **2³¹ pixels** for reprojection and visualization. When exceeded, GEE fails to render the image tiles.

### Recommended Solution

To avoid these errors:

1. **Use a smaller ROI** during processing to reduce pixel count.
2. **Comment out the visualization line** to avoid interactive rendering:

   ```javascript
   //Map.addLayer(Tiledrain_rotation_separated_CDL2017, visParamsFinalClipped, "Tiledrain_rotation_separated_CDL2017");
   ```
3. Export the image to Google Drive and open it in a desktop GIS platform such as: ArcGIS Pro and QGIS.

---

### SOFTWARE

**Name:** Google Earth Engine<br>
**System Requirements:** A modern web browser (Chrome, Firefox, Edge, Safari) and a Google account.  
**Version:** Web-based on May 19th, 2025.  
**Developer:** Google Inc.  
**URL:** [https://code.earthengine.google.com/]()<br>
**Computation Time**: <br>
- The script takes approximately **2 seconds** in GEE to generate the TREC dataset. <br>
- The visualization time depends depends on the extent of image selected in the GEE display window.

**Name:** Soil and Water Assessment Tool Plus (SWAT+)  
**Base Version:** Revision 61.0.2.5-40  
**System Requirements:** WindowsOS<br>
**Source:** The SWAT+ Model used in the study is based on the SWAT+ source code available on [SWATplus Github page](https://github.com/swat-model/swatplus/blob/main/doc/Building.md) and a direct executable file can be obtained by request in the [SWAT+ community forum](https://swat.tamu.edu/contact/).<br>
**Computation Time:** <br>
Typically around **10 minutes**, but depends on the watershed size, number of LSUs, and complexity in management practices.

**Name:** Python<br>
**Package requirements:** `geopandas`,`pandas`<br>
**Version:** 3.13.1<br>
**Required packages:** geopandas, pandas, numpy, shapely, joblib <br>

```
pip install geopandas pandas numpy shapely joblib
```
---

### LICENSING

This dataset is licensed under the **Creative Commons Attribution (CC-BY) 4.0 International License**.  
This means others are free to share (copy and redistribute the material in any medium or format) and adapt (remix, transform, and build upon the material) for any purpose, even commercially, **as long as appropriate credit is given** to the original creator(s).

Please cite both items when using this dataset:

> This dataset: Mamidala, R. & Liu, L. Tile Drainage and Rotation-Enhanced Cropland Data Layer. Iowa State University (2025) doi:https://doi.org/10.25380/iastate.29869676 <br>
> The related publication as linked to this dataset at the link above.

For more information on the license, visit:  [https://creativecommons.org/licenses/by/4.0](https://creativecommons.org/licenses/by/4.0)


---
