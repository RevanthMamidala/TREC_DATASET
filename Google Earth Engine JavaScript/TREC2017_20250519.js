// Author: Revanth Mamidala
// Last Updated: 2025-05-19
// Description: This script processes crop frequency data and the CDL dataset to separate continuous crops and tile-drained areas.

// ===============================================
//  LOAD INPUT DATASETS
// ===============================================
var cornImage = ee.Image("projects/ee-revanthm81011/assets/Tile_Separated_LULC/crop_frequency_corn_2008-2024");
var soybeansImage = ee.Image("projects/ee-revanthm81011/assets/Tile_Separated_LULC/crop_frequency_soybeans_2008-2024");
var cottonImage = ee.Image("projects/ee-revanthm81011/assets/Tile_Separated_LULC/crop_frequency_cotton_2008-2024");
var wheatImage = ee.Image("projects/ee-revanthm81011/assets/Tile_Separated_LULC/crop_frequency_wheat_2008-2024");

var CDL2017 = ee.Image("projects/ee-revanthm81011/assets/Tile_Separated_LULC/2017_30m_cdls");

var tileImage = ee.Image("projects/sat-io/open-datasets/agtile/AgTile-US");
var tile30m_unmasked = tileImage.unmask(0);  // Replace masked values with 0
var tile30m_binary = tile30m_unmasked.reproject({crs: 'EPSG:5070', scale: 30});

// ===============================================
//  DEFINE FUNCTIONS FOR MASKING
// ===============================================
// Function to create binary image where value is between 15 and 17
function binaryFunction(image) {
  return image.gte(15).and(image.lte(17)).rename('binary');
}

// Function to convert binary to null binary: 0 becomes 1, 1 becomes 0
function nullbinaryFunction(binaryImg) {
  return binaryImg.subtract(1).multiply(-1).rename('null_binary');
}

// ===============================================
// Calculating binaries for Single-Crop in CDL dataset
// ===============================================
//    Single-Crop classes are defined as the crops with either there is no crop rotation 
//    or a double crop with missing frequency data for the partner crop.

// Wheat Classes
var wheatClasses = [22, 23, 24, 30, 230, 234, 236];
// Cotton Classes
var cottonClasses = [2, 232]
// Corn Classes
var cornClasses = [1, 12, 13, 226, 228, 237];
// Soybeans Classes
var soybeanClasses = [5,240,254];

// Creating CDL Binaries with crop classes
var CDL_wt_bin = CDL2017.remap(wheatClasses, ee.List.repeat(1, wheatClasses.length)).unmask(0);
var CDL_ct_bin = CDL2017.remap(cottonClasses, ee.List.repeat(1, cottonClasses.length)).unmask(0);
var CDL_cc_bin = CDL2017.remap(cornClasses, ee.List.repeat(1, cornClasses.length)).unmask(0);
var CDL_ss_bin = CDL2017.remap(soybeanClasses, ee.List.repeat(1, soybeanClasses.length)).unmask(0);


// Creating binaries for contionuous-crops from Frequncy rasters
var Xwt_bin = binaryFunction(wheatImage);
var Xct_bin = binaryFunction(cottonImage);
var Xcc_bin = binaryFunction(cornImage);
var Xss_bin = binaryFunction(soybeansImage);

// Restrict the continuous-crop binary to the crop description.
var Ywt_bin = Xwt_bin.multiply(CDL_wt_bin)
var Yct_bin = Xct_bin.multiply(CDL_ct_bin)
var Ycc_bin = Xcc_bin.multiply(CDL_cc_bin)
var Yss_bin = Xss_bin.multiply(CDL_ss_bin)

// Assign unique codes to each continuous crop
var Xwt_522 = Ywt_bin.multiply(522);
var Xct_502 = Yct_bin.multiply(502);
var Xcc_501 = Ycc_bin.multiply(501);
var Xss_505 = Yss_bin.multiply(505);

var Xwt_nullbin = nullbinaryFunction(Ywt_bin);
var Xct_nullbin = nullbinaryFunction(Yct_bin);
var Xcc_nullbin = nullbinaryFunction(Ycc_bin);
var Xss_nullbin = nullbinaryFunction(Yss_bin);

// Creating a null-binary file for Ag-Tile US raster
var tile30m_nullbinary = nullbinaryFunction(tile30m_binary);

// ===============================================
//  APPLY CROP MASKING AND ENCODE CONTINUOUS CROPS without any other rotations
// ===============================================
var CDL2017_wtnull = CDL2017.multiply(Xwt_nullbin);
var CDL2017w = CDL2017_wtnull.add(Xwt_522);

var CDL2017w_ctnull = CDL2017w.multiply(Xct_nullbin);
var CDL2017wc = CDL2017w_ctnull.add(Xct_502);

var CDL2017wc_ssnull = CDL2017wc.multiply(Xss_nullbin);
var CDL2017wcs = CDL2017wc_ssnull.add(Xss_505);

var CDL2017wcs_ccnull = CDL2017wcs.multiply(Xcc_nullbin);
var Single_crop_Rot_SepCDL2017 = CDL2017wcs_ccnull.add(Xcc_501);

//Map.addLayer(RotationSeparatedCDL2017, {}, "RotationSeparatedCDL2017");


// ===============================================
//  Continuous crop rotation for double crops
// ===============================================
/**
 * Function to reclassify a double crop CDL class based on crop frequency.
 * Adds 500 to base code if both crops are continuous, otherwise assigns individual crop value.
 * @param {ee.Image} cdl - The CDL image
 * @param {Number} classCode - CDL value for double crop (e.g., 225)
 * @param {ee.Image} crop1_bin - Binary frequency image for crop 1 (e.g., Xwt_bin)
 * @param {ee.Image} crop2_bin - Binary frequency image for crop 2 (e.g., Xcc_bin)
 * @param {Number} crop1_code - Unique code for crop 1 (e.g., 522)
 * @param {Number} crop2_code - Unique code for crop 2 (e.g., 501)
 * @return {ee.Image} reclassified image with encoded values
 */
function processDoubleCropClass(cdl, classCode, crop1_bin, crop2_bin, crop1_code, crop2_code) {
  var dc_mask = cdl.eq(classCode);
  
  var both_continuous = dc_mask.and(crop1_bin.eq(1)).and(crop2_bin.eq(1))
    .multiply(classCode + 500);

  var only_crop1 = dc_mask.and(crop1_bin.eq(1)).and(crop2_bin.neq(1))
    .multiply(crop1_code+500);

  var only_crop2 = dc_mask.and(crop1_bin.neq(1)).and(crop2_bin.eq(1))
    .multiply(crop2_code+500);

  var no_crop_continuous = dc_mask.and(crop1_bin.neq(1)).and(crop2_bin.neq(1))
    .multiply(classCode);

  return both_continuous.add(only_crop1).add(only_crop2).add(no_crop_continuous);
}

// Separating continuous double crops
var DC_26  = processDoubleCropClass(Single_crop_Rot_SepCDL2017, 26, Ywt_bin, Yss_bin, 22, 5);
var DC_225 = processDoubleCropClass(Single_crop_Rot_SepCDL2017, 225, Ywt_bin, Ycc_bin, 22, 1);
var DC_238 = processDoubleCropClass(Single_crop_Rot_SepCDL2017, 238, Ywt_bin, Yct_bin, 22, 2);
var DC_239 = processDoubleCropClass(Single_crop_Rot_SepCDL2017, 239, Yss_bin, Yct_bin, 5, 2);
var DC_241 = processDoubleCropClass(Single_crop_Rot_SepCDL2017, 241, Ycc_bin, Yss_bin, 1, 5);
var DC_combined = DC_26.add(DC_225).add(DC_238).add(DC_239).add(DC_241);
var DC_combined_bin = (DC_combined.gte(1)).unmask(0);
var DC_combined_nullbin = nullbinaryFunction(DC_combined_bin);

var Single_crop_Rot_SepCDL2017_nullbin = Single_crop_Rot_SepCDL2017.multiply(DC_combined_nullbin);
var Final_RotationSep_CDL2017 = Single_crop_Rot_SepCDL2017_nullbin.add(DC_combined)

// ===============================================
//  TILE DRAINAGE SEPARATION
// ===============================================
// Tile Drained pixels shall be restricted to the land use classes that can have tile drainage.
var tileRelevantClasses = [
  // Row crops
  1, 2, 3, 4, 5, 6, 10, 11, 12, 13,
  // Small grains
  21, 22, 23, 24, 25, 27, 28, 29, 30,
  // Oilseeds & specialty grains
  31, 32, 33, 34, 35, 38, 39,
  // Forages & pasture
  36, 37, 176,
  // Root crops & others
  14, 41, 42, 43, 44, 205, 206, 208,
  // Vegetables, melons, and fruits
  45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
  55, 56, 57, 58, 59, 60, 61,
  // Double crops
  26, 225, 226, 228, 230,231, 232, 233, 
  234, 235, 236, 237, 238, 239, 240, 241, 254,
  // Tree crops (orchard & perennial)
  66, 67, 68, 69, 72, 74, 75, 76, 77, 204, 207, 209, 210, 211,
  212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223,
  224, 227, 229, 242, 243, 244, 245, 246, 247, 248, 249, 250
];
var CDL_tileRelevantClasses_bin = CDL2017.remap(tileRelevantClasses, ee.List.repeat(1, tileRelevantClasses.length)).unmask(0);
var tileDrain_bin = tile30m_binary.multiply(CDL_tileRelevantClasses_bin);
var tileDrain_nullbin = nullbinaryFunction(tileDrain_bin);

var RotationSeparatedCDL2017_Tilenull = Final_RotationSep_CDL2017.multiply(tileDrain_nullbin);
var RotationSeparatedCDL2017_Tiled = (Final_RotationSep_CDL2017.multiply(tileDrain_bin)).add(tileDrain_bin.multiply(1000));

// Final combined output
var Tiledrain_rotation_separated_CDL2017 = RotationSeparatedCDL2017_Tilenull.add(RotationSeparatedCDL2017_Tiled);

var visParamsFinalClipped = {
    min: 1,
    max: 20,
    palette: [
        '#440154', '#481567', '#482677', '#453781', '#404788',
        '#39568C', '#33638D', '#2D708E', '#287D8E', '#238A8D',
        '#1F968B', '#20A386', '#29AF7F', '#3CBB75', '#55C667',
        '#73D055', '#95D840', '#B8DE29', '#DCE319', '#FDE725'
    ]
};

Map.addLayer(Tiledrain_rotation_separated_CDL2017, visParamsFinalClipped, "Tiledrain_rotation_separated_CDL2017");


// ===============================================
//  CLIP TO Region Of Interest (If required)
// ===============================================
//var ROI = ee.FeatureCollection("projects/ee-revanthm81011/assets/BufferPlygn"); //Polygon with same CRS as the CDL dataset
//var FinalClipped = Tiledrain_rotation_separated_CDL2017.clip(ROI);

// ===============================================
//  VISUALIZATION LAYERS
// ===============================================
// Map.centerObject(ROI, 10);

 //// Visualize tile and non-tile drained areas separately
 //var visParamsNonTile = {
 //  min: 1, max: 999,
 //  //palette: ['ADFF2F', 'FFFF00', 'FFA500', 'FF4500']
 //    palette: [
 //    '#FDE725', '#DCE319', '#B8DE29', '#95D840', '#73D055',
 //    '#55C667', '#3CBB75', '#29AF7F', '#20A386', '#1F968B'
 //  ]
 //};
 //var visParamsTile = {
 //  min: 1001, max: 1522,
 //  //palette: ['1E90FF', '00BFFF', '87CEFA', '4682B4']
 //    palette: [
 //    '#238A8D', '#287D8E', '#2D708E', '#33638D', '#39568C',
 //    '#404788', '#453781', '#482677', '#481567', '#440154'
 //  ]
 //};


 //var nonTileCrops = Tiledrain_rotation_separated_CDL2017.updateMask(FinalClipped.gt(0).and(FinalClipped.lte(999)));
 //var tileCrops = FinalClipped.updateMask(FinalClipped.gte(1000));

// Map.addLayer(nonTileCrops, visParamsNonTile, 'Non-Tile Drained Crops');
// Map.addLayer(tileCrops, visParamsTile, 'Tile Drained Crops');
// Map.addLayer(FinalClipped, visParamsFinalClipped, "FinalClipped");

// ===============================================
//  EXPORT OPTIONS (UNCOMMENT TO EXPORT)
// ===============================================

// Export.image.toDrive({
//   image: Tiledrain_rotation_separated_CDL2017,  // Full un-clipped image
//   description: 'Tiledrain_RotationSeparated_CDL2017_Full',
//   folder: 'GEE_Exports',  // Google Drive folder (create manually if it doesn’t exist)
//   fileNamePrefix: 'Tiledrain_RotationSeparated_CDL2017_Full',
//   region: ROI.geometry(),  // Use full image bounds
//   scale: 30,  // Output resolution
//   crs: 'EPSG:5070',  // Target CRS
//   maxPixels: 1e13  // Allow large exports
// });

// Export.image.toAsset({
//   image: Tiledrain_rotation_separated_CDL2017,  // Full un-clipped image
//   description: 'Tiledrain_RotationSeparated_CDL2017_FullAsset',  // Task name
//   assetId: 'projects/ee-revanthm81011/assets/Tiledrain_RotationSeparated_CDL2017_Full',  // Destination asset path
//   region: Tiledrain_rotation_separated_CDL2017.geometry(),  // Use full extent of the image
//   scale: 30,  // Match native resolution
//   crs: 'EPSG:5070',  // Reproject to match your other data
//   maxPixels: 1e13  // Allow export of large rasters
// });
