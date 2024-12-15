[EN]

# Description:

This database contains the data set described in Andrieux B. et al. (in preparation) about carbon stocks in Quebec's spruce feathermoss forests. 
Carbon density in different reservoirs of the ecosystem is available at the plot scale (n = 72): living aboveground biomass, coarse woody debris, organic soil horizon, mineral soil (top 15 cm), mineral soil (15-35 cm) and illuvial (B) horizon (top 15 cm). 
Ancillary biophysical data includes geographic locations, topographic surveys, composition of the moss mat, time since fire, and soil properties (carbon content, bulk density, forest floor depth, pH, texture and amorphous metals).

# Data files in "Climat_SOC.rar" (.csv, semicolon-separated):

## Loc.csv - Plots' geographical locations and topography
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ Y - Latitude (dd)
	|
	|________ X - Longitude (dd)
	|
	|________ Z - Elevation (m)
	|
	|________ Slope_INC - Slope inclination (°)
	|
	|________ Slope_OR - Slope orientation with respect to the north (°)

## CLIMAT.csv - Climatic data
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ MAT - Mean annual temperature (°C)
	|
	|________ MAP - Mean annual precipitation (mm)
	|
	|________ GDD5 - Growing degree-days above 5°C (°C)
	|
	|________ WB - Water balance (mm)
	
## Cstocks.csv - Carbon (C) stocks among the main ecosystem pools
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ FH_C - Forest floor (fragmented + humus) carbon density (t/ha)
	|
	|________ min015_C - Mineral soil (from 0 to 15 cm) carbon density (t/ha)
	|
	|________ min1535_C - Mineral soil (from 15 to 35 cm) carbon density (t/ha)
	|
	|________ B015_C - Accumulation horizon B (from 0 to 15 cm) carbon density (t/ha)
	|
	|________ CWD_C - Lying woody debris carbon density (t/ha)
	|
	|________ ABGtrees_C - Aboveground tree organs carbon density (t/ha)

## TSF.csv - Fire history
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ TSF - Time since fire (year before 2015)

## PHYCHI.csv - Physico-chemical soil parameters
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ Mor_pH - Forest floor potential of hydrogen (pH)
	|
	|________ B_pH - Accumulation horizon B pH
	|
	|________ min015_Sand - Mineral soil (from 0 to 15 cm) sand percentile 
	|
	|________ min015_Silt - Mineral soil (from 0 to 15 cm) silt percentile 
	|
	|________ min015_Clay - Mineral soil (from 0 to 15 cm) clay percentile 
	|
	|________ min1535_Sand - Mineral soil (from 15 to 35 cm) sand percentile 
	|
	|________ min1535_Silt - Mineral soil (from 15 to 35 cm) silt percentile 
	|
	|________ min1535_Clay - Mineral soil (from 15 to 35 cm) clay percentile 
	|
	|________ Al_pyro - Sodium pyrophosphate exctractable aluminum (g/kg) in the B horizon
	| 
	|________ Fe_pyro - Sodium pyrophosphate exctractable iron (g/kg) in the B horizon

## MOSSES.csv - Muscinal stratum composition
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ HYL - Hylocomium splendens sum of occurrences
	|
	|________ PLE - Pleurozium schreberi sum of occurrences
	|
	|________ PTI - Ptilium crista-castrensis sum of occurrences
	|
	|________ SPH - Sphagnum spp. sum of occurrences

## Cvar.csv - Soil parameters used to calculate carbon stocks
	|
	|________ Site_ID - Plot's unique identifier
	|
	|________ FH_Cp - FH layer carbon concentration (%)
	|
	|________ FH_BD - FH layer bulk density (g/cm3)
	|
	|________ FH_depth - Mean FH layer depth (cm)
	|
	|________ min015_Cp - Mineral soil (from 0 to 15 cm) carbon concentration (%)
	|
	|________ min015_BD - Mineral soil (from 0 to 15 cm) bulk density (g/cm3)
	|
	|________ min1535_Cp - Mineral soil (from 15 to 35 cm) carbon concentration (%)
	|
	|________ min1535_BD - Mineral soil (from 15 to 35 cm) bulk density (g/cm3)
	|
	|________ B015_Cp - Accumulation horizon B (from 0 to 15 cm) carbon concentration (%)
	|
	|________ B015_BD - Accumulation horizon B (from 0 to 15 cm) bulk density (g/cm3)