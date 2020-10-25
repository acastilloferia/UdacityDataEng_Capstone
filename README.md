# UdacityDataEng_Capstone
Udacity Capstone Project for Data Engineer Nanodegree
I have decided to explore Udacity given Data Sets:
File Name | Description
------------ | -------------
immigration_data_sample.csv | Reduce set for initial exploration of immigration data
I94_SAS_Labels_Descriptions | SAS format dictionary with valid values for Immigrations File
SAS_Valid_Values.pys | PYTHON format dictionary created from SAS information
airport_codes_csv.csv | Complete informations regarding Wordwide airports
us-cities-demographics.csv | Complete informations regarding US Cities and its population
* File 1
* File 2
* airport_codes_csv.csv
* Filee 4

### Immigration Data
This is the main dataset provided for this project. There is a sample file *immigration_data_sample.csv* and a complete
set (year 2016) in SAS format in the workspace path *../../*.
I have used SAS library to import 1 month SAS file (i94_apr16_sub.sas7bdat)
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.\
config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
.enableHiveSupport().getOrCreate()
df_spark =spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
```
Once read I have stored it in a parquet file *sas_data* available in *OUTPUT_FOLDER*. All data wrangling has been done over parquet file in order to 
improve performance. Steps followed are:
*

### City temperatures
### I94_SAS_Labels_Descriptions.SAS and 
This file has been used to properly understand the contents of Immigration file. It is written in RAW text 
and prepared to be processed under SAS ecosystem. I have edited the file to generate an standard .py importable
source (*SAS_Valid_Values.py*)
* This is a sample code for SAS format
```
libname library 'Your file location' ;
proc format library=library ;
/* I94YR - 4 digit year */
/* I94MON - Numeric month */
/* I94CIT & I94RES - This format shows all the valid and invalid codes for processing */
  value i94cntyl
   582 =  'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)'
   236 =  'AFGHANISTAN'
   101 =  'ALBANIA'
   316 =  'ALGERIA'
   102 =  'ANDORRA'
```   
* this is a sample code for PYTHON format
```javascript
import configparser
# I94YR - 4 digit year
# I94MON - Numeric month 
# I94CIT & I94RES - This format shows all the valid and invalid codes for processing 
i94cntyl = {582:'MEXICO Air Sea, and Not Reported (I-94, no land arrivals)',
   236:'AFGHANISTAN',
   101:'ALBANIA',
   316:'ALGERIA',
   102:'ANDORRA',
```

### Airport Codes File
I have inspected this file using Pandas Dataframe. I have perfomed the following steps:
* Split coordinates field into latitude & longitude (following standard notation).
* Filter only *US* airports  
* Populate *ident* information when *na* in *gps_code*, *iata_code* (3-char) and *local_code** Default *elevation*
* Default *elevation* as 0.0
* Extract *state_code* from *us-region* (US-xx)
* Create an unique *city_code* with municipality_state_code.
* Drop duplicates
* Save to parquet (overwrite mode) to outdata_path+"airports/airports.parquet"

### us-cities-demographics.csv
I have inspected this file using Pandas Dataframe. I have perfomed the following steps:
* Infer na at *Average Household Size* with average.
* Infer na at *Number of Veterans* with average fitted to population case.
* Infer na at *Foreign-born* with average fitted to population case.
* Infer na at *Male Population* with average fitted to population case.
* Infer na at *Female Population* as population less *Female Population*
* Create an unique *city_code* with city_state_code.
* Standarize column name (replace *space* with *underscore* and use lowercase)
* Create a new dataframe *population* from *Race*, *Count*, and *city_code* columns
* Save cities dataframe to parquet (overwrite mode) to outdata_path+"cities/cities.parquet" (remove *Race*, *Count* and duplicates before)
* Save population dataframe to parquet (overwrite mode) to outdata_path+"population/population.parquet"
