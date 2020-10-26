# UdacityDataEng_Capstone
Udacity Capstone Project for Data Engineer Nanodegree
I have decided to explore Udacity given Data Sets:
File Name | Description
------------ | -------------
immigration_data_sample.csv | Reduce set for initial exploration of immigration data
I94_SAS_Labels_Descriptions | SAS format dictionary with valid values for Immigrations File
SAS_Valid_Values.py | PYTHON format dictionary created from SAS information
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
* Cast as integers: *cicid*, *i94yr*, *i94mon*, *i94cit*, *i94res*
* Use Python dictionary to create a *descI94cit* field based on *i94cit* (use of udf function)
* Use Python dictionary to create a *newI94port* field based on *i94port* (use of udf function)
* Use Python dictionary to create a *descrI94mode* field based on *i94mode* (use of udf function)
* Use Python dictionary to create a *descrI94addr* field based on *i94addr* (use of udf function)
* Update *i94addr* as '99' when 'All Other Codes' (use of udf function)
* Create an standard date for SAS *arrdate* as *stdArrdate* (use of udf function)
* Create an standard date for SAS *depdate* as *stdDepdate* (use of udf function)
* Save to parquet (append mode) to outdata_path+"immigrations/immigrations.parquet" partitioned by State / Year and Month.

### City temperatures
### I94_SAS_Labels_Descriptions.SAS and SAS_Valid_Values.py
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
Following steps exposes data wrangling applied to this dataset:
* Initial review ``` df_air.info() ```

![Initial Info_Airports](/images/img_air_ini.png)
* Initial information schema ``` df_air.head() ```

![Initial Schema Airports](/images/img_air_ini_cols.png)
* Final review ``` df_air.info() ```

![Final Info_Airports](/images/img_air_end.png)
* Final information schema ``` df_air.head() ```

![Final Schema Airports](/images/img_air_end_cols.png)

[Field details for Airports described in Dictionary](/DICTIONARY/Data_Dictionary.md#airports-dictionary)

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
Following steps exposes data wrangling applied to this dataset:
* Initial review ``` df.info() ```

![Initial Info_Cities](/images/img_citraw_ini.png)
* Initial information schema ``` df.head() ```

![Initial Schema Cities](/images/img_citraw_ini_cols.png)
* Final review Cities ``` df_city.info() ```

![Final Info_Cities](/images/img_city_end.png)
* Final information schema Cities``` df_city.head() ```

![Final Schema Cities](/images/img_cit_end_cols.png)
* Final review Population ``` df_population.info() ```

![Final Info_Population](/images/img_pop_end.png)
* Final information schema Population``` df_population.head() ```

![Final Schema Population](/images/img_pop_end_cols.png)

[Field details for Cities described in Dictionary](/DICTIONARY/Data_Dictionary.md#cities-dictionary)

[Field details for Population described in Dictionary](/DICTIONARY/Data_Dictionary.md#population-dictionary)
