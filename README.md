# UdacityDataEng_Capstone

This project has been built based on the provided dataset from Udacity. This dataset is focused in a SAS Format Immigration records from 2016 issued monthly. Some other datasets (csv format) are also provided:
-	Airports
-	Cities
-	Temperatures

After first Data assessment, I have browsed (https://www.kaggle.com/sudalairajkumar/daily-temperature-of-major-cities) for an alternate temperature’s dataset, as far as the provided one has only record up to 2013. In order to assess and manage the data from the dataset I have used Pandas for small ones (Airports and Cities) and PySpark for the big ones (Immigrations and Temperatures).

In addition, a SAS Dictionary with valid values is also provided. Using a macro-based editor, I have converted this file into a Python importable file to assist in the other datasets processing.

As a part of the assessment I have run two data quality steps:
-	Top 10 Immigration registration Airports not linked
-	Top 10 Temperatures not linked with registered cities

A first Data Model approach aimed to build an Star based desing as follows:
-	Immigration Records (Fact Table)
-	Airports (Dim Table)
-	Cities (Dim Table)
-	Population (Dim Table derived from cities dataset)
-	Temperatures (Dim Table)
-	Dates (Dim Table derived from dates used by Immigration Records)

A correct relationship between Immigration <> Airports + Airports <>Cities + Cities<>Temperatures is required to grant this model. Based on the gaps detected by quality check I have decided to use a different link between tables: StateCode. This field is available in all datasets (including SAS Dictionary). In addition, **StateCode** will be used also to partition large parquet files.

With this premises, the purpose of the final data model will be to provide analytic tables of Immigration records with information grouped by States. That will also ease the option to incorporate more datasets from external sources to enrich the analytic (always detailed, up to state level).

At this stage I think that final analytical usage (100% defined) cannot be properly define so my technological design will be based on Spark+EMR. Advantages of this decision:
-	Direct translation from Python ETL to DAG jobs.
-	Ability to work with huge volumes of raw information.
-	Ability to define a “lazy” (not fully normalized) star data model that allow users to quickly work with an analytical model.
-	No information is discarded. That will allow our data model to be enriched with external sources.

---

There will be major change upon analysing the new scenario:
-	The data was increased by 100x.
-	The pipelines would be run daily by 7 am every day.
-	The database needed to be accessed by 100+ people.

Spark is easily expandable to assume a data volume 100 times bigger, as far as process is based on distributed storage. We will need to increase number nodes (per configuration). All other Spark operations will remain immutable.

In order to face daily processing, a new component should be considered: Airflow. That can trigger the existing process with the required setting. We can hook Airflow to existing EMR ETL. As an improvement, we can delegate Data Quality to Airflow, considering that this component can report/trace/log better the results.

Regarding the last requirement (+100 people) with simultaneous access, I would plan Redshift Database. Once Spark ETL is completed, Airflow can stage a consolidated information (from parquet processed files in Spark) and load Dim + Fact tables in Redshift.

---

The project has two deliveries:
-	Data_Wrangling_Notebook.
-	ETL Pyhon Script (derived from Notebook).

The steps followed in the code are:
-	Process Airport DataSet and Store in Parquet
-	Process Airport DataSet and Store in Parquet
-	Process Airport DataSet and Store in Parquet
-	Process Airport DataSet and Store in Parquet
-	Create Date Auxiliar dataset derived from and Store in Parquet
-	Perform DataQuality #1 and print results.
-	Perform DataQuality #2 and print results.

File Name | Description | Go to File
------------ | ------------- | -------------
[immigration_data_sample.csv](/README.md#immigration-data) | Reduce set for initial exploration of immigration data | [Dataset available](https://github.com/acastilloferia/UdacityDataEng_Capstone/tree/main/INPUT_DATA/sas_data)
[I94_SAS_Labels_Descriptions](/README.md#i94_sas_labels_descriptionssas-and-sas_valid_valuespy) | SAS format dictionary with valid values for Immigrations File | [Dataset available](https://github.com/acastilloferia/UdacityDataEng_Capstone/blob/main/INPUT_DATA/I94_SAS_Labels_Descriptions.SAS)
[airport_codes_csv.csv](/README.md#airport-codes-file) | Complete informations regarding Wordwide airports | [Dataset available](https://github.com/acastilloferia/UdacityDataEng_Capstone/blob/main/INPUT_DATA/airport-codes_csv.csv)
[us-cities-demographics.csv](/README.md#US-cities-demographics-file) | Complete informations regarding US Cities and its population | [Dataset available](https://github.com/acastilloferia/UdacityDataEng_Capstone/blob/main/INPUT_DATA/us-cities-demographics.csv)
[SAS_Valid_Values.py](/README.md#i94_sas_labels_descriptionssas-and-sas_valid_valuespy) | PYTHON format dictionary created from SAS information (added) | [Dataset available](https://github.com/acastilloferia/UdacityDataEng_Capstone/blob/main/INPUT_DATA/SAS_Valid_Values.py)
[city_temperature.zip](/README.md#city-temperatures) | Dataset in CSV with Temperatures by city imported from external source (added) | [Dataset available](https://github.com/acastilloferia/UdacityDataEng_Capstone/blob/main/INPUT_DATA/city_temperature.zip)

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
Following steps exposes data wrangling applied to this dataset:
* Initial review ``` df2.describe().show() ```

![Initial Info_Immigration](/images/img_immi_ini.png)
* Initial information schema ``` df2.show(5) ```

![Initial Schema Immigration](/images/img_immi_ini_cols.png)
* Final review ``` df2.describe().show() ```

![Final Info_Immigration](/images/img_immi_end.png)
* Final information schema ``` df2.show(5) ```

![Final Schema Immigration](/images/img_immi_end_cols.png)

[Field details for Immigration described in Dictionary](/DICTIONARY/Data_Dictionary.md#immigration-dictionary)
### City temperatures
Dataset provided by Udacity handled data up to 2013. Considering that immigration Dataset is focused on 2016, I have browsed for an alternative third party dataset with similar information updated, at least, till 2016. Followin url gives access to this dataset, also available in ZIP format in INPUT_DATA folder (https://www.kaggle.com/sudalairajkumar/daily-temperature-of-major-cities)
I have inspected this file using Pandas Dataframe. I have perfomed the following steps:
* Filter only *US* data by field *Country*  
* Remove *Region* and *Country* fields (no longer needed)
* Remove duplicates
Afterwards, I have moved the dataset to Spark Dataframe, performing following actions:
* Define *StateCode* based on *State* Name vs *SAS_Valid_Values.py* information.
* Create an unique *cityCode* with City_StateCode information.
* Drop *State* and *City* information
* Save to parquet (append mode) to outdata_path+"temperatures/temperatures.parquet" partitioned by StateCode, Year and Month
Following steps exposes data wrangling applied to this dataset:
* Initial review ``` df3.info() ```

![Initial Info_Temperatures](/images/img_temp_ini.png)
* Initial information schema ``` df3.head() ```

![Initial Schema Temperatures](/images/img_temp_ini_cols.png)
* Final review ``` df3_spark.describe().show() ```

![Final Info_Temperatures](/images/img_temp_end.png)
* Final information schema ``` df3_spark.show(5) ```

![Final Schema Temperatures](/images/img_temp_end_cols.png)

[Field details for Temperatures described in Dictionary](/DICTIONARY/Data_Dictionary.md#temperatures-dictionary)

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

### US Cities Demographics File
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
