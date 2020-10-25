# UdacityDataEng_Capstone
Udacity Capstone Project for Data Engineer Nanodegree
I have decided to explore Udacity given Data Sets:
File Name | Description
------------ | -------------
Content from cell 1 | Content from cell 2
airport_codes_csv.csv | Complete informations regarding Wordwide airports
us-cities-demographics.csv | Complete informations regarding US Cities and its population
* File 1
* File 2
* airport_codes_csv.csv
* Filee 4

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
* Save cities dataframeto parquet (overwrite mode) to outdata_path+"cities/cities.parquet" (remove *Race*, *Count* and duplicates before)
* Save population dataframeto parquet (overwrite mode) to outdata_path+"population/population.parquet"
