# Data Dictionary

## Table of Contents

|DataSet|Description|
|---|---|
|Immigration Dictionary|List of fields stored in parquet file with Immigration information|
|Auxiliar Dates|List of fields extracted from distinct dates (arrival +`departure) from Immigratrion table|
|Airports Dictionary|List of fields stored in parquet file with Airports information|
|Cities Dictionary|List of fields stored in parquet file with cities information|
|Population Dictionary|List of fields stored in parquet file with population linked cities information|
|Temperatures Dictionary|List of fields stored in parquet file with temperatures linked cities information|

### Immigration Dictionary
|Field|Description|
|---|---|
|cicid|unique integer identifier for the record|
|i94yr|integer representing year|
|i94mon|integer representing month|
|i94ict|integer coded in SAS dictionary i94cntyl representing departing country|
|i94res|integer coded in SAS dictionary i94cntyl representing nationality|
|i94port|integer coded in SAS dictionary representing Airport|
|arrdate|integer representing date (SAS format starts in Jan1st1960)|
|i94mode|There are missing values as well as not reported (9) i94model={1:'Air',   2:'Sea',   3:'Land',   9:'Not reported'}|
|i94addr|State code matching SAS List i94addr. Unmatches codes should go into 'other' |
|depdate|integer representing date (SAS format starts in Jan1st1960)|
|i94bir|Integer representing Birth year of passenger|
|i94visa|Integer Visa codes collapsed into three categories{1 = Business, 2 = Pleasure, 3 = Student}|
|count|Integer field used for analytics|
|dtadfile|Character Date Field - Date added to I-94 Files|
|visapost|Department of State where where Visa was issued|
|occup|Occupation that will be performed in U.S.|
|entdepa|Arrival Flag - admitted or paroled into the U.S.|
|entdepd|Departure Flag - Departed, lost I-94 or is deceased|
|entdepu|Update Flag - Either apprehended, overstayed, adjusted to perm residence|
|matflag|Match flag - Match of arrival and departure records|
|biryear|4 digit year of birth|
|dtaddto|Character Date Field - Date to which admitted to U.S. (allowed to stay until)|
|gender|Non-immigrant sex|
|insnum|INS number|
|airline|Airline used to arrive in U.S.|
|adnum|Admission Number|
|fltno|Flight number of Airline used to arrive in U.S.|
|visatype|Class of admission legally admitting the non-immigrant to temporarily stay in U.S.|
|descI94ict|Matching description (lookup SAS dictionary) for departing country|
|descI94res|Matching description (lookup SAS dictionary) for nationality|
|newI94port|Matching description (lookup SAS dictionary) for representing port|
|descI94mode|Matching description (lookup SAS dictionary) for travel media|
|stdArrdate|Arrival Date ISO formated based on SAS integer representation for dates|
|stdDepdate|Departure Date ISO formated based on SAS integer representation for dates|

### Auxiliar Dates Dictionary
|Field|Description|
|---|---|
|dateEvent|Standard format date from Immigration records|
|day|Extracted day from dateEvent|
|week|Extracted week from dateEvent|
|month|Extracted month from dateEvent|
|year|Extracted year from dateEvent|
|weekday|Extracted weekday from dateEvent|

### Airports Dictionary
|Field|Description|
|---|---|
|ident|Unique Identifier|
|type|Facility type from the provided list['heliport', 'small_airport', 'closed', 'seaplane_base','balloonport', 'medium_airport', 'large_airport']
|name|Name of the facitily|
|elevation_ft|Elevation in feet. Default to 0.0|
|iso_country|Country filtered to US|
|iso_region|Concatentation of country and State US-xx|
|municipality|Name of the city linked to this airport|
|gps_code|Unique Identifier|
|iata_code|International 3char code. If not provided, this information is infered from gps_code|
|local_code|If not provided, copied from gps_code|
|latitude|Calculated field from numerical coordinates|
|longitude|Calculated field from numerical coordinates|
|state_code|Extracted information from iso_country|
|city_code|Unique identifier for the city (name_state)|

### Cities Dictionary
|Field|Description|
|---|---|
|City|Name of the city|
|State|State Name of the city|
|median_age|Median Age in the city|
|male_population|Number of male citizens|
|female_population|Number of female citizens|
|total_population|Total population as male + female|
|number_of_veterans|Number of veterans in the city|
|foreign_born|Number of foreigners in the city|
|average_household_size|Average Hosehold Size in the city|
|state_code|State Code within US|
|city_code|Unique Identifier for the city (name_state)|

### Population Dictionary
|Field|Description|
|---|---|
|city_code|Unique Identifier for the city (name_state)|
|race|Race description from list ['Hispanic or Latino', 'White', 'Asian', 'Black or African-American','American Indian and Alaska Native']|
|count|Number of people per race per city|

### Temperatures Dictionary
|Field|Description|
|---|---|
|Month|Month of the observation used for partitioning|
|Day|Day of the observation used for future partitioning|
|Year|Year of the observation used for partitioning|
|AvgTemperature|Average Temperature in the city for the given date|
|stateCode|State Code for the given city used for partitioning|
|cityCode|City_stateCode unique identifier for the city|
|temperatureDate|Date of the observation|

