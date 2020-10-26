# Data Dictionary

## Table of Contents

|aaaaa|bbbbb|
|---|---|
|Immigration Dictionary|List of fields stored in parquet file with Immigration information|
|Airports Dictionary|List of fields stored in parquet file with Airports information|
|Cities Dictionary|List of fields stored in parquet file with cities information|
|Population Dictionary|List of fields stored in parquet file with population linked cities information|
|Temperatures Dictionary|List of fields stored in parquet file with temperatures linked cities information|

### Immigration Dictionary
|Field|Description|
|---|---|
|ident|Unique Identifier|

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

