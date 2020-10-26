# Data Dictionary

## Table of Contents

|aaaaa|bbbbb|
|---|---|
|Airports Dictionary| List of fields stored in parquet file with Airports information|
|aaaa1 | bbbb1|

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
