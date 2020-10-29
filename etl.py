import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as sf
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek,concat_ws
from SAS_Valid_Values import i94cntyl, i94prtl, i94model, i94addrl
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('START', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('START','AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    input_parameters:
        - none
       
    output parameters:
         - spark session with granted s3 user
    
    Description:
    
    Creates a generic sparksession granted with user and password staroes at dl.cfg
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark._jsc.hadoopConfiguration().set('fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration().set('fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
    spark._jsc.hadoopConfiguration().set('fs.s3.access.key', os.environ['AWS_ACCESS_KEY_ID'])
    spark._jsc.hadoopConfiguration().set('fs.s3.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
    return spark


def process_temperatures_data(spark, input_data, output_data):
    """
    input_parameters:
        - spark: Spark session object
        - input_data: s3a route to public udacity input data
        - output_data: s3 route to private results 
        
    output parameters:
        - none
    
    Description:
    
    Reads input source for temperatures data and perfoms etl and wrangling.
    Store final information in output source with parquet format
    """
    
    # get filepath to temperatures data file
    temp_data_path = input_data+"city_temperature.zip"
    print ("Getting Path to Temperatures ...")
    
    # Read in the data here
    df3 = pd.read_csv(temp_data_path,sep=',',low_memory=False, compression='zip')

    # Filter for only US cities
    df3=df3[df3['Country']=='US']

    # Drop unsued columns and duplicates
    df3.drop('Region',  axis='columns', inplace=True)
    df3.drop('Country',  axis='columns', inplace=True)
    df3=df3.drop_duplicates()

    # Create a reverse dict from SAS i94addrl
    i94addrl_r = dict(zip(i94addrl.values(),i94addrl.keys()))

    # Convert to spark Dataframe
    df3_spark=spark.createDataFrame(df3)

    # Define a lookup UDF and create StateCode column based on State name informationç
    def getState(key, default=None):
        if str(key).upper() in i94addrl_r:
            return i94addrl_r[str(key).upper()]
        return default
    get_i94addrl_r = udf(lambda x: getState(x))
    df3_spark = df3_spark.withColumn("stateCode",get_i94addrl_r(df3_spark.State))

    # Create CityCode with contatenation
    df3_spark = df3_spark.withColumn('cityCode', sf.concat(sf.col('City'),sf.lit('_'), sf.col('stateCode')))

    # Group observation date into a single column
    df3_spark = df3_spark.withColumn('temperatureDate', 
                        sf.concat(sf.col('Year'),sf.lit('-'), sf.lpad(df3_spark['Month'],2,'0'),
                        sf.lit('-'), sf.lpad(df3_spark['Day'],2,'0')))

    # Remove unused columns
    df3_spark=df3_spark.drop('State')
    df3_spark=df3_spark.drop('City')

    # Remove null values
    df3_spark=df3_spark.na.drop()

    # Write to parquet file (pending partition by STATECODE / YEAR / MONTH)
    df3_spark.write.mode('append').partitionBy("stateCode","Year", "Month").parquet(output_data+"temperatures/temperatures.parquet")
    print ("Writing Temperatures Table to Parquet")
    print ("Number of rows written: ",df3_spark.count())

    
def process_cities_data(spark, input_data, output_data):
    """
    input_parameters:
        - spark: Spark session object
        - input_data: s3a route to public udacity input data
        - output_data: s3 route to private results 
        
    output parameters:
        - none
    
    Description:
    
    Reads input source for cities data and perfoms etl and wrangling.
    Store final information in output source with parquet format
    """
    
    # get filepath to cities data file
    city_data_path = input_data+"us-cities-demographics.csv"
    print ("Getting Path to Cities ...")    
    
    # Read in the data here
    df = pd.read_csv(city_data_path,sep=';')

    # Fix average HouseHold Size by applying mean value
    df['Average Household Size'].fillna(df['Average Household Size'].mean(), inplace=True)

    # Fix Number of Beterans by mean percentage over mean population applied to total population
    df['Number of Veterans'].fillna((df['Total Population']*df['Number of Veterans'].mean()/df['Total Population'].mean()).astype(int), inplace=True)

    # Fix Foreign Born by mean percentage over mean population applied to total population
    df['Foreign-born'].fillna((df['Total Population']*df['Foreign-born'].mean()/df['Total Population'].mean()).astype(int), inplace=True)

    # Fix Male Population by mean percentage over mean population applied to total population
    df['Male Population'].fillna((df['Total Population']*df['Male Population'].mean()/df['Total Population'].mean()).astype(int), inplace=True)

    # Fix Female Population as total population minus estimated Male Population
    df['Female Population'].fillna((df['Total Population']-df['Male Population']).astype(int), inplace=True)

    # Create CityCode unique identifier
    df['city_code']= df[['City', 'State Code']].agg('_'.join, axis=1)

    # Create df with compatible column names
    df_city = df.rename(columns = {'Median Age': 'median_age', 'Male Population': 'male_population','Female Population': 'female_population'
                                  ,'Total Population': 'total_population','Number of Veterans': 'number_of_veterans','Foreign-born': 'foreign_born',
                                   'Average Household Size': 'average_household_size','State Code': 'state_code'}, inplace = False)

    # Create df for population based on df_city columns
    df_population = pd.DataFrame()
    df_population['city_code']=df_city['city_code']
    df_population['race']=df_city['Race']
    df_population['count']=df_city['Count']
    df_population=df_population.drop_duplicates()

    # Write to S3 Parquet file
    population_table = spark.createDataFrame(df_population)
    population_table.write.mode('overwrite').parquet(output_data+"population/population.parquet")
    print ("Writing Population Table to Parquet")
    print ("Number of rows written: ",population_table.count())
    
    # Remove not needed columns and duplicates
    df_city.drop('Race',  axis='columns', inplace=True)
    df_city.drop('Count',  axis='columns', inplace=True)
    df_city=df_city.drop_duplicates()

    # Write to S3 Parquet file
    cities_table = spark.createDataFrame(df_city)
    cities_table.write.mode('overwrite').parquet(output_data+"cities/cities.parquet")
    print ("Writing Citites Table to Parquet")
    print ("Number of rows written: ",cities_table.count())
    
    
    
def process_airports_data(spark, input_data, output_data):
    """
    input_parameters:
        - spark: Spark session object
        - input_data: s3a route to public udacity input data
        - output_data: s3 route to private results 
        
    output parameters:
        - none
    
    Description:
    
    Reads input source for airports data and perfoms etl and wrangling.
    Store final information in output source with parquet format
    """
    
    # get filepath to airports data file
    airp_data_path = input_data+"airport-codes_csv.csv"
    print ("Getting Path to Airports ...")
    
    # Read in the data here
    df_air = pd.read_csv(airp_data_path)

    # Split coordinates
    new_coordinates = df_air["coordinates"].str.split(",", n = 1, expand = True)

    # Create 2 dimension coordinates
    df_air["latitude"]= new_coordinates[1].astype(float).round(2)
    df_air["longitude"]= new_coordinates[0].astype(float).round(2)

    # Adapt format to temperatures file
    df_air['latitude'] = [str(x)+'N' if x > 0 else str(x*-1)+'S' for x in df_air['latitude']]
    df_air['longitude'] = [str(x)+'N' if x > 0 else str(x*-1)+'W' for x in df_air['longitude']]

    # Adapt format to elevation_ft
    df_air['elevation_ft'] = [str(x) for x in df_air['elevation_ft']]

    # Delete original coordinates
    del df_air['coordinates']
    
    # Only US Airports
    df_air=df_air[df_air['iso_country']=='US']
        
    # Review Not Nulls: ident should be populated to iata_code and local_code
    df_air['gps_code'] = df_air['ident']
    df_air['iata_code'] = df_air['ident']
    df_air['local_code'] = df_air['ident']

    # Clean 4 digit iata_code (remove leading K)
    df_air["iata_code"]= [str(x)[-3:] for x in df_air['iata_code']]

    # Set default elevation to 0 for NaN
    df_air['elevation_ft'].fillna('0.0', inplace=True)

    # Set default municipalty as name for NaN
    df_air['municipality'].fillna("("+str(df_air['name'])+")", inplace=True)

    # Define State Code from iso_region
    df_air["state_code"]= [str(x)[-2:] for x in df_air['iso_region']]

    # Define city Code
    df_air["city_code"]= df_air[['municipality', 'state_code']].agg('_'.join, axis=1)

    # Remove unused columns and duplicates
    df_air.drop('continent',  axis='columns', inplace=True)
    df_air.drop_duplicates()

    # Write to S3 Parquet file
    airports_table = spark.createDataFrame(df_air)
    airports_table.write.mode('overwrite').parquet(output_data+"airports/airports.parquet")
    print ("Writing Airports Table to Parquet")   
    print ("Number of rows written: ",airports_table.count())
    
    
def process_immigration_data(spark, input_data, output_data):
    """
    input_parameters:
        - spark: Spark session object
        - input_data: s3a route to public udacity input data
        - output_data: s3 route to private results 
        
    output parameters:
        - none
    
    Description:
    
    Reads input source for immigration data (pre-processes from SAS to parquet) and perfoms etl and wrangling.
    Store final information in output source with parquet format
    """

    # get filepath to immigration data file
    immi_data_path = input_data+"sas_data"
    print ("Getting Path to Immigration Records ...")

    # Load SAS_DATA from RAW parquet file
    df2=spark.read.parquet(immi_data_path)

    # Cast cicid to Integer
    df2 = df2.withColumn("cicid",df2["cicid"].cast(IntegerType()))

    # Cast i94yr (Year) to Integer
    df2 = df2.withColumn("i94yr",df2["i94yr"].cast(IntegerType()))

    # Cast i94mon (Month) to Integer
    df2 = df2.withColumn("i94mon",df2["i94mon"].cast(IntegerType()))

    # Cast i94cit (Country Code SAS) to Integer
    df2 = df2.withColumn("i94cit",df2["i94cit"].cast(IntegerType()))

    # Retrive i94cit description from SAS Dictionary (Country Code SAS)
    def getSASAny(dic, key, default=None):
        if key in dic:
            return dic[key]
        return default

    get_i94cit = udf(lambda x: getSASAny(i94cntyl,x))
    df2 = df2.withColumn("descI94cit",get_i94cit(df2.i94cit))

    # Cast i94res (Country Code SAS) to Integer
    df2 = df2.withColumn("i94res",df2["i94res"].cast(IntegerType()))

    # Retrive i94res description from SAS Dictionary (Country Code SAS)
    def getSASAny(dic, key, default=None):
        if key in dic:
            return dic[key]
        return default

    get_i94res = udf(lambda x: getSASAny(i94cntyl,x))
    df2 = df2.withColumn("descI94res",get_i94cit(df2.i94res))

    # Lookup Dictionary from SAS for i94port value and add to dataframe
    def getSASAny(dic, key, default=None):
        if key in dic:
            return dic[key]
        return default

    get_i94port = udf(lambda x: getSASAny(i94prtl,x))
    df2 = df2.withColumn("newI94port",get_i94port(df2.i94port))

    # Lookup Dictionary from SAS for i94mode value and add to dataframe (default 9 = 'Not reported')
    def getSASAny(dic, key, default=None):
        if key in dic:
            return dic[key]
        return default
    get_i94mode = udf(lambda x: getSASAny(i94model,x,'Not reported'))
    df2 = df2.withColumn("descrI94mode",get_i94mode(df2.i94mode))

    # Lookup Dictionary from SAS for i94addr value and add to dataframe (default 99= 'All Other Codes')
    def getSASAny(dic, key, default=None):
        if key in dic:
            return dic[key]
        return default
    get_i94addr = udf(lambda x: getSASAny(i94addrl,x,'All Other Codes'))

    df2 = df2.withColumn("descrI94addr",get_i94addr(df2.i94addr))

    #Update i94addr to 99 code for non-matching items with SAS Dictionary
    df2=df2.withColumn('i94addr',sf.when(df2.descrI94addr == 'All Other Codes',99).otherwise(df2.i94addr))

    # Cast arrdate (SAS Date Format) to Date String
    def sas_dtnum_to_date (days):
        t0 = datetime.date(year = 1960, month = 1, day = 1)
        return (str(t0 + datetime.timedelta(days)))
    sas_date = udf(lambda x: sas_dtnum_to_date(x))
    df2 = df2.withColumn("stdArrdate",sas_date(df2.arrdate))

    # Cast depdate (SAS Date Format) to Date String
    def sas_dtnum_to_date2 (days):
        t0 = datetime.date(year = 1960, month = 1, day = 1)
        if days==None:
            return None
        else:
            return (str(t0 + datetime.timedelta(days)))
    sas_date2 = udf(lambda x: sas_dtnum_to_date2(x))
    df2 = df2.withColumn("stdDepdate",sas_date2(df2.depdate))

    # Store in parquet File Partitioned by StateCode, Year, Month
    df2.write.mode('append').partitionBy("i94addr","i94yr", "i94mon").parquet(output_data+"immigrations/immigrations.parquet")
    print ("Writing Immigration Table to Parquet")   
    print ("Number of rows written: ",df2.count())
    
    # Create Time Table Aux for Data Analisys (change to final names)
    df_date_arr=spark.createDataFrame(df2.select('stdArrdate').distinct().collect())
    df_date_dep=spark.createDataFrame(df2.select('stdDepdate').distinct().collect())
    print ("Collection new Auxiliar Dates from Immigration Dataset")
    
    # Join both dates into a single column
    df_dates=df_date_dep.union(df_date_arr)

    # Create dummy date record to ensure parquet file exists
    initial_date = [("1960-01-01","01","01","01","1960","01")]
    date_cols = ["dateEvent","day","week","month","year","weekday"]
    dateevents_table = spark.createDataFrame(data=initial_date, schema = date_cols)
    dateevents_table.write.mode('append').parquet(output_data+"dateevents/dateevents.parquet")

    # Load existing Dates from Parquet
    dateevents_table=spark.read.parquet(output_data+"dateevents/dateevents.parquet")

    # Select only DateEvent Column from dataframe
    dateevents_table=spark.createDataFrame(dateevents_table.select('dateEvent').distinct().collect())

    # Integrate existing dates and new dates into a single column
    dateevents_table=dateevents_table.union(df_dates)

    # Consolidate dates with distinct clause
    dateevents_table=spark.createDataFrame(dateevents_table.distinct().collect())

    # Calculate date derivated fields into datafram
    dateevents_table = dateevents_table.select(dateevents_table.dateEvent,dayofmonth('dateEvent').alias('day'),weekofyear('dateEvent').alias('week'),month('dateEvent').alias('month'),
                                                     year('dateEvent').alias('year'),dayofweek('dateEvent').alias('weekday'))

    #overwrite results in parquet
    dateevents_table.write.mode('overwrite').parquet(output_data+"dateevents/dateevents.parquet")
    print ("Writing Auxiliar Dates Table to Parquet")
    print ("Number of rows written: ",dateevents_table.count())

    
def perform_dataquality(spark, output_data):
    """
    input_parameters:
        - spark: Spark session object
        - output_data: s3 route to quality results 
        
    output parameters:
        - none
    
    Description:
    
    Reads input source (existing parquet files) and does data assessment for quality proposals.
    Store resulting dataframes in output route (csv format)
    """

    # get filepath to output data file
    dq1_data_path = output_data+"dataquality1.csv"
    dq2_data_path = output_data+"dataquality2.csv"
    print ("Getting Path to DataQuality output file ...")
    
    # Check integrity of Temperatures vs Cities
    # Load existing Temperatures Records from Parquet
    temperatures_check_table=spark.read.parquet(output_data+"temperatures/temperatures.parquet")
    temperatures_check_table.createGlobalTempView("temperature_check")

    # Load existing Cities Records into a tempView
    cities_check_table=spark.read.parquet(output_data+"cities/cities.parquet")
    cities_check_table.createGlobalTempView("cities_check")

    # Return Temperatures reported from cities not in cities tables into a Pandas Dataframe
    dataQuality1=spark.sql("SELECT distinct (state_code) FROM global_temp.cities_check \
                           where state_code not in (SELECT distinct (stateCode) FROM global_temp.temperature_check)").toPandas()
    dataQuality1.to_csv(dq1_data_path)
    print ("Writing DataQuality 1 output to CSV")
    
    # Return Temperatures reported from cities not in cities tables into a Pandas Dataframe
    dataQuality2=spark.sql("SELECT distinct (cityCode) FROM global_temp.temperature_check \
                           where cityCode not in (SELECT distinct (city_code) FROM global_temp.cities_check)").toPandas()
    dataQuality2.to_csv(dq2_data_path)
    print ("Writing DataQuality 2 output to CSV") 
    
    spark.catalog.dropGlobalTempView("temperature_check")
    spark.catalog.dropGlobalTempView("cities_check")
    print ("Dropped temporary views generated")

    
def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = "s3://udcusr44caps/outdata/"
    input_data = ""
    output_data = "outdata/"
    process_temperatures_data(spark, input_data, output_data)    
    process_cities_data(spark, input_data, output_data)
    process_airports_data(spark, input_data, output_data)
    process_immigration_data(spark, input_data, output_data)
    perform_dataquality(spark, output_data)
    
if __name__ == "__main__":
    main()