import sys
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from awsglue.utils import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import datetime
import awswrangler as wr

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

fact_output_path = 's3://processed-data-bucket/fact_air_quality'
dim_output_path = 's3://processed-data-bucket/dim_time_metrics'
s3_lookup_table_path = 's3://processed-data-bucket/dim_locations/lookup_locations.parquet'

past_datetime = datetime.datetime.now() - datetime.timedelta(days = 1 )
past_date = f'date={past_datetime.strftime("%Y-%m-%d")}'
input_file_path = f's3://raw-data-bucket/air_quality_dataset/{past_date}'

df_lookup_table = wr.s3.read_parquet(path = s3_lookup_table_path)
lookup_locations = df_lookup_table.to_dict(orient='records')

dim_time_stat_schema = StructType(
    [StructField('time_id', IntegerType(), True), StructField('date_timestamp', StringType(), True),
     StructField('european_aqi', IntegerType(), True), StructField('european_aqi_pm2_5', IntegerType(), True),
     StructField('european_aqi_pm10', IntegerType(), True),
     StructField('european_aqi_nitrogen_dioxide', IntegerType(), True),
     StructField('european_aqi_ozone', IntegerType(), True), StructField('location_id', StringType(), True),
     StructField('generated_date', StringType(), True)])
dim_time_stat = spark.createDataFrame([], schema=dim_time_stat_schema)

# Extracting the S3 File API Data uploaded to S3 using GlueContext
for location in lookup_locations:
    location_id = str(location['location_id'])
    dyf = glueContext.create_dynamic_frame_from_options(connection_type="s3", connection_options={"paths":[f"{input_file_path}/location_{location_id}.json"]}, format="json", format_options={"withHeader":True})
    df = dyf.toDF().createOrReplaceTempView("dim_time_stat_part")
    dim_current_location_stats = spark.sql('''
    select exploded_metrics.time time_id, 
    from_unixtime(exploded_metrics.time) date_timestamp, 
    exploded_metrics.european_aqi, exploded_metrics.european_aqi_pm2_5, 
    exploded_metrics.european_aqi_pm10, 
    exploded_metrics.european_aqi_nitrogen_dioxide, 
    exploded_metrics.european_aqi_ozone from 
    (Select explode(arrays_zip(
    hourly.time,
    hourly.european_aqi,
    hourly.european_aqi_pm2_5,
    hourly.european_aqi_pm10,
    hourly.european_aqi_nitrogen_dioxide,
    hourly.european_aqi_ozone)) exploded_metrics 
    from dim_time_stat_part)
    tbl
    ''')
    dim_time_stat = dim_time_stat.union(
        dim_current_location_stats.withColumn('location_id', lit(location_id)).withColumn('generated_date',
                                                                                          col('date_timestamp').cast(
                                                                                              DateType())))
dim_time_stat = dim_time_stat.select('time_id', 'location_id', 'date_timestamp', 'generated_date', 'european_aqi',
                                     'european_aqi_pm2_5', 'european_aqi_pm10', 'european_aqi_nitrogen_dioxide',
                                     'european_aqi_ozone')
fact_air_quality = dim_time_stat.groupBy('location_id').agg(
    min('generated_date').cast(DateType()).alias('generated_date'),
    avg('european_aqi').cast(IntegerType()).alias('avg_aqi'),
    avg('european_aqi_pm2_5').cast(IntegerType()).alias('avg_aqi_pm2_5'),
    avg('european_aqi_pm10').cast(IntegerType()).alias('avg_aqi_pm10'),
    avg('european_aqi_nitrogen_dioxide').cast(IntegerType()).alias('avg_aqi_no2'),
    avg('european_aqi_ozone').cast(IntegerType()).alias('avg_aqi_o'))

# Writing Dynamic Frame to S3 and exposing the S3 Files to Athena as Table under Database via AWS Glue Data Catalog
s3output = glueContext.getSink(
    path=fact_output_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=['generated_date'],
    compression="snappy",
    enableUpdateCatalog=True
)
s3output.setCatalogInfo(
    catalogDatabase="db_air_quality", catalogTableName="fact_air_quality"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DynamicFrame.fromDF(fact_air_quality, glueContext))

s3output = glueContext.getSink(
    path=dim_output_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=['generated_date'],
    compression="snappy",
    enableUpdateCatalog=True
)
s3output.setCatalogInfo(
    catalogDatabase="db_air_quality", catalogTableName="dim_time_metrics"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DynamicFrame.fromDF(dim_time_stat, glueContext))
job.commit()
