import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, initcap

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'healthcare_data_combined'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#print("Starting data load for tobacco, alcohol, and nutrition data")

tobacco_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": ["s3://s3healthdata/Data_Files/tobacco/"]},
    format="csv",
    format_options={"withHeader": True}
)

alcohol_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": ["s3://s3healthdata/Data_Files/alcohol/"]},
    format="csv",
    format_options={"withHeader": True}
)

nutrition_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3", 
    connection_options={"paths": ["s3://s3healthdata/Data_Files/nutrition/"]},
    format="csv",
    format_options={"withHeader": True}
)

tobacco_df = tobacco_data.toDF()
alcohol_df = alcohol_data.toDF()
nutrition_df = nutrition_data.toDF()

from pyspark.sql.functions import col
tobacco_df = tobacco_df.withColumn("year_id", col("year_id").cast("int"))
alcohol_df = alcohol_df.withColumn("year_id", col("year_id").cast("int"))
nutrition_df = nutrition_df.withColumn("year_id", col("year_id").cast("int")) \
                           .withColumn("location_name", initcap(col("location_name")))

merged_df = tobacco_df \
    .join(alcohol_df, on=["location_name", "year_id", "sex_name"], how="inner") \
    .join(nutrition_df, on=["location_name", "year_id", "sex_name"], how="inner")
    
logger = glueContext.get_logger()

logger.info(f"Number of rows in merged_df: {merged_df.count()}")

merged_df.show(5)

merged_dynamic_frame = DynamicFrame.fromDF(merged_df, glueContext, "merged_dynamic_frame")

glueContext.write_dynamic_frame.from_options(
    frame = merged_dynamic_frame,
    connection_type = "s3",
    connection_options = {
        "path": "s3://s3healthdata/Lifestyledata_combined_v5/",
        "partitionKeys": [],
    },
    format = "csv",
    format_options={"separator": ","}
)

logger.info(f"Tobacco data contains {tobacco_df.count()} rows")
logger.info(f"Alcohol data contains {alcohol_df.count()} rows")
logger.info(f"Nutrition data contains {nutrition_df.count()} rows")

job.commit()
