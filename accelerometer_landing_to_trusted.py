import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1737319792305 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLandingZone_node1737319792305")

# Script generated for node Customer Trusted
CustomerTrusted_node1737319819749 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1737319819749")

# Script generated for node Join Customer
JoinCustomer_node1737319842571 = Join.apply(frame1=CustomerTrusted_node1737319819749, frame2=AccelerometerLandingZone_node1737319792305, keys1=["email"], keys2=["user"], transformation_ctx="JoinCustomer_node1737319842571")

# Script generated for node SQL Query
SqlQuery0 = '''
select
x , y, z, user, timestamp
from myDataSource

'''
SQLQuery_node1737320117642 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":JoinCustomer_node1737319842571}, transformation_ctx = "SQLQuery_node1737320117642")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737320117642, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737319172847", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1737319853221 = glueContext.getSink(path="s3://stedi-lake-house-rudy/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1737319853221")
AccelerometerTrusted_node1737319853221.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1737319853221.setFormat("json")
AccelerometerTrusted_node1737319853221.writeFrame(SQLQuery_node1737320117642)
job.commit()
