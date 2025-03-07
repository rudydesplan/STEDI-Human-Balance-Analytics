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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1737394329491 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1737394329491")

# Script generated for node Customer Trusted
CustomerTrusted_node1737394330625 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1737394330625")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct
    step_trainer.sensorreadingtime
    , step_trainer.serialnumber
    , step_trainer.distancefromobject
FROM step_trainer
INNER JOIN customer
ON step_trainer.serialnumber = customer.serialnumber
'''
SQLQuery_node1737394427863 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer":CustomerTrusted_node1737394330625, "step_trainer":StepTrainerLanding_node1737394329491}, transformation_ctx = "SQLQuery_node1737394427863")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737394427863, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737392904412", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1737394519233 = glueContext.getSink(path="s3://stedi-lake-house-rudy/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1737394519233")
StepTrainerTrusted_node1737394519233.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1737394519233.setFormat("json")
StepTrainerTrusted_node1737394519233.writeFrame(SQLQuery_node1737394427863)
job.commit()
