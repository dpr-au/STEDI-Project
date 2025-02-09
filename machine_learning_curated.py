import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1738427479240 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1738427479240")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1738356086890 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1738356086890")

# Script generated for node SQL Query
SqlQuery3154 = '''
select * 
from a
inner join t on a.timestamp = t.sensorreadingtime;
'''
SQLQuery_node1738502232431 = sparkSqlQuery(glueContext, query = SqlQuery3154, mapping = {"a":AccelerometerTrusted_node1738427479240, "t":StepTrainerTrusted_node1738356086890}, transformation_ctx = "SQLQuery_node1738502232431")

# Script generated for node Drop Fields
DropFields_node1738447184613 = DropFields.apply(frame=SQLQuery_node1738502232431, paths=["serialNumber", "customerName", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate", "x", "y", "user", "z"], transformation_ctx="DropFields_node1738447184613")

# Script generated for node Drop Duplicates
DropDuplicates_node1738504086051 =  DynamicFrame.fromDF(DropFields_node1738447184613.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1738504086051")

# Script generated for node Machine Learning  Zone
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1738504086051, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738356037811", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningZone_node1738356301411 = glueContext.getSink(path="s3://stedi-proj4/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningZone_node1738356301411")
MachineLearningZone_node1738356301411.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningZone_node1738356301411.setFormat("json")
MachineLearningZone_node1738356301411.writeFrame(DropDuplicates_node1738504086051)
job.commit()