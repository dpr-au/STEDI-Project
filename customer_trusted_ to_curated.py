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

# Script generated for node Customer Trusted
CustomerTrusted_node1738427479240 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1738427479240")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1738356086890 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1738356086890")

# Script generated for node Join
Join_node1738356099371 = Join.apply(frame1=AccelerometerLanding_node1738356086890, frame2=CustomerTrusted_node1738427479240, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1738356099371")

# Script generated for node Drop Fields and Duplicates
SqlQuery6833 = '''
select distinct customerName, email, phone, birthDay, serialNumber, registrationDate, lastUpdateDate, shareWithResearchAsOfDate, shareWithPublicAsOfDate, shareWithFriendsAsOfDate from myDataSource
'''
DropFieldsandDuplicates_node1738430745288 = sparkSqlQuery(glueContext, query = SqlQuery6833, mapping = {"myDataSource":Join_node1738356099371}, transformation_ctx = "DropFieldsandDuplicates_node1738430745288")

# Script generated for node Customer curated
EvaluateDataQuality().process_rows(frame=DropFieldsandDuplicates_node1738430745288, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738356037811", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customercurated_node1738356301411 = glueContext.getSink(path="s3://stedi-proj4/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customercurated_node1738356301411")
Customercurated_node1738356301411.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
Customercurated_node1738356301411.setFormat("json")
Customercurated_node1738356301411.writeFrame(DropFieldsandDuplicates_node1738430745288)
job.commit()