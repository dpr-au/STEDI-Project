import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1738427479240 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1738427479240")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1738356086890 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AWSGlueDataCatalog_node1738356086890")

# Script generated for node Customer Privacy Filter
CustomerPrivacyFilter_node1738356099371 = Join.apply(frame1=AWSGlueDataCatalog_node1738356086890, frame2=AWSGlueDataCatalog_node1738427479240, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilter_node1738356099371")

# Script generated for node Drop Fields
DropFields_node1738356558832 = DropFields.apply(frame=CustomerPrivacyFilter_node1738356099371, paths=["email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername", "timestamp"], transformation_ctx="DropFields_node1738356558832")

# Script generated for node Accelometer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1738356558832, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738356037811", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelometerTrusted_node1738356301411 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1738356558832, connection_type="s3", format="json", connection_options={"path": "s3://stedi-proj4/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AccelometerTrusted_node1738356301411")

job.commit()