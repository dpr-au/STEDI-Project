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

# Script generated for node CustomerCurated
CustomerCurated_node1738427479240 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1738427479240")

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1738356086890 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1738356086890")

# Script generated for node SQL Query
SqlQuery6812 = '''
SELECT *
FROM s
INNER JOIN c ON s.serialnumber = c.serialnumber;
'''
SQLQuery_node1738502232431 = sparkSqlQuery(glueContext, query = SqlQuery6812, mapping = {"c":CustomerCurated_node1738427479240, "s":StepTrainerLanding_node1738356086890}, transformation_ctx = "SQLQuery_node1738502232431")

# Script generated for node Drop Fields
DropFields_node1738447184613 = DropFields.apply(frame=SQLQuery_node1738502232431, paths=["serialNumber", "customerName", "email", "phone", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1738447184613")

# Script generated for node Drop Duplicates
DropDuplicates_node1738504086051 =  DynamicFrame.fromDF(DropFields_node1738447184613.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1738504086051")

# Script generated for node Step_Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1738504086051, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738356037811", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Step_TrainerTrusted_node1738356301411 = glueContext.getSink(path="s3://stedi-proj4/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_TrainerTrusted_node1738356301411")
Step_TrainerTrusted_node1738356301411.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
Step_TrainerTrusted_node1738356301411.setFormat("json")
Step_TrainerTrusted_node1738356301411.writeFrame(DropDuplicates_node1738504086051)
job.commit()