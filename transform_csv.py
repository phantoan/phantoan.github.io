import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import json


# Normalize json object and return map type
def normalize_json( obj):
    new_data = dict()


    for key, value in obj["consultation_content"].items():
        if isinstance(value, dict):
            for k, v in value.items():
                new_data["consultation_content" + key + "_" + k] = v
        else:
            new_data["consultation_content" + "_" + key] = json.dumps(value,ensure_ascii=False)
    if "consultation_content_points_symptoms_index" not in new_data:
        new_data["consultation_content_points_symptoms_index"] = ""

    for sheet in obj["interview_sheet"]:

        question = sheet["question"]

        new_data["interview_sheet" + "_" + str(question) + "_" + "question"] = question
        if "answer" not in sheet:
            new_data["interview_sheet" + "_" + str(question) + "_" + "answer"] = ""
        else:
            new_data["interview_sheet" + "_" + str(question) + "_" + "answer"] = json.dumps(sheet["answer"], ensure_ascii=False)
        if "item_common" not in sheet:
            new_data["interview_sheet" + "_" + str(question) + "_" + "item_common"] = ""
        else:
            new_data["interview_sheet" + "_" + str(question) + "_" + "item_common"] = json.dumps(sheet["item_common"], ensure_ascii=False)
        if "items" in sheet:
            new_data["interview_sheet" + "_" + str(question) + "_" + "items" + "_" + "details"] = json.dumps(
                sheet["items"], ensure_ascii=False)
        else:
            new_data["interview_sheet" + "_" + str(question) + "_" + "items" + "_" + "details"] = ""

    for key, value in obj["others"].items():
        if not isinstance(value, dict):
            new_data["others" + "_" + key] = value
        else:
            for k, v in value.items():
                new_data["others" + "_" + key + "_" + k] = v
    return new_data

#  transform data
def transform_data(obj):
    csv = obj.value.split(',', 2)
    normalized_string = x[2][1:-1].replace("\\n", " ")
    interview_json = json.loads(normalized_string)
    newData = normalize_json(interview_json)
    newData["sf_user_id"] = csv[0]
    newData["updated_at"] = csv[1]
    return newData

# Init job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
input_path = "s3://tienbm-glue-test/data"
output_path = "s3://tienbm-glue-test/output/"

# CSV Schema
schema = ['sf_user_id', 'updated_at', 'consultation_content_consultation_item',
          'consultation_content_consultation_item_other', 'consultation_content_points_symptoms',
          "consultation_content_points_symptoms_index", 'interview_sheet_1_question', 'interview_sheet_1_answer',
          'interview_sheet_1_item_common', 'interview_sheet_1_items_details', 'interview_sheet_2_question',
          'interview_sheet_2_answer', 'interview_sheet_2_item_common', 'interview_sheet_2_items_details',
          'interview_sheet_3_question', 'interview_sheet_3_answer', 'interview_sheet_3_item_common',
          'interview_sheet_3_items_details', 'interview_sheet_4_question', 'interview_sheet_4_answer',
          'interview_sheet_4_item_common', 'interview_sheet_4_items_details', 'interview_sheet_5_question',
          'interview_sheet_5_answer', 'interview_sheet_5_item_common', 'interview_sheet_5_items_details',
          'interview_sheet_6_question', 'interview_sheet_6_answer', 'interview_sheet_6_item_common',
          'interview_sheet_6_items_details', 'interview_sheet_7_question', 'interview_sheet_7_answer',
          'interview_sheet_7_item_common', 'interview_sheet_7_items_details', 'interview_sheet_8_question',
          'interview_sheet_8_answer', 'interview_sheet_8_item_common', 'interview_sheet_8_items_details',
          'interview_sheet_9_question', 'interview_sheet_9_answer', 'interview_sheet_9_item_common',
          'interview_sheet_9_items_details', 'interview_sheet_10_question', 'interview_sheet_10_answer',
          'interview_sheet_10_item_common', 'interview_sheet_10_items_details', 'interview_sheet_11_question',
          'interview_sheet_11_answer', 'interview_sheet_11_item_common', 'interview_sheet_11_items_details',
          'interview_sheet_12_question', 'interview_sheet_12_answer', 'interview_sheet_12_item_common',
          'interview_sheet_12_items_details', 'interview_sheet_13_question', 'interview_sheet_13_answer',
          'interview_sheet_13_item_common', 'interview_sheet_13_items_details', 'interview_sheet_14_question',
          'interview_sheet_14_answer', 'interview_sheet_14_item_common', 'interview_sheet_14_items_details',
          'interview_sheet_15_question', 'interview_sheet_15_answer', 'interview_sheet_15_item_common',
          'interview_sheet_15_items_details', 'interview_sheet_16_question', 'interview_sheet_16_answer',
          'interview_sheet_16_item_common', 'interview_sheet_16_items_details', 'interview_sheet_17_question',
          'interview_sheet_17_answer', 'interview_sheet_17_item_common', 'interview_sheet_17_items_details',
          'interview_sheet_18_question', 'interview_sheet_18_answer', 'interview_sheet_18_item_common',
          'interview_sheet_18_items_details', 'interview_sheet_19_question', 'interview_sheet_19_answer',
          'interview_sheet_19_item_common', 'interview_sheet_19_items_details', 'interview_sheet_20_question',
          'interview_sheet_20_answer', 'interview_sheet_20_item_common', 'interview_sheet_20_items_details',
          'interview_sheet_21_question', 'interview_sheet_21_answer', 'interview_sheet_21_item_common',
          'interview_sheet_21_items_details', 'interview_sheet_22_question', 'interview_sheet_22_answer',
          'interview_sheet_22_item_common', 'interview_sheet_22_items_details', 'interview_sheet_23_question',
          'interview_sheet_23_answer', 'interview_sheet_23_item_common', 'interview_sheet_23_items_details',
          'interview_sheet_24_question', 'interview_sheet_24_answer', 'interview_sheet_24_item_common',
          'interview_sheet_24_items_details', 'others_plan_to_move', 'others_when_to_move1', 'others_when_to_move2',
          'others_moving_destination', 'others_other_requests']

df = spark.read.option("header", True).text(
    input_path)
first = df.rdd.first()
rdd = df.rdd.filter(lambda x: x != first).map(lambda x: transform_data(x))
df2 = spark.createDataFrame(rdd)
df2.select(schema).write.option("header", True).option("delimiter", ";").mode("overwrite") \
    .csv(output_path)
job.commit()
