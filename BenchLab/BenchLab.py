# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <img src="https://i.imgur.com/SJy1UpK.png" width="30%">
# MAGIC 
# MAGIC Benchmark your workload on different cluster specs and spark configs
# MAGIC 
# MAGIC Join up with cost data to tune cost/performance of your workload 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <a href="$./BenchRunner">BenchRunner</a>
# MAGIC 
# MAGIC 
# MAGIC * Read from the RunsConfig table
# MAGIC * Run all runs that do not have a runId via the REST API 
# MAGIC * Execute one at a time, wait 1 min between checks
# MAGIC 
# MAGIC * When job completes then
# MAGIC * Read from RunsHistorical table
# MAGIC * Query REST API Runs to see if run has completed
# MAGIC * Merge result data into RunsHistorical Table
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC <a href="https://e2-demo-west.cloud.databricks.com/sql/dashboards/88866f42-7bc9-453f-9288-d3fa39889041-data-profiling---table-overview?o=2556758628403379&p_Table%20Name=web_sales">BenchDash</a>
# MAGIC * Show basic performance graph of runs 
# MAGIC * Enrich data with cloud costs 
# MAGIC * show workload price performance 

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Create Database ##

# COMMAND ----------

spark.sql("create database if not exists BenchLab;")
spark.sql("use BenchLab; ")
benchbase = "BenchLab"

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace table RunsConfig(
# MAGIC experimentId string,
# MAGIC notebookPath string, 
# MAGIC sparkVersion string, 
# MAGIC driverInstanceType string, 
# MAGIC workerInstanceType string, 
# MAGIC elasticDisk string, 
# MAGIC numWorkers string, 
# MAGIC spark_conf string,
# MAGIC notebookParams string, 
# MAGIC runId int
# MAGIC )
# MAGIC USING DELTA; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC insert into RunsConfig 
# MAGIC select 
# MAGIC concat(uuid(),nodes)
# MAGIC ,"/Users/franco.patano@databricks.com/New SQL Analytics/Create Data Profile"
# MAGIC , "7.3.x-scala2.12"
# MAGIC , instance_type_id
# MAGIC , instance_type_id
# MAGIC , "true"
# MAGIC , nodes
# MAGIC , '"spark.sql.join.prefersortmergejoin": "false",
# MAGIC         "spark.sql.adaptive.coalescePartitions.enabled": "true",
# MAGIC         "spark.sql.adaptive.localShuffleReader.enabled": "true",
# MAGIC         "spark.sql.adaptive.skewJoin.enabled": "true",
# MAGIC         "spark.sql.adaptive.enabled": "true",
# MAGIC         "spark.sql.autoBroadcastJoinThreshold": "20971520",
# MAGIC         "spark.databricks.io.cache.enabled": "true"'
# MAGIC , '"SelectDatabase":"sql_analytics_demo_singlenodebench"'
# MAGIC ,NULL
# MAGIC 
# MAGIC from instanceTypes
# MAGIC LATERAL VIEW EXPLODE(ARRAY(2,4,6)) as nodes
# MAGIC 
# MAGIC where category = 'Compute Optimized'
# MAGIC  and instance_type_id like 'c5d.%'
# MAGIC  and num_cores < 32

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select 
# MAGIC uuid()
# MAGIC ,"/Users/franco.patano@databricks.com/New SQL Analytics/Create Data Profile"
# MAGIC , "7.3.x-scala2.12"
# MAGIC , instance_type_id
# MAGIC , instance_type_id
# MAGIC , "true"
# MAGIC , explode(array(2,4,6))
# MAGIC , '"spark.sql.join.prefersortmergejoin": "false",
# MAGIC         "spark.sql.adaptive.coalescePartitions.enabled": "true",
# MAGIC         "spark.sql.adaptive.localShuffleReader.enabled": "true",
# MAGIC         "spark.sql.adaptive.skewJoin.enabled": "true",
# MAGIC         "spark.sql.adaptive.enabled": "true",
# MAGIC         "spark.sql.autoBroadcastJoinThreshold": "20971520",
# MAGIC         "spark.databricks.io.cache.enabled": "true"'
# MAGIC , '"SelectDatabase":"sql_analytics_demo_singlenodebench"'
# MAGIC ,NULL
# MAGIC 
# MAGIC from instanceTypes
# MAGIC where category = 'Compute Optimized'
# MAGIC  and instance_type_id like 'c5d.%'
# MAGIC  and num_cores < 32

# COMMAND ----------

# MAGIC %sql optimize RunsConfig zorder by experimentId

# COMMAND ----------

# MAGIC %sql select * from RunsConfig

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Connection ##

# COMMAND ----------

# DBTITLE 1,Create Connection
# MAGIC %python
# MAGIC import requests
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC import json
# MAGIC 
# MAGIC #DOMAIN = 'https://e2-demo-west.cloud.databricks.com'
# MAGIC API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
# MAGIC TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

response = requests.get(API_URL+'/api/2.0/clusters/list-node-types',
headers={'Authorization': "Bearer " + TOKEN})

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Query and Create InstanceType Table #

# COMMAND ----------

from pandas.io.json import json_normalize

json = response.json()
## Convert from JSON to pandas dataframe
pandas_df = json_normalize(json)

## Convert from pandas to Spark DataFrame
sparkDF = spark.createDataFrame(pandas_df)

sparkDF.createOrReplaceTempView("responseSQL")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table instanceTypes 
# MAGIC USING DELTA
# MAGIC as
# MAGIC select
# MAGIC   instance_type_id,
# MAGIC   memory_mb,
# MAGIC   num_gpus,
# MAGIC   num_cores,
# MAGIC   is_io_cache_enabled,
# MAGIC   category,
# MAGIC   node_instance_type,
# MAGIC   memory_mb / num_cores as memoryToCoreRatio
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       response.instance_type_id,
# MAGIC       response.memory_mb,
# MAGIC       response.num_gpus,
# MAGIC       response.num_cores,
# MAGIC       response.is_io_cache_enabled,
# MAGIC       response.category,
# MAGIC       response.node_instance_type,
# MAGIC       explode(*) as response
# MAGIC     from
# MAGIC       responseSQL
# MAGIC   )
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from benchlab.instanceTypes
# MAGIC where category = 'Compute Optimized'
# MAGIC  and instance_type_id like 'c5d.%'
# MAGIC  and num_cores < 32

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Get Runs from Config to send # 

# COMMAND ----------



tablesDF = spark.sql(f"""
select * from {benchbase}.RunsConfig where runId is null limit 1
""")
notebookPath = tablesDF.select(collect_list("notebookPath")).collect()[0][0]
sparkVersion = tablesDF.select(collect_list("sparkVersion")).collect()[0][0]
driverInstanceType = tablesDF.select(collect_list("driverInstanceType")).collect()[0][0]
workerInstanceType = tablesDF.select(collect_list("workerInstanceType")).collect()[0][0]
elasticDisk = tablesDF.select(collect_list("elasticDisk")).collect()[0][0]
numWorkers = tablesDF.select(collect_list("numWorkers")).collect()[0][0]
spark_conf = tablesDF.select(collect_list("spark_conf")).collect()[0][0]
notebookParams = tablesDF.select(collect_list("notebookParams")).collect()[0][0]
experimentId = tablesDF.select(collect_list("experimentId")).collect()[0][0]

notebookPath = notebookPath[0] 
sparkVersion = sparkVersion[0]
driverInstanceType = driverInstanceType[0]
workerInstanceType = workerInstanceType[0]
elasticDisk = elasticDisk[0]
numWorkers = numWorkers[0]
spark_conf = spark_conf[0]
notebookParams = notebookParams[0]
experimentId = experimentId[0]


# COMMAND ----------

import json

notebookParams = json.loads('{'+notebookParams+'}')
spark_conf = json.loads('{'+spark_conf+'}')



# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC # Send Run to Databricks Jobs Service to run now #

# COMMAND ----------

## Switching to create job endpoint for this request
create_job_endpoint = '/api/2.0/jobs/runs/submit'


## Post the request 
response = requests.post(
  API_URL + create_job_endpoint,
  headers={'Authorization': 'Bearer ' + TOKEN},
  json={
        "run_name": f"benchlab:{experimentId}",
        "new_cluster": {
          "spark_version": f"{sparkVersion}",
          "node_type_id": f"{driverInstanceType}",
          "enable_elastic_disk": f"{elasticDisk}",
          "spark_conf": spark_conf ,
          "aws_attributes": {
            "availability": "SPOT_WITH_FALLBACK"
          },
          "num_workers": f"{numWorkers}"
        },
        "notebook_task": {
          "notebook_path": f"{notebookPath}",
          "base_parameters": notebookParams
        }
      }
 )
response.text


# COMMAND ----------

# Get Runid from the response, and update the table with the run id


jobsRunId = response.json()['run_id']

spark.sql(f"UPDATE RunsConfig SET runId = {jobsRunId} where experimentId = {experimentId}")

# need to build in error handeling 



# COMMAND ----------

jobsRunId = 83

# COMMAND ----------



# COMMAND ----------

create_job_endpoint = f'/api/2.0/jobs/runs/get?run_id={jobsRunId}'

response = requests.get(
  API_URL + create_job_endpoint,
  headers={'Authorization': 'Bearer ' + TOKEN}
)

# COMMAND ----------

response.text

# COMMAND ----------

response.json()

# COMMAND ----------

json_data = json.dumps(response.json())

# COMMAND ----------

json_data


# COMMAND ----------

rdd_results = sc.parallelize([json_data])
bronze_df = spark.read.json(rdd_results)

# COMMAND ----------

bronze_df.createOrReplaceTempView("RunsGet")

# COMMAND ----------

# MAGIC %sql describe RunsGet

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from RunsGet

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table runsActual 
# MAGIC using delta as 
# MAGIC 
# MAGIC select
# MAGIC   run_id,
# MAGIC   run_type,
# MAGIC   job_id,
# MAGIC   setup_duration,
# MAGIC   execution_duration,
# MAGIC   start_time,
# MAGIC   state.life_cycle_state,
# MAGIC   state.result_state,
# MAGIC   state.state_message,
# MAGIC   cluster_instance.cluster_id,
# MAGIC   cluster_instance.spark_context_id,
# MAGIC   number_in_job,
# MAGIC   cluster_spec.new_cluster.enable_elastic_disk,
# MAGIC   cluster_spec.new_cluster.node_type_id,
# MAGIC   cluster_spec.new_cluster.spark_version,
# MAGIC   cluster_spec.new_cluster.num_workers,
# MAGIC   cluster_spec.new_cluster.aws_attributes.availability,
# MAGIC   cluster_spec.new_cluster.aws_attributes.zone_id,
# MAGIC   creator_user_name,
# MAGIC   task.notebook_task.notebook_path,
# MAGIC   run_name,
# MAGIC   run_page_url
# MAGIC from
# MAGIC   RunsGet

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select * from runsActual

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC describe instancetypes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC optimize instancetypes zorder by instance_type_id

# COMMAND ----------

