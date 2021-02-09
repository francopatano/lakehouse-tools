# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Bench Runner #
# MAGIC 
# MAGIC Bench runner will grab all the rows in the runs config table that do not have a run id updated. 
# MAGIC 
# MAGIC Then it will send an API command to run it with the configs set. 
# MAGIC 
# MAGIC Finally it will update the row for the run with the run id. 

# COMMAND ----------

spark.sql("create database if not exists BenchLab;")
spark.sql("use BenchLab; ")
benchbase = "BenchLab"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Select the list of runs that have not been submitted from the Runs Config table

# COMMAND ----------

# MAGIC %python
# MAGIC import requests
# MAGIC from pyspark.sql.types import *
# MAGIC from pyspark.sql.functions import *
# MAGIC import json
# MAGIC import time
# MAGIC 
# MAGIC #DOMAIN = 'https://e2-demo-west.cloud.databricks.com'
# MAGIC API_URL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
# MAGIC TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Loop through the list of run configs and send them to the jobs run now api
# MAGIC * after successful loop, query for the run id and write it back to the runs config table

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from benchlab.RunsConfig where runId is null

# COMMAND ----------

runsToSend = list(spark.sql(f"""
select * from {benchbase}.RunsConfig where runId is null
""").toPandas()['experimentId'])


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from runsactual

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from runsactual
# MAGIC where result_state = 'FAILED'; 
# MAGIC 
# MAGIC optimize runsactual; 

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW tempResponseProcessing as SELECT * FROM ResponseProcessing WHERE 1=2

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW persistedRunsGet as SELECT * FROM runsActual WHERE 1=2

# COMMAND ----------


for experiment in runsToSend: 
  notebookPath, sparkVersion,driverInstanceType, workerInstanceType, elasticDisk, numWorkers, spark_conf, notebookParams, experimentId  = spark.sql(f"""
  select notebookPath,sparkVersion, driverInstanceType, workerInstanceType, elasticDisk, numWorkers, spark_conf, notebookParams, experimentId from {benchbase}.RunsConfig where experimentId = '{experiment}'
  """).collect()[0]
  
  notebookParams = json.loads('{'+notebookParams+'}')
  spark_conf = json.loads('{'+spark_conf+'}')
  
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
  
  jobsRunId = response.json()['run_id']
  
  jobIsCompleted = True
  while jobIsCompleted:
    create_job_endpoint = f'/api/2.0/jobs/runs/get?run_id={jobsRunId}'
    response = requests.get(API_URL + create_job_endpoint, headers={'Authorization': 'Bearer ' + TOKEN})
    json_data = json.dumps(response.json())
    rdd_results = sc.parallelize([json_data])
    bronze_df = spark.read.json(rdd_results)
    bronze_df = bronze_df.filter(col('state.life_cycle_state') == 'TERMINATED')
    bronze_df.createOrReplaceTempView("RunsGet")
    if bronze_df.filter(col('state.life_cycle_state') == 'TERMINATED').take(1):
      jobIsCompleted = False
      spark.sql("""
      CREATE OR REPLACE TEMP VIEW persistedRunsGet as 
      SELECT * FROM persistedRunsGet UNION
      select
        run_id,
        run_type,
        job_id,
        setup_duration,
        execution_duration,
        start_time,
        state.life_cycle_state,
        state.result_state,
        state.state_message,
        cluster_instance.cluster_id,
        cluster_instance.spark_context_id,
        number_in_job,
        cluster_spec.new_cluster.enable_elastic_disk,
        cluster_spec.new_cluster.node_type_id,
        cluster_spec.new_cluster.spark_version,
        cluster_spec.new_cluster.num_workers,
        cluster_spec.new_cluster.aws_attributes.availability,
        cluster_spec.new_cluster.aws_attributes.zone_id,
        creator_user_name,
        task.notebook_task.notebook_path,
        run_name,
        run_page_url
      from
        RunsGet
      """)
    else:
      print("sleeping for 1 minute... ")
      time.sleep(60)
    
  spark.sql(f"""
  CREATE OR REPLACE TEMP VIEW tempResponseProcessing as 
  SELECT * FROM tempResponseProcessing UNION
  select '{experiment}', {jobsRunId}
   """)
  
sql = """
INSERT INTO runsActual 
SELECT * FROM persistedRunsGet
"""
spark.sql(sql)
spark.sql("optimize runsActual")
    
spark.sql(f"""
MERGE INTO RunsConfig r
USING tempResponseProcessing t on r.experimentId = t.experimentId
WHEN MATCHED THEN UPDATE SET r.runId = t.runId
""")

spark.sql("OPTIMIZE RunsConfig")

spark.sql("DROP TABLE tempResponseProcessing")
spark.sql("DROP TABLE persistedRunsGet")


# COMMAND ----------

# %sql 

# MERGE INTO RunsConfig r
# USING tempResponseProcessing t on r.experimentId = t.experimentId
# WHEN MATCHED THEN UPDATE SET r.runId = t.runId


# COMMAND ----------



# COMMAND ----------

  spark.sql("""
  CREATE OR REPLACE TEMP VIEW persistedRunsGet as 
  
  SELECT * FROM persistedRunsGet UNION
  
  select
    run_id,
    run_type,
    job_id,
    setup_duration,
    execution_duration,
    start_time,
    state.life_cycle_state,
    state.result_state,
    state.state_message,
    cluster_instance.cluster_id,
    cluster_instance.spark_context_id,
    number_in_job,
    cluster_spec.new_cluster.enable_elastic_disk,
    cluster_spec.new_cluster.node_type_id,
    cluster_spec.new_cluster.spark_version,
    cluster_spec.new_cluster.num_workers,
    cluster_spec.new_cluster.aws_attributes.availability,
    cluster_spec.new_cluster.aws_attributes.zone_id,
    creator_user_name,
    task.notebook_task.notebook_path,
    run_name,
    run_page_url
  from
    RunsGet
  """)

sql = """
INSERT INTO runsActual 
SELECT * FROM persistedRunsGet
"""
spark.sql(sql)
spark.sql("optimize runsActual")

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# %sql 

# create or replace table ResponseProcessing (experimentId string, runId int) USING DELTA; 

# COMMAND ----------

dbutils.widgets.help()
