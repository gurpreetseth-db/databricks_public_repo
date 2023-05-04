# Databricks notebook source
# MAGIC %md
# MAGIC ###DELETION VECTOR
# MAGIC Deletion vectors are a storage optimization feature that can be enabled on Delta Lake tables. By default, when a single row in a data file is deleted, the entire Parquet file containing the record must be rewritten. With deletion vectors enabled for the table, DELETE operations use deletion vectors to mark existing rows as removed without rewriting the Parquet file. Subsequent reads on the table resolve current table state by applying the deletions noted by deletion vectors to the most recent table version.
# MAGIC
# MAGIC #### Pre-Requisites
# MAGIC * Cluster with Databricks Run Time Version > 12.1 & Photon 
# MAGIC
# MAGIC #### Information
# MAGIC * This notebook will need 4 parameters i.e.
# MAGIC   * SCHEMA_NAME - Database that will be created for this demo
# MAGIC   * NON_DV_TABLE_NAME - Name for a Delta Table (without Deletion Vector)
# MAGIC   * DV_TABLE - Name for a Delta Table (With Deletion Vector Enabled)
# MAGIC   * INCREMENTAL_TABLE_NAME - Name for a Delta Table (With Incremental Datasets to act as MERGE Source)
# MAGIC
# MAGIC * Script will first Creare Schema, Tables and then Populate it with DUMMY Datasets
# MAGIC   * Both Non-DV & DV tables have CDC enabled to showcase record count based on operation (Insert, Delete, Update Pre, Update Post)
# MAGIC   * Post Initial Insert both Non-DV and DV Table will be having 100000000 records, Incremental Table will be having 101000 records
# MAGIC   * Non-DV and DV Tables will be having name column with value starting as "Company"
# MAGIC   * Incremental Table will be having name column with value starting as "Organization"
# MAGIC
# MAGIC * A MERGE function will be created to perform MERGE Test for both Non-DV and DV table
# MAGIC * MERGE on Non-DV enabled Table will re-write the whole files and will take longer time to run (depending on cluster size)
# MAGIC * MERGER on DV enabled Table will only write 2 extra Parquet Files and 1 Deletion Vector Bin file and will take 40-50% lesser time to execute
# MAGIC
# MAGIC More about Deletion Vector : https://docs.databricks.com/delta/deletion-vectors.html# 

# COMMAND ----------

# DBTITLE 1,Schema and Table Input Parameters
from pyspark.sql.functions import lit, col, concat, expr
from delta import DeltaTable

# Preparation step:
# We create a fake production data set to serve as an example.

# Widgets for Schema and Table Names
dbutils.widgets.text("SCHEMA_NAME", "deletion_vectors_example_database")
dbutils.widgets.text("NON_DV_TABLE_NAME", "non_dv_example")
dbutils.widgets.text("DV_TABLE_NAME", "dv_example")
dbutils.widgets.text("INCREMENTAL_TABLE_NAME", "incremental_table")

SCHEMA = dbutils.widgets.get("SCHEMA_NAME")
NORMAL_TABLE = dbutils.widgets.get("NON_DV_TABLE_NAME")
DV_TABLE = dbutils.widgets.get("DV_TABLE_NAME")
INCREMETAL_TABLE = "." + dbutils.widgets.get("INCREMENTAL_TABLE_NAME")

# COMMAND ----------

# DBTITLE 1,Prepare Environment - Database/Schema, Tables and Populate Data
SCALE_FACTOR = 100 * 1000 * 1000

# CREATE A DATABASE
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE {SCHEMA}")


# DROP TABLE IF ALREADY EXISTS
spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.{NORMAL_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.{DV_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}{INCREMETAL_TABLE}")

# POPULATE NON DELETION VECTOR TABLE
df_nondv_table = (
    spark.range(0, SCALE_FACTOR, 1, 10)
    .toDF("id")
    .withColumn("name", concat(lit("Company "), col("id")))
)

df_nondv_table.write.option("database", SCHEMA).option(
    "delta.enableChangeDataFeed", True
).saveAsTable(NORMAL_TABLE)

# POPULATE DELETION VECTOR TABLE
df_dv_table = (
    spark.range(0, SCALE_FACTOR, 1, 10)
    .toDF("id")
    .withColumn("name", concat(lit("Company "), col("id")))
)

df_dv_table.write.option("database", SCHEMA).option(
    "delta.enableDeletionVectors", True
).option("delta.enableChangeDataFeed", True).saveAsTable(DV_TABLE)

# POPULATE TABLE FOR INCREMENTAL DATA
spark.range(SCALE_FACTOR, SCALE_FACTOR + 1000).toDF("id").union(
    spark.range(0, SCALE_FACTOR).toDF("id").where("id % 1000 = 42")
).withColumn("name", concat(lit("Organization "), col("id"))).write.saveAsTable(
    SCHEMA + INCREMETAL_TABLE
)

# COMMAND ----------

# DBTITLE 1,VALIDATE RECORD COUNTS IN TABLES
dftbl = sqlContext.sql("show tables")
dfdbs = sqlContext.sql(f"Select '{SCHEMA}' as databaseName ")
dftbls = "0"
for row in dfdbs.rdd.collect():
    tmp = "show tables from " + row["databaseName"]
    dftbls = sqlContext.sql(tmp)

tmplist = []
for row in dftbls.rdd.collect():
    try:
        tmp = (
            "select count(*) myrowcnt from " + row["database"] + "." + row["tableName"]
        )
        tmpdf = sqlContext.sql(tmp)
        myrowcnt = tmpdf.collect()[0]["myrowcnt"]
        tmplist.append((row["database"], row["tableName"], myrowcnt))
    except:
        tmplist.append((row["database"], row["tableName"], -1))

columns = ["database", "tableName", "rowCount"]
df = spark.createDataFrame(tmplist, columns)
display(df)

# COMMAND ----------

# DBTITLE 1,CREATE SQL MERGER FUNCTION - add specific conditions to UPDATE & DELETE Data
# CREATE A SQL MERGE FUNCTION

def sql_merge(Source_Table, Target_Table):
    target_table = {Target_Table}
    spark.sql(
        f"""
    MERGE INTO {Target_Table} t
    USING {Source_Table} s
    ON t.id = s.id
    WHEN MATCHED AND t.id BETWEEN 9000041 AND 9000655 THEN DELETE
    WHEN MATCHED AND t.id NOT BETWEEN 9000041 AND 9000655 THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
  """
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform MERGE on NON DELETION VECTOR TABLE
# MAGIC Note down the time it took - Later compare it with Deletion Vector enabled table, **CMD 11**

# COMMAND ----------

# DBTITLE 1,Perform MERGE on NON DELETION VECTOR TABLE
Source_Table = SCHEMA + INCREMETAL_TABLE
Target_Table = SCHEMA + "." + NORMAL_TABLE

sql_merge(Source_Table, Target_Table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check operationMetrics Column
# MAGIC #### We will see 2 Version
# MAGIC  **Version 0** - Initial CREATE TABLE  - Which must have added around 10 files 
# MAGIC  
# MAGIC  **Version 1** - MERGE Statement - Will remove existing 10 files and re-write new 10-20 new files which is the default behavior and is expensive as every change need to re-write complete dataset.
# MAGIC
# MAGIC **Deleted Records will be  : 1 (Id = 9000042 )**
# MAGIC
# MAGIC **Inserted Records will be : 1000**
# MAGIC  
# MAGIC **Updated Records will be  : 99999**
# MAGIC  
# MAGIC
# MAGIC  

# COMMAND ----------

# DBTITLE 1,SHOW History of the table - check operationMetrics column
df = spark.sql(f"DESCRIBE HISTORY {Target_Table}")
display(df)

# COMMAND ----------

# DBTITLE 1,Count of Record Group By CHANGE TYPE (Insert, Update Pre/Post & Delete)
df = spark.sql(f"SELECT _change_type,count(*) FROM table_changes('{Target_Table}',1) Group By _change_type")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform MERGE on DELETION VECTOR TABLE
# MAGIC Note down the time it took - Which will certainly be 40-50% lesser than Non-Deletion Vector Enabled Cluster from above execution, **CMD 7**

# COMMAND ----------

Source_Table = SCHEMA + INCREMETAL_TABLE
Target_Table = SCHEMA + "." + DV_TABLE

sql_merge(Source_Table, Target_Table)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check operationMetrics Column
# MAGIC #### We will see 2 Version
# MAGIC  **Version 0** - Initial CREATE TABLE  - Which must have added around 10 files 
# MAGIC  
# MAGIC  **Version 1** - MERGE Statement - Will add new 2 new files(for Updated Records & New Records). This time it has not re-written whole dataset and that is what Deletion Vector brings on the table.
# MAGIC
# MAGIC **Deleted Records will be  : 1 (Id = 9000042 )**
# MAGIC
# MAGIC **Inserted Records will be : 1000**
# MAGIC  
# MAGIC **Updated Records will be  : 99999**

# COMMAND ----------

# DBTITLE 1,SHOW History of the table - check operationMetrics column
df = spark.sql(f"DESCRIBE HISTORY {Target_Table}")
display(df)

# COMMAND ----------

# DBTITLE 1,Count of Record Group By CHANGE TYPE (Insert, Update Pre/Post & Delete)
df = spark.sql(
    f"SELECT _change_type,count(*) FROM table_changes('{Target_Table}',1) Group By _change_type"
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lets review what consist of this DeletionVector enabled table
# MAGIC #### We will see around 12 Parquet files and 1 bin file
# MAGIC   * BIN file has information about records which got DELETED
# MAGIC   * One of the Parquet File will be having CHANGED records
# MAGIC   * One of the Parquet File will be having NEWLY INSERTED records

# COMMAND ----------

df = dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/dv_example")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Lets Review Delta Log Files - We should have 2 of them
# MAGIC * 00000000000000000000.json - From initial table creation operation (with 10 Parquet files)
# MAGIC * 00000000000000000001.json - From MERGE operation (2 new Parquet files)

# COMMAND ----------

df = dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/_delta_log")
display(df)

# COMMAND ----------

# DBTITLE 1,First VERSION of file will show 10 parquet files from initial write
display(spark.read.json(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/_delta_log/00000000000000000000.json").where("add is not null").select("add.path"))

# COMMAND ----------

# DBTITLE 1,Second VERSION of file will show 12 parquet files (10 from initial writes and 2 new)
display(spark.read.json(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/_delta_log/00000000000000000001.json").where("add is not null").select("add.path"))

# COMMAND ----------

# DBTITLE 1,Code will read Version 0 JSON file and fetch 10th file and show its contents which are all UPDATED records = 99999
import pyspark
from pyspark.sql import Row

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

df = spark.read.json(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/_delta_log/00000000000000000001.json").where("add is not null").select("add.path")
x = df.collect()[10].__getitem__('path')
display(spark.read.parquet(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/{x}").count())

# COMMAND ----------

# DBTITLE 1,Code will read Version 1 JSON file and fetch 11th file and show its contents which are all NEWLY inserted records = 1000
import pyspark
from pyspark.sql import Row

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

df = spark.read.json(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/_delta_log/00000000000000000001.json").where("add is not null").select("add.path")
x = df.collect()[11].__getitem__('path')
display(spark.read.parquet(f"dbfs:/user/hive/warehouse/{SCHEMA}.db/{DV_TABLE}/{x}").count())
