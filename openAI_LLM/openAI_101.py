# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks SQL AI Functions
# MAGIC
# MAGIC AI Functions are built-in Databricks SQL functions, allowing you to access Large Language Models (LLMs) directly from SQL.
# MAGIC
# MAGIC Popular LLMs such as the one provided by OpenAI APIs let you apply all sort of transformations on top of text, from classification, information extraction to automatic answers.
# MAGIC
# MAGIC Leveraging Databricks SQL AI functions `AI_GENERATE_TEXT()`, you can now apply these transformations and experiment with LLMs on your data from within a familiar SQL interface. 
# MAGIC
# MAGIC Once you have developed the correct LLM prompt, you can quickly turn that into a production pipeline using existing Databricks tools such as Delta Live Tables or scheduled Jobs. This greatly simplifies both the development and productionization workflow for LLMs.
# MAGIC
# MAGIC AI Functions abstracts away the technical complexities of calling LLMs, enabling analysts and data scientists to start using these models without worrying about the underlying infrastructure.
# MAGIC
# MAGIC ## Increasing customer satisfaction and churn reduction with automatic reviews analysis
# MAGIC
# MAGIC In this demo, we'll build a data pipeline that takes customer reviews, in the form of freeform text, and enrich them with meaning derived by asking natural language questions of Azure OpenAI's GPT-3.5 Turbo model. We'll even provide recommendations for next best actions to our customer service team - i.e. whether a customer requires follow-up, and a sample message to follow-up with
# MAGIC
# MAGIC For each review, we:
# MAGIC - Determine sentiment and whether a response is required back to the customer
# MAGIC - Generate a response mentioning alternative products that may satisfy the customer
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-flow.png" width="1200">
# MAGIC
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-review.png" width="1200">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2FDBSQL%2Fsql-ai-functions%2F01-SQL-AI-Functions-Introduction&cid=local&uid=local">

# COMMAND ----------

# MAGIC %md
# MAGIC # PRE-REQUISITES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve your Azure OpenAI details
# MAGIC
# MAGIC To be able to call our AI function, we'll need a key to query the API. In this demo, we'll be using Azure OpenAI. Here are the steps to retrieve your key:
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-setup-1.png" width="1250px">
# MAGIC
# MAGIC - Navigate to [Azure Portal > All Services > Cognitive Services > Azure OpenAI](https://portal.azure.com/#view/Microsoft_Azure_ProjectOxford/CognitiveServicesHub/~/OpenAI)  
# MAGIC - Click into the relevant resource
# MAGIC - Click on `Keys and Endpoint`
# MAGIC   - Copy down your:
# MAGIC     - **Key** (do **NOT** store this in plaintext, include it in your code, and/or commit it into your git repo)
# MAGIC     - **Resource name** which is represented in the endpoint URL (of the form `https://<resource-name>.openai.azure.com/`)
# MAGIC - Click on `Model deployments`
# MAGIC   - Note down your **model deployment name**
# MAGIC
# MAGIC When you use `AI_GENERATE_TEXT()` these values will map to the `deploymentName` and `resourceName` parameters. Here is the function signature:
# MAGIC &nbsp;
# MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-function-setup-2.png" width="1000px">
# MAGIC
# MAGIC <!-- Collect usage data (view). Remove it to disable collection. View README for more details.  -->
# MAGIC <img width="1px" src="https://www.google-analytics.com/collect?v=1&gtm=GTM-NKQ8TT7&tid=UA-163989034-1&aip=1&t=event&ec=dbdemos&ea=VIEW&dp=%2F_dbdemos%2FDBSQL%2Fsql-ai-functions%2F02-Create-OpenAI-model-and-store-secrets&cid=local&uid=local">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store your key using Databricks secrets
# MAGIC
# MAGIC We'll use [Databricks secrets](https://docs.databricks.com/security/secrets/index.html) to hold our API tokens. Use the [Databricks Secrets CLI](https://docs.databricks.com/dev-tools/cli/secrets-cli.html) or [Secrets API 2.0](https://docs.databricks.com/dev-tools/api/latest/secrets.html) to manage your secrets. The below examples use the Secrets CLI
# MAGIC
# MAGIC - If you don't already have a secret scope to keep your OpenAI keys in, create one now: 
# MAGIC   `databricks secrets create-scope --scope <<SCOPE NAME>>`
# MAGIC - You will need to give `READ` or higher access for principals (e.g. users, groups) who are allowed to connect to OpenAI. 
# MAGIC   - We recommend creating a group `openai-users` and adding permitted users to that group
# MAGIC   - Then give that group `READ` (or higher) permission to the scope: 
# MAGIC     `databricks secrets put-acl --scope <<SCOPE NAME>> --principal openai-users --permission READ`
# MAGIC - Create a secret for your API access token. We recommend format `<resource-name>-key`: 
# MAGIC   `databricks secrets put --scope <<SCOPE NAME>> --key <<OPEN_AI_KEY>>`
# MAGIC
# MAGIC **Caution**: Do **NOT** include your token in plain text in your Notebook, code, or git repo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Sample Data
# MAGIC
# MAGIC - We'will use [Amazons's Customer Review Dataset](https://s3.amazonaws.com/amazon-reviews-pds/readme.html)
# MAGIC - We assume your cluster has the necessary[instance profile](https://docs.databricks.com/storage/amazon-s3.html#access-s3-buckets-using-instance-profiles) or [configuration](https://docs.databricks.com/storage/amazon-s3.html#configuration) to read from Amazon's public S3 bucket

# COMMAND ----------

CATALOG = "main"
SCHEMA = "openai_experimentation"
TABLE_RAW_REVIEWS = "raw_amazon_reviews_pds"
SEED = "123456"


dbutils.widgets.removeAll()
dbutils.widgets.text("catalog", CATALOG, "Catalog")
dbutils.widgets.text("schema", SCHEMA, "Schema")
dbutils.widgets.text("raw_reviews_table", TABLE_RAW_REVIEWS, "Target table for Review Data")
dbutils.widgets.text("seed", SEED, "Random seed for reproducibility")


# COMMAND ----------

"%sql
USE CATALOG ${catalog};
CREATE SCHEMA IF NOT EXISTS ${schema};
USE SCHEMA ${schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS $raw_reviews_table
# MAGIC COMMENT "Raw data: customer reviews";
# MAGIC
# MAGIC COPY INTO $raw_reviews_table
# MAGIC FROM "s3://amazon-reviews-pds/parquet/"
# MAGIC FILEFORMAT = PARQUET
# MAGIC FORMAT_OPTIONS ('inferSchema' = 'true')
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC
# MAGIC OPTIMIZE $raw_reviews_table;
# MAGIC ANALYZE TABLE $raw_reviews_table COMPUTE STATISTICS;

# COMMAND ----------

# DBTITLE 1,View subset of data
# MAGIC %sql
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   $ raw_review_table
# MAGIC WHERE
# MAGIC   product_category = 'Grocery'
# MAGIC LIMIT
# MAGIC   10

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # NOTE: Run All Below Scripts in DBSQL Pro or Serverless

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Azure OpenAI: Calling ai_generate_text()
# MAGIC
# MAGIC ```
# MAGIC AI_GENERATE_TEXT(prompt,
# MAGIC   "azure_openai/gpt-35-turbo",
# MAGIC   "apiKey", SECRET("SCOPE", "SECRET"),
# MAGIC   "temperature", CAST(0.0 AS DOUBLE),
# MAGIC   "deploymentName", "llmbricks",
# MAGIC   "apiVersion", "2023-03-15-preview",  
# MAGIC   "resourceName", "llmbricks"
# MAGIC   
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AI_GENERATE_TEXT("Classify product categories + one-sentence description: Tim Tams, Vegemite, Cadbury",
# MAGIC   "azure_openai/gpt-35-turbo",
# MAGIC   "apiKey", SECRET("SCOPE NAME", "OPEN API KEY VALUE"),
# MAGIC   "temperature", CAST(0.0 AS DOUBLE),
# MAGIC   "deploymentName", "dbdemos-open-ai",
# MAGIC   "apiVersion", "2023-03-15-preview",  
# MAGIC   "resourceName", "dbdemos-open-ai") as prompt_response

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt Design
# MAGIC The keys to getting useful results back from a GPT model are:
# MAGIC - Asking it a well-formed question
# MAGIC - Being specific about the type of answer that you are expecting
# MAGIC
# MAGIC In order to get results in a form that we can easily store in a table, we'll ask the model to return the result in a string that reflects `JSON` representation, and be very specific of the schema that we expect
# MAGIC
# MAGIC Here's the prompt we've settled on:
# MAGIC ```
# MAGIC A customer left a review. We follow up with anyone who appears unhappy.
# MAGIC Extract all entities mentioned. For each entity:
# MAGIC - classify sentiment as ["POSITIVE","NEUTRAL","NEGATIVE"]
# MAGIC - whether customer requires a follow-up: Y or N
# MAGIC - reason for requiring followup
# MAGIC
# MAGIC Return JSON ONLY. No other text outside the JSON. JSON format:
# MAGIC {
# MAGIC entities: [{
# MAGIC     "entity_name": <entity name>,
# MAGIC     "entity_type": <entity type>,
# MAGIC     "entity_sentiment": <entity sentiment>,
# MAGIC     "followup": <Y or N for follow up>,
# MAGIC     "followup_reason": <reason for followup>
# MAGIC }]
# MAGIC }
# MAGIC
# MAGIC Review:
# MAGIC <insert review text here>
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Functions
# MAGIC
# MAGIC - We'll create SQL functions in order to abstract away the details of the `AI_GENERATE_TEXT()` call from the end users

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Wrapper function to handle all our calls to Azure OpenAI
# MAGIC -- Analysts who want to use arbitrary prompts can use this handler
# MAGIC CREATE OR REPLACE FUNCTION PROMPT_HANDLER(prompt STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN AI_GENERATE_TEXT(prompt,
# MAGIC   "azure_openai/gpt-35-turbo",
# MAGIC   "apiKey", SECRET("SCOPE NAME", "OPEN API KEY VALUE"),
# MAGIC   "temperature", CAST(0.0 AS DOUBLE),
# MAGIC   "deploymentName", "llmbricks",
# MAGIC   "apiVersion", "2023-03-15-preview",  
# MAGIC   "resourceName", "llmbricks"
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Extracts entities, entity sentiment, and whether follow-up is required from a customer review
# MAGIC -- Since we're receiving a well-formed JSON, we can parse it and return a STRUCT data type for easier querying downstream
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ANNOTATE_REVIEW(review STRING)
# MAGIC RETURNS STRUCT<entities: ARRAY<STRUCT<entity_name: STRING, entity_type: STRING, entity_sentiment: STRING, followup: STRING, followup_reason: STRING>>>
# MAGIC RETURN FROM_JSON(
# MAGIC   PROMPT_HANDLER(CONCAT(
# MAGIC     'A customer left a review. We follow up with anyone who appears unhappy.
# MAGIC      Extract all entities mentioned. For each entity:
# MAGIC       - classify sentiment as ["POSITIVE","NEUTRAL","NEGATIVE"]
# MAGIC       - whether customer requires a follow-up: Y or N
# MAGIC       - reason for requiring followup
# MAGIC
# MAGIC     Return JSON ONLY. No other text outside the JSON. JSON format:
# MAGIC     {
# MAGIC         entities: [{
# MAGIC             "entity_name": <entity name>,
# MAGIC             "entity_type": <entity type>,
# MAGIC             "entity_sentiment": <entity sentiment>,
# MAGIC             "followup": <Y or N for follow up>,
# MAGIC             "followup_reason": <reason for followup>
# MAGIC         }]
# MAGIC     }
# MAGIC
# MAGIC     Review:
# MAGIC     ', review)),
# MAGIC   "STRUCT<entities: ARRAY<STRUCT<entity_name: STRING, entity_type: STRING, entity_sentiment: STRING, followup: STRING, followup_reason: STRING>>>"
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Generate a response to a customer based on their complaint
# MAGIC CREATE OR REPLACE FUNCTION GENERATE_RESPONSE(product STRING, entity STRING, reason STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN PROMPT_HANDLER(
# MAGIC   CONCAT("What alternative products can you recommend for ", product,
# MAGIC     " when a customer had a complaint about ", entity, " because ", reason,
# MAGIC     "Give me a response in the tone of an empathetic message back to the customer; only provide the body")
# MAGIC );
# MAGIC
# MAGIC
# MAGIC -- Detect product brands in a given piece of text
# MAGIC CREATE OR REPLACE FUNCTION DETECT_BRANDS(text STRING)
# MAGIC RETURNS ARRAY<STRING>
# MAGIC RETURN FROM_JSON(
# MAGIC   PROMPT_HANDLER(
# MAGIC     CONCAT('Detect brand names in this text and return array of correctly spelt names. Text:', text)
# MAGIC   ), "array<string>"
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's take a quick look at these functions in action!

# COMMAND ----------

# DBTITLE 1,Look at a subset of the review data
# MAGIC %sql
# MAGIC SELECT review_id, product_id, product_title, review_body 
# MAGIC FROM $raw_review_table 
# MAGIC WHERE product_category = "Grocery" 
# MAGIC LIMIT 3;

# COMMAND ----------

# DBTITLE 1,ANNOTATE_REVIEW() to classify freeform customer reviews
# MAGIC %sql
# MAGIC SELECT review_body, ANNOTATE_REVIEW(review_body)
# MAGIC FROM $raw_review_table 
# MAGIC WHERE product_category = "Grocery" 
# MAGIC LIMIT 3;

# COMMAND ----------

# DBTITLE 1,Untitled
# MAGIC %sql
# MAGIC SELECT GENERATE_RESPONSE("Country Choice Snacking Cookies", "cookies", "Quality issue") AS customer_response

# COMMAND ----------

# MAGIC %md
# MAGIC ### AdHoc Queries
# MAGIC Analysts can use the `PROMPT_HANDLER()` function we created earlier to apply their own prompts to the data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT review_id,
# MAGIC   PROMPT_HANDLER(
# MAGIC     CONCAT(
# MAGIC       "Does this review discuss beverages? Exclude non-alcoholic products. Answer Y or N only, no explanations or notes. Review: ", 
# MAGIC       review_body)
# MAGIC   ) AS discusses_beverages,
# MAGIC   review_body
# MAGIC FROM $raw_review_table%md
# MAGIC ### AdHoc Queries
# MAGIC Analysts can use the `PROMPT_HANDLER()` function we created earlier to apply their own prompts to the data
# MAGIC WHERE review_id IN ("R9LEFDWWXPDEY", "R27UON10EV9FSV", "R299ZTEFIAHRQD")
# MAGIC ORDER BY discusses_beverages DESC
# MAGIC LIMIT 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building out an incremental data pipeline
# MAGIC &nbsp;
# MAGIC * **Bronze** tables: raw customer reviews in freeform text
# MAGIC * **Silver** tables: customer reviews enriched with
# MAGIC   * Extraction of entities (topics) discussed in the review
# MAGIC   * Sentiment analysis for each entity
# MAGIC   * Whether a follow-up is required
# MAGIC   * Reason for the follow-up
# MAGIC * **Gold** tables: cleansed data including **starter responses** to use in replies to customers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer

# COMMAND ----------

# DBTITLE 1,Load approx. 10 random rows to experiment with
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_customer_reviews_grocery
# MAGIC COMMENT "Raw data: A sample of Grocery-related reviews from Amazon's Customer Reviews Dataset"
# MAGIC AS WITH grocery_reviews AS (
# MAGIC   SELECT * FROM $data_table WHERE product_category IN ("Grocery")
# MAGIC )
# MAGIC SELECT * FROM grocery_reviews TABLESAMPLE (0.00041623690206528424 PERCENT) REPEATABLE ($seed); -- Get approx. 10 random rows

# COMMAND ----------

# DBTITLE 1,Use this later to generate more data to demonstrate incremental pipelines
# MAGIC %sql
# MAGIC WITH sample AS (SELECT * FROM $data_table WHERE product_category IN ("Grocery")),
# MAGIC subset AS (SELECT * FROM sample TABLESAMPLE (0.00041623690206528424 PERCENT))
# MAGIC MERGE INTO bronze_customer_reviews_grocery b USING subset s ON s.review_id = b.review_id
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# DBTITLE 1,View a subset of the data
# MAGIC %sql
# MAGIC SELECT * FROM bronze_customer_reviews_grocery TABLESAMPLE (20 PERCENT)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver layer
# MAGIC
# MAGIC The Silver layer is enriched with:
# MAGIC
# MAGIC - information about entities mentioned in a review
# MAGIC - sentiment per entity
# MAGIC - whether a review requires a follow-up
# MAGIC - reason for the follow-up

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_reviews_annotated
# MAGIC COMMENT "Customer reviews annotated with entity sentiment and whether a follow-up is required"
# MAGIC AS SELECT *, CAST(NULL AS 
# MAGIC     STRUCT<entities: ARRAY<STRUCT<entity_name: STRING, entity_type: STRING, entity_sentiment: STRING, followup: STRING, 
# MAGIC     followup_reason: STRING>>>) AS annotations
# MAGIC FROM $data_table WHERE 1=0; -- Only duplicate the schema, don't copy data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_reviews_annotated s USING bronze_customer_reviews_grocery b
# MAGIC ON b.review_id = s.review_id
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (marketplace, customer_id, review_id, product_id, product_parent, product_title, star_rating, 
# MAGIC     helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date, 
# MAGIC     year, product_category, annotations)
# MAGIC   VALUES (marketplace, customer_id, review_id, product_id, product_parent, product_title, star_rating, 
# MAGIC     helpful_votes, total_votes, vine, verified_purchase, review_headline, review_body, review_date, 
# MAGIC     year, product_category, 
# MAGIC     -- Annotate our review using Azure OpenAI
# MAGIC     ANNOTATE_REVIEW(review_body)); 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT review_body, annotations FROM silver_reviews_annotated LIMIT 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_reviews_processed
# MAGIC COMMENT "Annotated reviews transformed for easier querying"
# MAGIC AS SELECT *, "" AS entity_name, "" AS entity_type, "" AS entity_sentiment, "" AS followup_required, "" AS followup_reason
# MAGIC FROM $data_table WHERE 1=0; -- Only duplicate the schema, don't copy data across

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH exploded AS (
# MAGIC   SELECT * EXCEPT(annotations), 
# MAGIC     EXPLODE(annotations.entities) AS entity_details
# MAGIC   FROM silver_reviews_annotated
# MAGIC ),
# MAGIC columns_extracted AS (
# MAGIC   SELECT * EXCEPT(entity_details),
# MAGIC     entity_details.entity_name AS entity_name,
# MAGIC     LOWER(entity_details.entity_type) AS entity_type,
# MAGIC     entity_details.entity_sentiment AS entity_sentiment,
# MAGIC     entity_details.followup AS followup_required,
# MAGIC     entity_details.followup_reason AS followup_reason
# MAGIC   FROM exploded
# MAGIC )
# MAGIC MERGE INTO silver_reviews_processed p USING columns_extracted c ON c.review_id = p.review_id
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM silver_reviews_processed LIMIT 2;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer
# MAGIC
# MAGIC These tables will be used to populate BI dashboards for our customer experience team

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_customer_reviews (
# MAGIC   customer_id STRING, review_id STRING, product_id STRING, product_title STRING, product_category STRING, 
# MAGIC   star_rating INT, review_date DATE, review_body STRING
# MAGIC )
# MAGIC COMMENT "Cleansed customer reviews";
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_customer_followups_required (
# MAGIC   customer_id STRING, review_id STRING, product_id STRING, product_title STRING, star_rating INT,
# MAGIC   review_date DATE, review_body STRING, entity_name STRING, entity_type STRING, entity_sentiment STRING, 
# MAGIC   followup_required STRING, followup_reason STRING, followup_response STRING
# MAGIC )
# MAGIC COMMENT "Customers that require follow up including sample response";

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_customer_reviews g USING silver_reviews_annotated s ON s.review_id = g.review_id
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH followups_required AS (
# MAGIC   SELECT customer_id, review_id, product_id, product_title, star_rating, review_date, review_body, 
# MAGIC     entity_name, entity_type, entity_sentiment, followup_required, followup_reason
# MAGIC   FROM silver_reviews_processed
# MAGIC   WHERE followup_required = "Y"
# MAGIC )
# MAGIC MERGE INTO gold_customer_followups_required g USING followups_required f
# MAGIC ON f.review_id = g.review_id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (customer_id, review_id, product_id, product_title, star_rating, review_date, review_body, 
# MAGIC     entity_name, entity_type, entity_sentiment, followup_required, followup_reason, followup_response)
# MAGIC   VALUES (customer_id, review_id, product_id, product_title, star_rating, review_date, review_body, 
# MAGIC     entity_name, entity_type, entity_sentiment, followup_required, followup_reason, 
# MAGIC     -- Generate a customer response
# MAGIC     GENERATE_RESPONSE(product_title, 
# MAGIC     entity_name, followup_reason))

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM gold_customer_followups_required

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scribbles

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sample AS (SELECT * FROM $data_table LIMIT 3)
# MAGIC SELECT ANNOTATE_REVIEW(review_body) AS annotations, review_body FROM sample

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ANNOTATE_REVIEW("I received this set within a week, and it looks just as pictured. It's a little transparent due to being white, but only a little. The fit is perfect...I'm 5'1 and around 105lbs. I suggest some cute heels if you're shorter.")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PROMPT_HANDLER("Please give me a prompt for Midjourney. I want a technical architecture diagram that reflects Susan's problems in the following post: Every morning Susan walks straight into a storm of messages, and doesn't know where to start! You see, Susan is a customer success specialist at a global retailer, and her primary objective is to ensure customers are happy and receive personalised recommendations whenever they encounter issues. 
# MAGIC
# MAGIC Overnight the company has received hundreds of reviews and feedback across multiple channels including websites, apps, social media posts, and email. Susan starts her day logging into each of these systems and picking up the messages not yet collected by her colleagues. Next, she has to make sense of these messages, identify what needs responding to, and formulate a response to send back to the customer. It isn't easy because these messages are often in different formats and every customer expresses their opinions in their own unique style.
# MAGIC
# MAGIC Susan always feels uneasy because she knows she isn't always interpreting, categorising, and responding to these messages in a consistent manner. Her biggest fear is that she may inadvertently miss responding to a customer because she didn't properly interpret the message. Susan isn't alone! Many of her colleagues feel this way, as do most fellow customer service representatives out there!
# MAGIC ")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT PROMPT_HANDLER("Please give me a prompt for Midjourney. I want a quirky, yet professional, captivating image for a  technical blog post on how to apply OpenAI to data using SQL in a Databricks Lakehouse. It should convey: (a) how easy it is to use OpenAI with Databricks SQL and (b) the new possibilities that it opens up for untapped data! Please be as detailed as possible") AS response
