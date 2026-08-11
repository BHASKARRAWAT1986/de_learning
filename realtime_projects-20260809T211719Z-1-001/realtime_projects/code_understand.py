bronze-to-silver-insert-py-task run
bronze-to-silver-insert-py-task run



from databricks.sdk.runtime import dbutils
dbutils.library.restartPython()

import os
from pyspark.sql.functions import col, current_timestamp, lit, from_json, get_json_object, sha2, to_timestamp, concat, month, udf, expr
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.errors import AnalysisException
from datetime import datetime
from delta.tables import DeltaTable
import importlib.util
import json
import requests
from requests.auth import HTTPBasicAuth
import sys
from databricks.sdk.runtime import dbutils

# Add current directory to path for local imports
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_dir = "/Workspace" + "/".join(notebook_path.split("/")[:-1])
sys.path.insert(0, workspace_dir)
from job_parameters import JobParameters

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# DBTITLE 1: Import transaction schema
schema_file_path = os.path.join(workspace_dir, "transaction-schema.py")
spec = importlib.util.spec_from_file_location("transaction_schema", schema_file_path)
transaction_schema_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transaction_schema_module)
transaction_schema = transaction_schema_module.schema


cfg = JobParameters.get_bronze_to_silver_params()

# DBTITLE 1: Create decryption UDF

# Extract primitive values from cfg so UDFs don't close over the cfg object.
# UDFs are serialized and sent to Spark executors, which don't have job_parameters
# in their Python path. Plain strings serialize without requiring any imports.
_decryption_url = cfg.decryption_url
_decryption_user = cfg.decryption_user
_decryption_password = cfg.decryption_password


def decrypt_value(encrypted_value):
    """Call decryption API for a single encrypted value"""
    if encrypted_value is None or encrypted_value == "":
        return encrypted_value
    
    try:
        response = requests.get(
            _decryption_url,
            headers={"x-StringToDecrypt": encrypted_value},
            auth=HTTPBasicAuth(_decryption_user, _decryption_password),
            timeout=10
        )
        
        if response.status_code == 200:
            return response.text
        elif response.status_code == 400:
            # Treat 400 as non-fatal; return original encrypted value
            print(f"Decryption warning (400). Returning encrypted value: {encrypted_value}")
            return encrypted_value
        else:
            raise RuntimeError(
                f"Decryption failed with status {response.status_code}: {response.text}"
            )
    except Exception as e:
        raise RuntimeError(f"Decryption error: {e}")

# Register as UDF
decrypt_udf = udf(decrypt_value, StringType())

# DBTITLE 1: Helper function to decrypt specific fields in transaction
def decrypt_transaction_fields(transaction_row):
    """
    Decrypt specific fields in the transaction struct:
    - GiftCard.AccountNumber
    - GiftCards[].AccountNumber
    - DeletedGiftCards[].AccountNumber
    - Tenders[].EncryptedAccountNumber -> AccountNumber
    """
    if transaction_row is None:
        return None
    
    try:
        transaction_dict = transaction_row.asDict(recursive=True)
    except Exception:
        transaction_dict = transaction_row
    
    # Decrypt GiftCard.AccountNumber
    if transaction_dict.get('GiftCard') and transaction_dict['GiftCard'].get('AccountNumber'):
        transaction_dict['GiftCard']['AccountNumber'] = decrypt_value(
            transaction_dict['GiftCard']['AccountNumber']
        )
    
    # Decrypt GiftCards[].AccountNumber
    if transaction_dict.get('GiftCards'):
        for gift_card in transaction_dict['GiftCards']:
            if gift_card and gift_card.get('AccountNumber'):
                gift_card['AccountNumber'] = decrypt_value(gift_card['AccountNumber'])
    
    # Decrypt DeletedGiftCards[].AccountNumber
    if transaction_dict.get('DeletedGiftCards'):
        for gift_card in transaction_dict['DeletedGiftCards']:
            if gift_card and gift_card.get('AccountNumber'):
                gift_card['AccountNumber'] = decrypt_value(gift_card['AccountNumber'])
    
    # Decrypt Tenders[].EncryptedAccountNumber and overwrite AccountNumber
    if transaction_dict.get('Tenders'):
        for tender in transaction_dict['Tenders']:
            if tender and tender.get('EncryptedAccountNumber'):
                decrypted_account = decrypt_value(tender['EncryptedAccountNumber'])
                tender['AccountNumber'] = decrypted_account
    
    return transaction_dict

# Register as UDF
decrypt_transaction_udf = udf(decrypt_transaction_fields, transaction_schema)

# DBTITLE 1: Read Bronze table as streaming source
# Read from Bronze (streaming)
from datetime import datetime, timedelta, timezone
_starting_timestamp = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")

bronze_stream = spark.readStream \
    .format("delta") \
    .option("startingTimestamp", _starting_timestamp) \
    .option("schemaTrackingLocation", f"{cfg.checkpoint_path}/schema") \
    .option("allowSourceColumnDrop", "always") \
    .table(cfg.bronze_table_full)

current_ts = current_timestamp()

# Get all cluster usage tags, which are stored as a JSON string
all_tags_json = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags")

# Parse the JSON string into a Python dictionary
all_tags = {tag['key']: tag['value'] for tag in json.loads(all_tags_json)}
run_name = all_tags.get('RunName')

print(run_name)

# Retrieve the 'RunName'
source_system_name = "EndZone"
source_data_name = cfg.bronze_table_full
original_source_name = "POS"

# Parse the JSON and extract structured data
silver_df = bronze_stream.select(
    # Keys from Bronze (for deduplication)
    col("store_number"),
    col("register_number"),
    col("transaction_number"),
    col("transaction_date"),
    
    # Cast VARIANT → string → typed struct using the declared schema
    from_json(
        col("transaction").cast("string"),
        transaction_schema
    ).alias("transaction"),
    
    # Processing timestamp
    current_ts.alias("SILVER_LAYER_TIMESTAMP"),
    current_ts.alias("SILVER_LAYER_UPDATE_TIMESTAMP"),
    lit(source_system_name).alias("SOURCE_SYSTEM_NAME"),
    lit(source_data_name).alias("SOURCE_DATA_NAME"),
    lit(original_source_name).alias("ORIGINAL_SOURCE_NAME")
).filter(
    # Only include transactions with valid Header
    col("transaction.Header").isNotNull()
).filter(
    col("store_number").isNotNull()
).filter(
    col("register_number").isNotNull()
).filter(
    col("transaction_number").isNotNull()
).filter(
    col("transaction_date").isNotNull()
).withColumn(
    # Decrypt specific fields in transaction
    "transaction_decrypted",
    decrypt_transaction_udf(col("transaction"))
).drop("transaction").withColumnRenamed("transaction_decrypted", "transaction")

# DBTITLE 1: Remove duplicates before writing to silver
deduped_batch = silver_df.dropDuplicates([
        "store_number", 
        "register_number", 
        "transaction_number", 
        "transaction_date"
    ])


# DBTITLE 1: Write to silver table
def write_to_silver(batch_df, batch_id):
    
    print(f"Batch {batch_id}: Processing {batch_df.count()} records for Silver")

    # Create or merge into Silver table
    try:
        spark.sql(f"DESCRIBE TABLE {cfg.silver_table_full}")
        table_exists = True
    except AnalysisException as e:
        if "Table or view not found" in str(e) or "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            table_exists = False
        else:
            raise

    if not table_exists:
        print(f"Batch {batch_id}: Creating Silver table")
        batch_df.write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .saveAsTable(cfg.silver_table_full)
        
        # Enable auto-optimize
        spark.sql(f"""
            ALTER TABLE {cfg.silver_table_full}
            SET TBLPROPERTIES (
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """)

        # Enable liquid clustering
        spark.sql(f"ALTER TABLE {cfg.silver_table_full} CLUSTER BY (transaction_date, store_number)")
    else:
        # Merge using keys from nested transaction header
        delta_table = DeltaTable.forName(spark, cfg.silver_table_full)
        
        delta_table.alias("target").merge(
            batch_df.alias("source"),
            """
            target.transaction_date   = source.transaction_date   AND
            target.store_number       = source.store_number       AND
            target.register_number    = source.register_number    AND
            target.transaction_number = source.transaction_number
            """
        ).whenNotMatchedInsertAll().execute()
        
        print(f"Batch {batch_id}: Silver merge complete")
		
# DBTITLE 1: Write to Silver table using Structured Streaming
# Start the regular silver stream
silver_query = deduped_batch.writeStream \
    .foreachBatch(write_to_silver) \
    .option("checkpointLocation", cfg.checkpoint_path) \
    .trigger(processingTime="2 minutes") \
    .start()

print(f"Silver stream started: {silver_query.id}")



pandas_udf instead of row-wise


https://learn.microsoft.com/en-us/azure/databricks/udf/pandas