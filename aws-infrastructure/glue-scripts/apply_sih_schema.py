"""
Glue job (Python shell): apply schema from sih-columns.json (in S3) to the Glue table 'sih'.
Run after the SIH crawler completes so the table uses the JSON schema instead of inferred types.
Uses --database_name, --table_name, --schema_s3_uri, --table_location.
"""
import json
import sys
import re
import boto3
from botocore.exceptions import ClientError

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "database_name", "table_name", "schema_s3_uri", "table_location"],
)
database_name = args["database_name"]
table_name = args["table_name"]
schema_s3_uri = (args["schema_s3_uri"] or "").strip().rstrip("/")
table_location = (args["table_location"] or "").rstrip("/") + "/"

# Parse s3://bucket/key
match = re.match(r"s3://([^/]+)/(.+)", schema_s3_uri)
if not match:
    print(f"ERROR: Invalid schema_s3_uri: {schema_s3_uri}", file=sys.stderr)
    sys.exit(2)
bucket, key = match.group(1), match.group(2)

s3 = boto3.client("s3")
glue = boto3.client("glue")

# Load columns from JSON in S3
try:
    resp = s3.get_object(Bucket=bucket, Key=key)
    columns_spec = json.loads(resp["Body"].read())
except Exception as e:
    print(f"ERROR: Failed to read schema from S3: {e}", file=sys.stderr)
    sys.exit(2)

columns = [{"Name": str(c.get("Name", "")), "Type": str(c.get("Type", "string"))} for c in columns_spec]

storage_descriptor = {
    "Columns": columns,
    "Location": table_location,
    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
    "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        "Parameters": {"field.delim": ","},
    },
}

try:
    resp = glue.get_table(DatabaseName=database_name, Name=table_name)
    table = resp["Table"]
    # Build a clean StorageDescriptor for update_table (only allowed fields)
    sd = {
        "Columns": columns,
        "Location": (table.get("StorageDescriptor") or {}).get("Location") or table_location,
        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "SerdeInfo": {
            "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "Parameters": {"field.delim": ","},
        },
    }
    partition_keys = table.get("PartitionKeys") or []
    partition_keys_clean = [{"Name": str(p.get("Name", "")), "Type": str(p.get("Type", "string"))} for p in partition_keys]
    glue.update_table(
        DatabaseName=database_name,
        TableInput={
            "Name": table_name,
            "StorageDescriptor": sd,
            "PartitionKeys": partition_keys_clean,
            "TableType": table.get("TableType") or "EXTERNAL_TABLE",
        },
    )
    print(f"Updated table {database_name}.{table_name} with schema from {schema_s3_uri}")
except ClientError as e:
    if e.response.get("Error", {}).get("Code") == "EntityNotFoundException":
        try:
            glue.create_table(
                DatabaseName=database_name,
                TableInput={
                    "Name": table_name,
                    "TableType": "EXTERNAL_TABLE",
                    "PartitionKeys": [],
                    "StorageDescriptor": storage_descriptor,
                },
            )
            print(f"Created table {database_name}.{table_name} with schema from {schema_s3_uri}")
        except Exception as create_err:
            print(f"ERROR: create_table failed: {create_err}", file=sys.stderr)
            sys.exit(2)
    else:
        print(f"ERROR: get_table/update_table failed: {e}", file=sys.stderr)
        sys.exit(2)
except Exception as e:
    print(f"ERROR: {e}", file=sys.stderr)
    sys.exit(2)
