"""
Glue ETL job: read dimension data from Glue Data Catalog (crawler output), then load into RDS.
Only creates the tables and loads data if they do not exist. Adds the specified primary keys.
Uses --jdbc_url and --secret_arn job parameters.
"""
import json
import sys
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Resolve job parameters
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "catalog_database",
        "jdbc_url",
        "secret_arn",
    ],
)
catalog_database = args["catalog_database"]
jdbc_url = args["jdbc_url"]
secret_arn = args["secret_arn"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 1) Get username and password from Secrets Manager
client = boto3.client("secretsmanager")
secret = client.get_secret_value(SecretId=secret_arn)
secret_dict = json.loads(secret["SecretString"])
user = secret_dict.get("username")
password = secret_dict.get("password")

if not user or not password:
    raise ValueError("Secret missing username or password")

# Cast struct/array/map columns to string so JDBC write does not fail
def ensure_jdbc_safe(dataframe):
    cols = []
    for f in dataframe.schema.fields:
        if isinstance(f.dataType, (T.StructType, T.ArrayType, T.MapType)):
            cols.append(F.col(f.name).cast("string").alias(f.name))
        else:
            cols.append(F.col(f.name))
    return dataframe.select(cols)


def process_dim_ibge_municipios():
    """Build dim_ibge_municipios from municipalities with cod_ibge_uf and uf as NULL (to be filled later)."""
    output_table = "dim_ibge_municipios"
    pk_column = "id"
    spark._jvm.Class.forName("org.postgresql.Driver")
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    try:
        db_meta = conn.getMetaData()
        rs = db_meta.getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists in RDS. Skipping.")
            conn.close()
            return
    except Exception:
        pass

    print("--- Building dim_ibge_municipios (municipalities + cod_ibge_uf, uf as NULL) ---")
    dyf_mun = glueContext.create_dynamic_frame.from_catalog(
        database=catalog_database,
        table_name="ibge_municipios",
    )
    df = dyf_mun.toDF()
    if df.rdd.isEmpty():
        print("No municipalities data; skipping dim_ibge_municipios.")
        conn.close()
        return

    df = df.withColumn("cod_ibge_uf", F.lit(None).cast("string")).withColumn("uf", F.lit(None).cast("string"))
    df = ensure_jdbc_safe(df)

    df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
        .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
        .mode("overwrite").save()

    # Update cod_ibge_uf with first 2 chars of cod_ibge
    stmt = conn.createStatement()
    try:
        stmt.execute(
            f"UPDATE {output_table} dim SET cod_ibge_uf = LEFT(CAST(cod_ibge AS varchar), 2)"
        )
        print("Updated cod_ibge_uf from first 2 characters of cod_ibge.")
    except Exception as e:
        print(f"Warning: Could not update cod_ibge_uf: {e}")
    finally:
        stmt.close()

    # Update uf with nome from dim_ibge_uf where cod_ibge_uf matches
    stmt = conn.createStatement()
    try:
        stmt.execute(
            "UPDATE dim_ibge_municipios dim SET uf = u.nome "
            "FROM dim_ibge_uf u WHERE dim.cod_ibge_uf = TRIM(CAST(u.cod_ibge AS varchar))"
        )
        print("Updated uf from dim_ibge_uf.")
    except Exception as e:
        print(f"Warning: Could not update uf from dim_ibge_uf: {e}")
    finally:
        stmt.close()

    try:
        stmt = conn.createStatement()
        stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        stmt.close()
    except Exception as e:
        print(f"Warning: Could not add primary key: {e}")
    conn.close()
    print(f"Written {output_table} with cod_ibge_uf and uf.")


def process_dimension(catalog_table, output_table, pk_column):
    print(f"--- Processing {catalog_table} -> {output_table} ---")
    try:
        # Check if table exists in RDS
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        db_meta = conn.getMetaData()
        rs = db_meta.getTables(None, None, output_table, None)
        table_exists = rs.next()
        
        if table_exists:
            print(f"Table {output_table} already exists in RDS. Skipping creation and load.")
            conn.close()
            return

        print(f"Table {output_table} does not exist. Reading from catalog...")
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=catalog_database,
            table_name=catalog_table,
        )
        df = dyf.toDF()
        
        if df.rdd.isEmpty():
            print(f"No data in catalog table {catalog_table}; skipping write.")
            conn.close()
            return

        df = ensure_jdbc_safe(df)
        
        print(f"Writing to RDS table: {output_table}")
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", output_table) \
            .option("user", user) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
            
        # Add primary key
        stmt = conn.createStatement()
        try:
            print(f"Adding primary key {pk_column} to {output_table}...")
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
            print(f"Successfully added primary key.")
        except Exception as e:
            print(f"Warning: Could not add primary key {pk_column} to {output_table}: {e}")
        finally:
            stmt.close()
            
        conn.close()
    except Exception as e:
        print(f"Error processing {catalog_table}: {e}")

# dim_ibge_uf first (dim_ibge_municipios UPDATE uf depends on it)
process_dimension("ibge_uf", "dim_ibge_uf", "id")

# dim_ibge_municipios: municipalities + cod_ibge_uf + uf (UPDATE uf from dim_ibge_uf)
process_dim_ibge_municipios()

# Other dimensions
dimensions = [
    {"catalog": "sigtap", "output": "dim_sigtap", "pk": "ip_cod"},
    {"catalog": "cid10",  "output": "dim_cid10",  "pk": "cid10"},
    {"catalog": "nacional", "output": "dim_nacional", "pk": "id"},
]
for dim in dimensions:
    process_dimension(dim["catalog"], dim["output"], dim["pk"])

job.commit()
