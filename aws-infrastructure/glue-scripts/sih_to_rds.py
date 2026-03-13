"""
Glue ETL job: read SIH data from Glue Data Catalog, then load into RDS table st_sih and fat_sih (same structure).
Adds foreign keys on fat_sih to dimension tables per star schema.
Uses --catalog_database, --catalog_table, --jdbc_url, --secret_arn, --output_table.
"""
FAT_SIH_TABLE = "fat_sih"

# (fat_sih column, dimension table, dimension PK column) for FK constraints
FAT_SIH_FOREIGN_KEYS = [
    ("uf_zi", "dim_ibge_municipios", "cod_ibge"),
    ("espec", "dim_espec", "cod"),
    ("ident", "dim_ident", "cod"),
    ("sexo", "dim_sexo", "cod"),
    ("marca_uti", "dim_marca_uti", "cod"),
    ("proc_solic", "dim_sigtap", "ip_cod"),
    ("proc_rea", "dim_sigtap", "ip_cod"),
    ("cobranca", "dim_cobranca", "cod"),
    ("diag_princ", "dim_cid10", "cid10"),
    ("diag_secun", "dim_cid10", "cid10"),
    ("nat_jur", "dim_natjur", "cod"),
    ("gestao", "dim_gestao", "cod"),
    ("munic_mov", "dim_ibge_municipios", "cod_ibge"),
    ("cod_idade", "dim_idade", "cod"),
    ("nacional", "dim_nacional", "cod"),
    ("instru", "dim_instrucao", "cod"),
    ("complex", "dim_complex", "cod"),
    ("financ", "dim_financ", "cod"),
    ("raca_cor", "dim_raca_cor", "cod"),
]
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "catalog_database",
        "catalog_table",
        "jdbc_url",
        "db_user",
        "db_password",
        "output_table",
    ],
)
catalog_database = args["catalog_database"]
catalog_table = args["catalog_table"]
jdbc_url = args["jdbc_url"]
user = args["db_user"]
password = args["db_password"]
output_table = args["output_table"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

qualified_table = catalog_database + "." + catalog_table
df = spark.table(qualified_table)

# Add column with the source CSV filename (path as in S3, e.g. s3://bucket/raw/sih/RDAC3101.csv -> RDAC3101.csv)
df = df.withColumn(
    "arquivo_origem",
    F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1),
)

if df.rdd.isEmpty():
    print("No data in catalog table; skipping write.")
    job.commit()
    sys.exit(0)

# Cast struct/array/map columns to string so JDBC write does not fail (JDBC has no type for complex types)
def ensure_jdbc_safe(dataframe):
    cols = []
    for f in dataframe.schema.fields:
        if isinstance(f.dataType, (T.StructType, T.ArrayType, T.MapType)):
            cols.append(F.col(f.name).cast("string").alias(f.name))
        else:
            cols.append(F.col(f.name))
    return dataframe.select(cols)

df = ensure_jdbc_safe(df)

# Drop output table if it exists (then we will create it on write)
try:
    spark._jvm.Class.forName("org.postgresql.Driver")
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    stmt = conn.createStatement()
    stmt.execute("DROP TABLE IF EXISTS " + output_table + " CASCADE")
    stmt.close()
    conn.close()
    print("Dropped table if existed:", output_table)
except Exception as e:
    print("Drop table (optional):", e)

# Write DataFrame to RDS (table will be created by Spark JDBC if not exists)
if "?" in jdbc_url:
    jdbc_url = jdbc_url + "&reWriteBatchedInserts=true"
else:
    jdbc_url = jdbc_url + "?reWriteBatchedInserts=true"

num_partitions = max(4, 2 * 2)
df.repartition(num_partitions).write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", output_table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", "50000") \
    .mode("overwrite") \
    .save()

print("Written to RDS table:", output_table)

# Populate FAT_SIH with same structure (same data as st_sih)
try:
    conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
    stmt = conn.createStatement()
    stmt.execute("DROP TABLE IF EXISTS " + FAT_SIH_TABLE + " CASCADE")
    stmt.close()
    conn.close()
    print("Dropped table if existed:", FAT_SIH_TABLE)
except Exception as e:
    print("Drop FAT_SIH (optional):", e)

df.repartition(num_partitions).write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", FAT_SIH_TABLE) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "org.postgresql.Driver") \
    .option("batchsize", "50000") \
    .mode("overwrite") \
    .save()

print("Written to RDS table:", FAT_SIH_TABLE)

# Add foreign keys to dimension tables (failures logged, e.g. orphan rows or missing dims)
conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
stmt = conn.createStatement()
for fk_col, dim_table, dim_pk in FAT_SIH_FOREIGN_KEYS:
    constraint_name = "fk_fat_sih_" + fk_col
    try:
        sql = (
            f"ALTER TABLE {FAT_SIH_TABLE} "
            f"ADD CONSTRAINT {constraint_name} "
            f"FOREIGN KEY ({fk_col}) REFERENCES {dim_table}({dim_pk})"
        )
        stmt.execute(sql)
        print("Added FK:", constraint_name)
    except Exception as e:
        print("FK", constraint_name, "(optional):", e)
stmt.close()
conn.close()

job.commit()
