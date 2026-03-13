"""
Glue ETL job (dimensions_aux): create simpler dimension tables from Glue Data Catalog.
Only creates and loads each table if it does not exist; adds the given primary key.
Uses --catalog_database, --jdbc_url, --secret_arn.
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

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "catalog_database", "jdbc_url", "secret_arn"],
)
catalog_database = args["catalog_database"]
jdbc_url = args["jdbc_url"]
secret_arn = args["secret_arn"]

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

client = boto3.client("secretsmanager")
secret = client.get_secret_value(SecretId=secret_arn)
secret_dict = json.loads(secret["SecretString"])
user = secret_dict.get("username")
password = secret_dict.get("password")
if not user or not password:
    raise ValueError("Secret missing username or password")


def ensure_jdbc_safe(dataframe):
    cols = []
    for f in dataframe.schema.fields:
        if isinstance(f.dataType, (T.StructType, T.ArrayType, T.MapType)):
            cols.append(F.col(f.name).cast("string").alias(f.name))
        else:
            cols.append(F.col(f.name))
    return dataframe.select(cols)


def process_dimension(catalog_table, output_table, pk_column):
    print(f"--- {catalog_table} -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        dyf = glueContext.create_dynamic_frame.from_catalog(
            database=catalog_database,
            table_name=catalog_table,
        )
        df = dyf.toDF()
        if df.rdd.isEmpty():
            print(f"No data in {catalog_table}; skipping.")
            conn.close()
            return
        df = ensure_jdbc_safe(df)
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {catalog_table}: {e}")


def process_static_dim_espec():
    """Create dim_espec from static data (cod, espec) if table does not exist."""
    output_table = "dim_espec"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "Clínica médica"),
            (2, "Cirurgia"),
            (3, "Obstetrícia"),
            (4, "Pediatria"),
            (5, "Psiquiatria"),
            (6, "Tisiologia"),
            (7, "Reabilitação"),
            (8, "Dermatologia sanitária"),
            (9, "Hanseníase"),
            (10, "Outras especialidades"),
            (12, "Cirurgia pediátrica"),
            (13, "Clínica pediátrica"),
            (14, "Hospital-dia"),
            (87, "Internação domiciliar"),
        ]
        df = spark.createDataFrame(data, ["cod", "espec"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_ident():
    """Create dim_ident from static data (cod, descricao) if table does not exist."""
    output_table = "dim_ident"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "AIH normal"),
            (3, "AIH de continuação"),
            (5, "AIH de longa permanência"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_sexo():
    """Create dim_sexo from static data (cod, descricao) if table does not exist."""
    output_table = "dim_sexo"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "Masculino"),
            (3, "Feminino"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_marca_uti():
    """Create dim_marca_uti from static data (seq, desc, cod) if table does not exist."""
    output_table = "dim_marca_uti"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "Não utilizou UTI", 0),
            (2, "Unidade intermediária", 64),
            (3, "Unidade intermediária neonatal", 65),
            (4, "UTI I", 74),
            (5, "UTI Adulto II", 75),
            (6, "UTI Adulto III", 76),
            (7, "UTI Infantil I", 77),
            (8, "UTI Infantil II", 78),
            (9, "UTI Infantil III", 79),
            (10, "UTI Neonatal I", 80),
            (11, "UTI Neonatal II", 81),
            (12, "UTI Neonatal III", 82),
            (13, "UTI Queimados", 83),
            (14, "UTI Doador", 99),
        ]
        df = spark.createDataFrame(data, ["seq", "desc", "cod"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_cobranca():
    """Create dim_cobranca from static data (cod, descricao) if table does not exist."""
    output_table = "dim_cobranca"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (11, "Serviços hospitalares"),
            (12, "Serviços profissionais"),
            (14, "Serviços hospitalares – complementar"),
            (15, "Serviços profissionais – complementar"),
            (16, "Serviços auxiliares"),
            (18, "SADT (Serviços de apoio diagnóstico e terapêutico)"),
            (19, "Outros serviços"),
            (21, "Diárias"),
            (22, "Taxas"),
            (23, "Materiais"),
            (24, "Medicamentos"),
            (25, "Órteses e próteses"),
            (26, "Sangue e hemoderivados"),
            (27, "Nutrição"),
            (28, "Gases medicinais"),
            (31, "UTI – diária"),
            (32, "UTI – taxa"),
            (41, "Transplantes"),
            (42, "Procedimentos especiais"),
            (43, "Terapias especiais"),
            (51, "Pacotes de procedimentos"),
            (61, "Internação domiciliar"),
            (62, "Atenção domiciliar – serviços"),
            (63, "Atenção domiciliar – materiais"),
            (64, "Atenção domiciliar – medicamentos"),
            (65, "Atenção domiciliar – equipamentos"),
            (66, "Atenção domiciliar – nutrição"),
            (67, "Atenção domiciliar – gases"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_natjur():
    """Create dim_natjur from static data (cod, descricao) if table does not exist."""
    output_table = "dim_natjur"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1015, "Administração pública direta"),
            (1023, "Autarquia"),
            (1031, "Fundação pública"),
            (1040, "Empresa pública"),
            (1058, "Sociedade de economia mista"),
            (1066, "Consórcio público"),
            (1074, "Fundação privada"),
            (2011, "Empresa privada"),
            (2020, "Sociedade empresária limitada"),
            (2038, "Sociedade anônima"),
            (2046, "Cooperativa"),
            (2054, "Entidade filantrópica"),
            (2062, "Organização social"),
            (2070, "Organização da sociedade civil"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_gestao():
    """Create dim_gestao from static data (cod, descricao) if table does not exist."""
    output_table = "dim_gestao"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "Gestão Municipal"),
            (2, "Gestão Estadual"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_idade():
    """Create dim_idade from static data (cod, descricao) if table does not exist."""
    output_table = "dim_idade"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (2, "Dias"),
            (3, "Meses"),
            (4, "Anos"),
            (5, "Idade ignorada"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_instrucao():
    """Create dim_instrucao from static data (cod, descricao) if table does not exist."""
    output_table = "dim_instrucao"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (0, "Ignorado / não informado"),
            (1, "Nenhuma escolaridade"),
            (2, "Ensino fundamental"),
            (3, "Ensino médio"),
            (4, "Ensino superior"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_complex():
    """Create dim_complex from static data (cod, descricao) if table does not exist."""
    output_table = "dim_complex"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "Atenção básica"),
            (2, "Média complexidade"),
            (3, "Alta complexidade"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_financ():
    """Create dim_financ from static data (cod, descricao) if table does not exist."""
    output_table = "dim_financ"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (1, "Atenção básica"),
            (2, "Média complexidade"),
            (3, "Alta complexidade"),
            (4, "Fundo de ações estratégicas e compensação (FAEC)"),
            (5, "Incentivos"),
            (6, "Média e alta complexidade (MAC)"),
            (7, "Assistência farmacêutica"),
            (8, "Vigilância em saúde"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


def process_static_dim_raca_cor():
    """Create dim_raca_cor from static data (cod, descricao) if table does not exist."""
    output_table = "dim_raca_cor"
    pk_column = "cod"
    print(f"--- static -> {output_table} ---")
    try:
        spark._jvm.Class.forName("org.postgresql.Driver")
        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        rs = conn.getMetaData().getTables(None, None, output_table, None)
        if rs.next():
            print(f"Table {output_table} already exists. Skipping.")
            conn.close()
            return
        conn.close()

        data = [
            (0, "Ignorado"),
            (1, "Branca"),
            (2, "Preta"),
            (3, "Parda"),
            (4, "Amarela"),
            (5, "Indígena"),
        ]
        df = spark.createDataFrame(data, ["cod", "descricao"])
        df.write.format("jdbc").option("url", jdbc_url).option("dbtable", output_table) \
            .option("user", user).option("password", password).option("driver", "org.postgresql.Driver") \
            .mode("overwrite").save()

        conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
        stmt = conn.createStatement()
        try:
            stmt.execute(f"ALTER TABLE {output_table} ADD PRIMARY KEY ({pk_column})")
        except Exception as e:
            print(f"Warning: PK {pk_column}: {e}")
        stmt.close()
        conn.close()
        print(f"Written {output_table}.")
    except Exception as e:
        print(f"Error {output_table}: {e}")


# Simpler dimensions: catalog_table -> output_table, pk
DIMENSIONS_AUX = [
    ("ibge_uf", "dim_ibge_uf", "id"),
    ("sigtap", "dim_sigtap", "ip_cod"),
    ("cid10", "dim_cid10", "cid10"),
]

for cat, out, pk in DIMENSIONS_AUX:
    process_dimension(cat, out, pk)

process_static_dim_espec()
process_static_dim_ident()
process_static_dim_sexo()
process_static_dim_marca_uti()
process_static_dim_cobranca()
process_static_dim_natjur()
process_static_dim_gestao()
process_static_dim_idade()
process_static_dim_instrucao()
process_static_dim_complex()
process_static_dim_financ()
process_static_dim_raca_cor()

job.commit()
