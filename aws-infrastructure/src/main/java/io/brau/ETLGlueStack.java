package io.brau;

import java.util.List;
import java.util.Map;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.glue.CfnClassifier;
import software.amazon.awscdk.services.glue.CfnCrawler;
import software.amazon.awscdk.services.glue.CfnDatabase;
import software.amazon.awscdk.services.glue.CfnJob;
import software.amazon.awscdk.services.glue.CfnTrigger;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.s3.IBucket;
import software.amazon.awscdk.services.s3.assets.Asset;
import software.amazon.awscdk.services.s3.deployment.BucketDeployment;
import software.amazon.awscdk.services.s3.deployment.Source;

/**
 * Stack that creates Glue resources for ETL: a Data Catalog database for CSV
 * and a crawler that reads CSV files (comma-separated) from the data lake S3 bucket.
 * Depends on {@link DatalakeInfrastructureStack} for the S3 bucket.
 */
public class ETLGlueStack extends Stack {

    private static final String GLUE_DATABASE_NAME = "datalake_csv";
    private static final String CRAWLER_NAME = "sih-sus-csv-crawler";
    private static final String MUNICIPIOS_CRAWLER_NAME = "municipios-csv-crawler";
    private static final String UF_CRAWLER_NAME = "uf-csv-crawler";
    private static final String SIGTAP_CRAWLER_NAME = "sigtap-csv-crawler";
    private static final String CID10_CRAWLER_NAME = "cid10-csv-crawler";
    private static final String NACIONAL_CRAWLER_NAME = "nacional-csv-crawler";
    private static final String CRAWLER_ROLE_NAME = "datalake-glue-crawler-role";
    private static final String CSV_CLASSIFIER_NAME = "csv-comma";
    /** S3 prefix where SIH CSV files are stored. */
    private static final String CSV_S3_PREFIX = "raw/sih/";
    /** S3 prefix where IBGE municipalities CSV files are stored (e.g. MUNICIPIOS.CSV). */
    private static final String CSV_IBGE_MUNICIPIOS_PREFIX = "raw/ibge-municipios/";
    /** S3 prefix where IBGE UF CSV files are stored (e.g. UF.CSV). */
    private static final String CSV_IBGE_UF_PREFIX = "raw/ibge-uf/";
    /** S3 prefix where SIGTAP CSV files are stored (e.g. TB_SIGTAP.CSV). */
    private static final String CSV_SIGTAP_PREFIX = "raw/sigtap/";
    /** S3 prefix where CID10 CSV files are stored (e.g. CID10.CSV). */
    private static final String CSV_CID10_PREFIX = "raw/cid10/";
    /** S3 prefix where nacionalidade CSV files are stored (e.g. NACION3D.CSV). */
    private static final String CSV_NACIONAL_PREFIX = "raw/nacional/";
    /** Glue table name for SIH CSV (same as crawler target table name). */
    private static final String SIH_TABLE_NAME = "sih";
    /** Job that applies sih-columns.json schema to table after crawler runs. */
    private static final String APPLY_SIH_SCHEMA_JOB_NAME = "apply-sih-schema";
    /** On-demand trigger: run SIH crawler (use this instead of starting crawler manually so schema job runs after). */
    private static final String SIH_CRAWLER_TRIGGER_NAME = "sih-crawler-then-schema";

    public ETLGlueStack(final Construct scope, final String id, final IBucket dataLakeBucket) {
        this(scope, id, null, dataLakeBucket);
    }

    public ETLGlueStack(final Construct scope, final String id, final StackProps props, final IBucket dataLakeBucket) {
        super(scope, id, props);

        // Catalog (database) in Glue Data Catalog for CSV tables (SIH and IBGE e.g. municipalities)
        CfnDatabase.Builder.create(this, "CsvCatalog")
                .catalogId(getAccount())
                .databaseInput(CfnDatabase.DatabaseInputProperty.builder()
                        .name(GLUE_DATABASE_NAME)
                        .description("Catalog for comma-separated CSV files from S3 (SIH and IBGE)")
                        .build())
                .build();

        // Deploy sih-columns.json to S3 so the apply-sih-schema job can read it when the crawler completes.
        BucketDeployment.Builder.create(this, "DeploySihSchemaJson")
                .sources(List.of(Source.asset("src/main/resources/glue-schemas")))
                .destinationBucket(dataLakeBucket)
                .destinationKeyPrefix("glue-schemas/")
                .build();

        String s3PathSih = "s3://" + dataLakeBucket.getBucketName() + "/" + CSV_S3_PREFIX;
        String schemaS3Uri = "s3://" + dataLakeBucket.getBucketName() + "/glue-schemas/sih-columns.json";

        // CSV classifier with comma delimiter
        CfnClassifier.Builder.create(this, "CsvCommaClassifier")
                .csvClassifier(CfnClassifier.CsvClassifierProperty.builder()
                        .name(CSV_CLASSIFIER_NAME)
                        .delimiter(",")
                        .containsHeader("PRESENT")
                        .build())
                .build();

        // IAM role for the Glue Crawler to access S3 and write logs
        Role crawlerRole = Role.Builder.create(this, "GlueCrawlerRole")
                .roleName(CRAWLER_ROLE_NAME)
                .assumedBy(new ServicePrincipal("glue.amazonaws.com"))
                .build();
        dataLakeBucket.grantRead(crawlerRole);
        // Allow crawler to write to the default Glue crawler log group
        String glueLogGroupArn = "arn:aws:logs:" + getRegion() + ":" + getAccount() + ":log-group:/aws-glue/crawlers:*";
        crawlerRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("logs:CreateLogStream", "logs:PutLogEvents"))
                .resources(List.of(glueLogGroupArn))
                .build());
        // Allow crawler to read and write Glue Data Catalog (datalake_csv database and tables)
        String catalogArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":catalog";
        String databaseArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":database/" + GLUE_DATABASE_NAME;
        String tableArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":table/" + GLUE_DATABASE_NAME + "/*";
        crawlerRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "glue:GetDatabase", "glue:GetDatabases",
                        "glue:GetTable", "glue:GetTables",
                        "glue:CreateTable", "glue:UpdateTable",
                        "glue:GetPartition", "glue:GetPartitions",
                        "glue:CreatePartition", "glue:UpdatePartition",
                        "glue:BatchCreatePartition", "glue:BatchUpdatePartition"))
                .resources(List.of(catalogArn, databaseArn, tableArn))
                .build());

        // Crawler that reads SIH CSV files from S3 and creates/updates table "sih".
        // After crawler completes, trigger runs apply-sih-schema job which overwrites schema with sih-columns.json.
        CfnCrawler.Builder.create(this, "CsvCrawler")
                .name(CRAWLER_NAME)
                .role(crawlerRole.getRoleArn())
                .databaseName(GLUE_DATABASE_NAME)
                .classifiers(List.of(CSV_CLASSIFIER_NAME))
                .schemaChangePolicy(CfnCrawler.SchemaChangePolicyProperty.builder()
                        .updateBehavior("UPDATE_IN_DATABASE")
                        .deleteBehavior("LOG")
                        .build())
                .targets(CfnCrawler.TargetsProperty.builder()
                        .s3Targets(List.of(CfnCrawler.S3TargetProperty.builder()
                                .path(s3PathSih)
                                .build()))
                        .build())
                .build();

        // Role for the job that applies sih-columns.json to the Glue table after crawler runs.
        Role applySchemaJobRole = Role.Builder.create(this, "ApplySihSchemaJobRole")
                .assumedBy(new ServicePrincipal("glue.amazonaws.com"))
                .build();
        dataLakeBucket.grantRead(applySchemaJobRole);
        applySchemaJobRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:CreateTable",
                        "glue:GetDatabase", "glue:GetDatabases"))
                .resources(List.of(catalogArn, databaseArn, tableArn))
                .build());
        String applySchemaLogArn = "arn:aws:logs:" + getRegion() + ":" + getAccount() + ":log-group:/aws-glue/jobs/*";
        applySchemaJobRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"))
                .resources(List.of(applySchemaLogArn))
                .build());

        Asset applySchemaScript = Asset.Builder.create(this, "ApplySihSchemaScript")
                .path("glue-scripts/apply_sih_schema.py")
                .build();
        applySchemaScript.grantRead(applySchemaJobRole);
        String applySchemaScriptUri = "s3://" + applySchemaScript.getS3BucketName() + "/" + applySchemaScript.getS3ObjectKey();

        CfnJob applySchemaJob = CfnJob.Builder.create(this, "ApplySihSchemaJob")
                .name(APPLY_SIH_SCHEMA_JOB_NAME)
                .role(applySchemaJobRole.getRoleArn())
                .command(CfnJob.JobCommandProperty.builder()
                        .name("pythonshell")
                        .scriptLocation(applySchemaScriptUri)
                        .pythonVersion("3")
                        .build())
                .defaultArguments(Map.of(
                        "--job-bookmark-option", "job-bookmark-disable",
                        "--database_name", GLUE_DATABASE_NAME,
                        "--table_name", SIH_TABLE_NAME,
                        "--schema_s3_uri", schemaS3Uri,
                        "--table_location", s3PathSih))
                .build();

        // On-demand trigger: start SIH crawler. Use this instead of "Run crawler" in console so the conditional trigger fires.
        CfnTrigger.Builder.create(this, "SihCrawlerOnDemandTrigger")
                .name(SIH_CRAWLER_TRIGGER_NAME)
                .type("ON_DEMAND")
                .actions(List.of(CfnTrigger.ActionProperty.builder()
                        .crawlerName(CRAWLER_NAME)
                        .build()))
                .build();

        // When SIH crawler succeeds, run apply-sih-schema job to set table schema from sih-columns.json.
        CfnTrigger.Builder.create(this, "SihCrawlerThenApplySchemaTrigger")
                .name("sih-crawler-then-apply-schema")
                .type("CONDITIONAL")
                .predicate(CfnTrigger.PredicateProperty.builder()
                        .conditions(List.of(CfnTrigger.ConditionProperty.builder()
                                .crawlerName(CRAWLER_NAME)
                                .crawlState("SUCCEEDED")
                                .logicalOperator("EQUALS")
                                .build()))
                        .logical("AND")
                        .build())
                .actions(List.of(CfnTrigger.ActionProperty.builder()
                        .jobName(APPLY_SIH_SCHEMA_JOB_NAME)
                        .build()))
                .startOnCreation(true)
                .build();

        // Crawler that reads IBGE municipalities CSV files (e.g. MUNICIPIOS.CSV) from S3 into the same datalake_csv database
        String s3PathMunicipios = "s3://" + dataLakeBucket.getBucketName() + "/" + CSV_IBGE_MUNICIPIOS_PREFIX;
        CfnCrawler.Builder.create(this, "MunicipiosCsvCrawler")
                .name(MUNICIPIOS_CRAWLER_NAME)
                .role(crawlerRole.getRoleArn())
                .databaseName(GLUE_DATABASE_NAME)
                .classifiers(List.of(CSV_CLASSIFIER_NAME))
                .targets(CfnCrawler.TargetsProperty.builder()
                        .s3Targets(List.of(CfnCrawler.S3TargetProperty.builder()
                                .path(s3PathMunicipios)
                                .build()))
                        .build())
                .build();

        // Crawler that reads IBGE UF CSV files (e.g. UF.CSV) from S3 into the same datalake_csv database
        String s3PathUf = "s3://" + dataLakeBucket.getBucketName() + "/" + CSV_IBGE_UF_PREFIX;
        CfnCrawler.Builder.create(this, "UfCsvCrawler")
                .name(UF_CRAWLER_NAME)
                .role(crawlerRole.getRoleArn())
                .databaseName(GLUE_DATABASE_NAME)
                .classifiers(List.of(CSV_CLASSIFIER_NAME))
                .targets(CfnCrawler.TargetsProperty.builder()
                        .s3Targets(List.of(CfnCrawler.S3TargetProperty.builder()
                                .path(s3PathUf)
                                .build()))
                        .build())
                .build();

        // Crawler that reads SIGTAP CSV files (e.g. TB_SIGTAP.CSV) from S3 into the same datalake_csv database
        String s3PathSigtap = "s3://" + dataLakeBucket.getBucketName() + "/" + CSV_SIGTAP_PREFIX;
        CfnCrawler.Builder.create(this, "SigtapCsvCrawler")
                .name(SIGTAP_CRAWLER_NAME)
                .role(crawlerRole.getRoleArn())
                .databaseName(GLUE_DATABASE_NAME)
                .classifiers(List.of(CSV_CLASSIFIER_NAME))
                .targets(CfnCrawler.TargetsProperty.builder()
                        .s3Targets(List.of(CfnCrawler.S3TargetProperty.builder()
                                .path(s3PathSigtap)
                                .build()))
                        .build())
                .build();

        // Crawler that reads CID10 CSV files (e.g. CID10.CSV) from S3 into the same datalake_csv database
        String s3PathCid10 = "s3://" + dataLakeBucket.getBucketName() + "/" + CSV_CID10_PREFIX;
        CfnCrawler.Builder.create(this, "Cid10CsvCrawler")
                .name(CID10_CRAWLER_NAME)
                .role(crawlerRole.getRoleArn())
                .databaseName(GLUE_DATABASE_NAME)
                .classifiers(List.of(CSV_CLASSIFIER_NAME))
                .targets(CfnCrawler.TargetsProperty.builder()
                        .s3Targets(List.of(CfnCrawler.S3TargetProperty.builder()
                                .path(s3PathCid10)
                                .build()))
                        .build())
                .build();

        // Crawler that reads nacionalidade CSV files (e.g. NACION3D.CSV) from S3 into the same datalake_csv database
        String s3PathNacional = "s3://" + dataLakeBucket.getBucketName() + "/" + CSV_NACIONAL_PREFIX;
        CfnCrawler.Builder.create(this, "NacionalCsvCrawler")
                .name(NACIONAL_CRAWLER_NAME)
                .role(crawlerRole.getRoleArn())
                .databaseName(GLUE_DATABASE_NAME)
                .classifiers(List.of(CSV_CLASSIFIER_NAME))
                .targets(CfnCrawler.TargetsProperty.builder()
                        .s3Targets(List.of(CfnCrawler.S3TargetProperty.builder()
                                .path(s3PathNacional)
                                .build()))
                        .build())
                .build();
    }
}
