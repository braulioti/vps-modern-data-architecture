package io.brau;

import java.util.List;
import java.util.Map;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.s3.assets.Asset;
import software.amazon.awscdk.services.glue.CfnJob;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.s3.IBucket;

/**
 * Stack that creates AWS Glue ETL Jobs for the data lake pipeline.
 * Includes a job that reads SIH data from the Glue Data Catalog (crawler) and loads it into RDS dev (st_sih).
 * Depends on {@link DatalakeInfrastructureStack} for the S3 bucket and {@link DatabasePublicStack} for the RDS dev instance.
 */
public class ETLGlueJobStack extends Stack {

    /** Name of the SIH-SUS Glue ETL job. */
    public static final String SIH_SUS_JOB_NAME = "sih-sus-job";

    /** Glue catalog database (same as ETLGlueStack). */
    private static final String GLUE_DATABASE_NAME = "datalake_csv";
    /** Default catalog table for SIH. Crawler on path raw/sih/ creates a table named "sih" (last path segment). */
    private static final String DEFAULT_SIH_CATALOG_TABLE = "sih";
    /** RDS/output table name. */
    private static final String OUTPUT_TABLE_ST_SIH = "st_sih";

    /** External PostgreSQL database configuration. */
    private static final String EXTERNAL_DB_HOST = "31.97.29.233";
    private static final String EXTERNAL_DB_NAME = "analytics_datasus";
    private static final String EXTERNAL_DB_USER = "postgres";
    private static final String EXTERNAL_DB_PASSWORD = "<alterar_senha>";
    private static final String EXTERNAL_JDBC_URL = "jdbc:postgresql://" + EXTERNAL_DB_HOST + ":5432/" + EXTERNAL_DB_NAME;

    private final IBucket dataLakeBucket;

    public ETLGlueJobStack(final Construct scope, final String id, final IBucket dataLakeBucket) {
        this(scope, id, null, dataLakeBucket);
    }

    public ETLGlueJobStack(
            final Construct scope,
            final String id,
            final StackProps props,
            final IBucket dataLakeBucket) {
        super(scope, id, props);
        this.dataLakeBucket = dataLakeBucket;

        // IAM role for the Glue job
        Role jobRole = Role.Builder.create(this, "GlueSihJobRole")
                .assumedBy(new ServicePrincipal("glue.amazonaws.com"))
                .build();
        dataLakeBucket.grantRead(jobRole);
        String catalogArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":catalog";
        String databaseArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":database/" + GLUE_DATABASE_NAME;
        String databaseDefaultArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":database/default";
        String tableArn = "arn:aws:glue:" + getRegion() + ":" + getAccount() + ":table/" + GLUE_DATABASE_NAME + "/*";
        jobRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of(
                        "glue:GetTable", "glue:GetTables",
                        "glue:GetDatabase", "glue:GetDatabases",
                        "glue:GetPartition", "glue:GetPartitions"))
                .resources(List.of(catalogArn, databaseArn, databaseDefaultArn, tableArn))
                .build());
        // CreateDatabase required when --enable-glue-datacatalog: Spark may create the "default" database
        jobRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("glue:CreateDatabase"))
                .resources(List.of(catalogArn, databaseDefaultArn))
                .build());
        jobRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("glue:GetConnection"))
                .resources(List.of(catalogArn))
                .build());
        String glueLogArn = "arn:aws:logs:" + getRegion() + ":" + getAccount() + ":log-group:/aws-glue/jobs/*";
        jobRole.addToPrincipalPolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW)
                .actions(List.of("logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"))
                .resources(List.of(glueLogArn))
                .build());
        // Script asset (glue-scripts/sih_to_rds.py)
        Asset scriptAsset = Asset.Builder.create(this, "SihToRdsScript")
                .path("glue-scripts/sih_to_rds.py")
                .build();
        scriptAsset.grantRead(jobRole);
        String scriptLocation = "s3://" + scriptAsset.getS3BucketName() + "/" + scriptAsset.getS3ObjectKey();

        // Glue ETL job (SIH). Catalog schema comes only from sih-columns.json (CfnTable in ETLGlueStack).
        CfnJob.Builder.create(this, "SihToRdsJob")
                .name(SIH_SUS_JOB_NAME)
                .role(jobRole.getRoleArn())
                .command(CfnJob.JobCommandProperty.builder()
                        .name("glueetl")
                        .scriptLocation(scriptLocation)
                        .pythonVersion("3")
                        .build())
                .defaultArguments(Map.of(
                        "--job-bookmark-option", "job-bookmark-disable",
                        "--enable-glue-datacatalog", "",
                        "--catalog_database", GLUE_DATABASE_NAME,
                        "--catalog_table", DEFAULT_SIH_CATALOG_TABLE,
                        "--jdbc_url", EXTERNAL_JDBC_URL,
                        "--db_user", EXTERNAL_DB_USER,
                        "--db_password", EXTERNAL_DB_PASSWORD,
                        "--output_table", OUTPUT_TABLE_ST_SIH))
                .glueVersion("4.0")
                .workerType("G.1X")
                .numberOfWorkers(2)
                .build();

        // Script asset (glue-scripts/dimensions_to_rds.py)
        Asset dimensionsScriptAsset = Asset.Builder.create(this, "DimensionsToRdsScript")
                .path("glue-scripts/dimensions_to_rds.py")
                .build();
        dimensionsScriptAsset.grantRead(jobRole);
        String dimensionsScriptLocation = "s3://" + dimensionsScriptAsset.getS3BucketName() + "/" + dimensionsScriptAsset.getS3ObjectKey();

        // Glue ETL job (Dimensions)
        CfnJob.Builder.create(this, "DimensionsToRdsJob")
                .name("dimensions-job")
                .role(jobRole.getRoleArn())
                .command(CfnJob.JobCommandProperty.builder()
                        .name("glueetl")
                        .scriptLocation(dimensionsScriptLocation)
                        .pythonVersion("3")
                        .build())
                .defaultArguments(Map.of(
                        "--job-bookmark-option", "job-bookmark-disable",
                        "--enable-glue-datacatalog", "",
                        "--catalog_database", GLUE_DATABASE_NAME,
                        "--jdbc_url", EXTERNAL_JDBC_URL,
                        "--db_user", EXTERNAL_DB_USER,
                        "--db_password", EXTERNAL_DB_PASSWORD))
                .glueVersion("4.0")
                .workerType("G.1X")
                .numberOfWorkers(2)
                .build();

        // Script asset (glue-scripts/dimensions_aux_to_rds.py)
        Asset dimensionsAuxScriptAsset = Asset.Builder.create(this, "DimensionsAuxToRdsScript")
                .path("glue-scripts/dimensions_aux_to_rds.py")
                .build();
        dimensionsAuxScriptAsset.grantRead(jobRole);
        String dimensionsAuxScriptLocation = "s3://" + dimensionsAuxScriptAsset.getS3BucketName() + "/" + dimensionsAuxScriptAsset.getS3ObjectKey();

        // Glue ETL job (Dimensions aux - simpler dimension tables only)
        CfnJob.Builder.create(this, "DimensionsAuxJob")
                .name("dimensions_aux")
                .role(jobRole.getRoleArn())
                .command(CfnJob.JobCommandProperty.builder()
                        .name("glueetl")
                        .scriptLocation(dimensionsAuxScriptLocation)
                        .pythonVersion("3")
                        .build())
                .defaultArguments(Map.of(
                        "--job-bookmark-option", "job-bookmark-disable",
                        "--enable-glue-datacatalog", "",
                        "--catalog_database", GLUE_DATABASE_NAME,
                        "--jdbc_url", EXTERNAL_JDBC_URL,
                        "--db_user", EXTERNAL_DB_USER,
                        "--db_password", EXTERNAL_DB_PASSWORD))
                .glueVersion("4.0")
                .workerType("G.1X")
                .numberOfWorkers(2)
                .build();
    }

    /** Data lake S3 bucket (from DatalakeInfrastructureStack). */
    public IBucket getDataLakeBucket() {
        return dataLakeBucket;
    }
}
