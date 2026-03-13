package io.brau;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awscdk.services.ec2.InterfaceVpcEndpointAwsService;
import software.amazon.awscdk.services.ec2.InterfaceVpcEndpointOptions;
import software.amazon.awscdk.services.ec2.SubnetConfiguration;
import software.amazon.awscdk.services.ec2.SubnetSelection;
import software.amazon.awscdk.services.ec2.SubnetType;
import software.amazon.awscdk.services.ec2.Vpc;
import software.amazon.awscdk.services.ecr.IRepository;
import java.util.List;
import software.amazon.awscdk.services.ecs.Cluster;
import software.amazon.awscdk.services.iam.ServicePrincipal;
import software.amazon.awscdk.services.ecs.ContainerDefinitionOptions;
import software.amazon.awscdk.services.ecs.ContainerImage;
import software.amazon.awscdk.services.ecs.FargateTaskDefinition;
import software.amazon.awscdk.services.ecs.AwsLogDriverProps;
import software.amazon.awscdk.services.ecs.LogDriver;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.s3.Bucket;

public class DatalakeInfrastructureStack extends Stack {

    private final Vpc vpc;
    private final Bucket bucket;

    private static final String VPC_NAME = "datalake-vpc";
    private static final String BUCKET_NAME = "braulioti-datalake-bucket";
    private static final String CLUSTER_NAME = "datalake-cluster";
    private static final String TASK_DEFINITION_FAMILY = "sih-sus-task";

    /** Environment variables for the task (aligned with docker-compose and .env). AWS_S3_BUCKET is added from the bucket name. */
    private static final Map<String, String> TASK_ENV = Map.of(
            "START_YEAR", "2025",
            "START_MONTH", "10",
            "END_YEAR", "2025",
            "END_MONTH", "11",
            "STATES", "MG,SP,RJ"
    );

    public DatalakeInfrastructureStack(final Construct scope, final String id, final IRepository ecrRepository) {
        this(scope, id, null, ecrRepository);
    }

    public DatalakeInfrastructureStack(final Construct scope, final String id, final StackProps props, final IRepository ecrRepository) {
        super(scope, id, props);

        // natGateways(0) = no NAT cost. Use Public + Private (PRIVATE_WITH_EGRESS) so template matches existing deployed subnets (PrivateSubnet1/2/3); using Isolated would create new subnets and conflict with existing CIDRs.
        this.vpc = Vpc.Builder.create(this, "DatalakeVPC")
                .vpcName(VPC_NAME)
                .natGateways(0)
                .subnetConfiguration(List.of(
                        SubnetConfiguration.builder()
                                .name("Public")
                                .subnetType(SubnetType.PUBLIC)
                                .cidrMask(19)
                                .build(),
                        SubnetConfiguration.builder()
                                .name("Private")
                                .subnetType(SubnetType.PRIVATE_WITH_EGRESS)
                                .cidrMask(19)
                                .build()))
                .build();

        // Secrets Manager interface endpoint so Glue (and other resources in VPC without NAT) can reach the API
        this.vpc.addInterfaceEndpoint("SecretsManager", InterfaceVpcEndpointOptions.builder()
                .service(InterfaceVpcEndpointAwsService.SECRETS_MANAGER)
                .privateDnsEnabled(true)
                .subnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build());

        // Glue API interface endpoint so the Glue job in VPC can reach Data Catalog (glue.us-east-1.amazonaws.com) without NAT
        this.vpc.addInterfaceEndpoint("Glue", InterfaceVpcEndpointOptions.builder()
                .service(InterfaceVpcEndpointAwsService.GLUE)
                .privateDnsEnabled(true)
                .subnets(SubnetSelection.builder().subnetType(SubnetType.PUBLIC).build())
                .build());

        this.bucket = Bucket.Builder.create(this, "DatalakeBucketSih")
                .bucketName(BUCKET_NAME)
                .build();

        Role taskRole = Role.Builder.create(this, "SihSusTaskRole")
                .roleName("sih-sus-task-role")
                .assumedBy(new ServicePrincipal("ecs-tasks.amazonaws.com"))
                .build();
        bucket.grantReadWrite(taskRole);

        Cluster.Builder.create(this, "DatalakeCluster")
                .clusterName(CLUSTER_NAME)
                .vpc(this.vpc)
                .build();

        LogGroup logGroup = LogGroup.Builder.create(this, "SihSusLogGroup")
                .logGroupName("/ecs/" + TASK_DEFINITION_FAMILY)
                .build();

        FargateTaskDefinition taskDef = FargateTaskDefinition.Builder.create(this, "SihSusTask")
                .family(TASK_DEFINITION_FAMILY)
                .cpu(1024)
                .memoryLimitMiB(2048)
                .taskRole(taskRole)
                .build();

        Map<String, String> taskEnv = new HashMap<>(TASK_ENV);
        taskEnv.put("AWS_S3_BUCKET", bucket.getBucketName());

        taskDef.addContainer("sih-sus", ContainerDefinitionOptions.builder()
                .image(ContainerImage.fromEcrRepository(ecrRepository, "latest"))
                .environment(taskEnv)
                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                        .logGroup(logGroup)
                        .streamPrefix("sih-sus")
                        .build()))
                .build());
    }

    public Vpc getVpc() {
        return vpc;
    }

    public Bucket getBucket() {
        return bucket;
    }
}
