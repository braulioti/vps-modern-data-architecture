package io.brau;

import software.constructs.Construct;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.ecr.Repository;

public class DatalakeInfrastructureStack extends Stack {

    private static final String ECR_REPOSITORY_NAME = "sih-sus-repo";

    public DatalakeInfrastructureStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public DatalakeInfrastructureStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        Repository.Builder.create(this, "SihSusRepo")
                .repositoryName(ECR_REPOSITORY_NAME)
                .build();
    }
}
