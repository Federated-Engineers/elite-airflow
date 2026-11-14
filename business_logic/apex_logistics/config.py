REGION = "eu-central-1"

# Cluster
CLUSTER_NAME = "apex-logistics-dev-cluster"
RELEASE_LABEL = "emr-spark-8.0.0"
IDLE_TIMEOUT_SECONDS = 1200

# IAM
SERVICE_ROLE = "elite-emr-staging-service-role"
INSTANCE_PROFILE = "elite-emr-staging-apex-logistics-instance-role"

# Network
SUBNET_IDS = [
    "subnet-007890019937ed2e9",
    "subnet-02dea4ee39939e241",
    "subnet-00ca9b61fa08f7f1d",
]
PRIMARY_SECURITY_GROUP_ID = "sg-0748e8892e400fd90"
CORE_TASK_SECURITY_GROUP_ID = "sg-02c10add6f7b15d65"

# S3
LOG_BUCKET = "federated-engineers-staging-elite-apex-logistics-emr-logs"
LOG_PREFIX = "emr"
LOG_URI = f"s3://{LOG_BUCKET}/{LOG_PREFIX}/"
SMOKE_TEST_SCRIPT = (
    "s3://spark-apex-freight-data/artifacts/dev/smoke_test/smoke_test.py"
)

# Instance fleets
PRIMARY_INSTANCE_TYPES = [
    {"InstanceType": "m7g.xlarge"},
]

WORKER_INSTANCE_TYPES = [
    {"InstanceType": "r7g.xlarge"},
    {"InstanceType": "r8g.xlarge"},
    {"InstanceType": "r6g.xlarge"},
]

SPOT_FALLBACK = {
    "SpotSpecification": {
        "TimeoutDurationMinutes": 10,
        "TimeoutAction": "SWITCH_TO_ON_DEMAND",
    }
}

INSTANCE_FLEETS = [
    {
        "InstanceFleetType": "MASTER",
        "TargetOnDemandCapacity": 1,
        "InstanceTypeConfigs": PRIMARY_INSTANCE_TYPES,
    },
    {
        "InstanceFleetType": "CORE",
        "TargetSpotCapacity": 1,
        "InstanceTypeConfigs": WORKER_INSTANCE_TYPES,
        "LaunchSpecifications": SPOT_FALLBACK,
    },
    {
        "InstanceFleetType": "TASK",
        "TargetSpotCapacity": 2,
        "InstanceTypeConfigs": WORKER_INSTANCE_TYPES,
        "LaunchSpecifications": SPOT_FALLBACK,
    },
]

# Cluster definition
JOB_FLOW_OVERRIDES = {
    "Name": CLUSTER_NAME,
    "ReleaseLabel": RELEASE_LABEL,
    "Applications": [{"Name": "Spark"}],
    "ServiceRole": SERVICE_ROLE,
    "JobFlowRole": INSTANCE_PROFILE,
    "LogUri": LOG_URI,
    "Instances": {
        "InstanceFleets": INSTANCE_FLEETS,
        "Ec2SubnetIds": SUBNET_IDS,
        "EmrManagedMasterSecurityGroup": PRIMARY_SECURITY_GROUP_ID,
        "EmrManagedSlaveSecurityGroup": CORE_TASK_SECURITY_GROUP_ID,
        "KeepJobFlowAliveWhenNoSteps": True,
    },
    "AutoTerminationPolicy": {"IdleTimeout": IDLE_TIMEOUT_SECONDS},
    "Tags": [
        {"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"},
    ],
}

# Steps
SMOKE_TEST_STEPS = [
    {
        "Name": "smoke-test",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "client",
                SMOKE_TEST_SCRIPT,
            ],
        },
    }
]
