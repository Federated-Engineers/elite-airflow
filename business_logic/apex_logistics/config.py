REGION = "eu-central-1"

# Cluster
CLUSTER_NAME = "apex-logistics-dev-cluster"
RELEASE_LABEL = "emr-spark-8.0.0"
IDLE_TIMEOUT_SECONDS = 1200

# IAM and network
SERVICE_ROLE = "{{ var.json.apex_logistics_emr_config.service_role }}"
INSTANCE_PROFILE = "{{ var.json.apex_logistics_emr_config.instance_profile }}"
SUBNET_IDS = "{{ var.json.apex_logistics_emr_config.subnet_ids }}"
PRIMARY_SECURITY_GROUP_ID = (
    "{{ var.json.apex_logistics_emr_config.primary_security_group_id }}"
)
CORE_TASK_SECURITY_GROUP_ID = (
    "{{ var.json.apex_logistics_emr_config.core_task_security_group_id }}"
)

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
