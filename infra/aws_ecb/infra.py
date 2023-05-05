from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb as dynamodb,
)
from constructs import Construct

class EmrConfigBuilder(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.build_tables()
        self.build_lambda_functions()
        self.build_apis()

    def build_tables(self):
        self.dyn_table = dynamodb.Table(self, "ECBMetadataTable",
            table_name="ecb_metadata_table",
            partition_key=dynamodb.Attribute(name="category", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="key", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PROVISIONED
        )
        self.dyn_table.auto_scale_write_capacity(
            min_capacity=5,
            max_capacity=5
        ).scale_on_utilization(target_utilization_percent=75)


    def build_lambda_functions(self):
        pass

    def build_apis(self):
        pass

