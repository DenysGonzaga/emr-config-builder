from aws_cdk import (
    Duration,
    Stack,
    aws_dynamodb   as dynamodb,
    aws_lambda     as lambda_,
    aws_iam        as iam,
    aws_apigateway as api_gateway 
)
from constructs import Construct


class EmrConfigBuilder(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.build_tables()
        self.build_lambda_functions()
        self.build_apis()


    def build_tables(self):
        self.dyn_meta_table = dynamodb.Table(self, "ECBMetadataTable",
            table_name="ecb_metadata_table",
            partition_key=dynamodb.Attribute(name="category", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="key", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PROVISIONED
        )
        self.dyn_meta_table.auto_scale_write_capacity(
            min_capacity=5,
            max_capacity=5
        ).scale_on_utilization(target_utilization_percent=75)


    def build_lambda_functions(self):
        self.api_lambda =  lambda_.DockerImageFunction(self, "ECBApiLambda",
                                        function_name="ecb_api_lambda",
                                        memory_size=128,
                                        code=lambda_.DockerImageCode.from_image_asset("../service/api_lambda"))
        
        self.load_lambda =  lambda_.DockerImageFunction(self, "ECBTableLoad",
                                function_name="ecb_table_load",
                                memory_size=256,
                                environment={
                                    "YARN_DATA_URL": "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html",
                                    "CORES_DATA_URL": "https://aws.amazon.com/pt/ec2/instance-types/",
                                    "DYN_TABLE_NAME": self.dyn_meta_table.table_name,
                                },
                                timeout=Duration.minutes(3),
                                code=lambda_.DockerImageCode.from_image_asset("../service/dynamo_load_lambda"))

        self.load_lambda.add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "dynamodb:BatchGetItem",
                    "dynamodb:GetItem",
                    "dynamodb:Query",
                    "dynamodb:Scan",
                    "dynamodb:BatchWriteItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem"
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.dyn_meta_table.table_name}",
                ],
            ))
        

    def build_apis(self):
        self.backend_api = api_gateway.LambdaRestApi(self, "ECBConfigRestApi",
            rest_api_name="api_ecb_config",
            deploy=True,
            proxy=False,
            handler=self.api_lambda
        )
        rsconfig = self.backend_api.root.add_resource("config")
        rsconfig.add_method("GET")

