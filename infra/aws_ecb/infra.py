from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    aws_dynamodb       as dynamodb,
    aws_lambda         as lambda_,
    aws_iam            as iam,
    aws_apigateway     as api_gateway,
    aws_events         as events,
    aws_events_targets as targets
)
from constructs import Construct


class EmrConfigBuilder(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        self.build_tables()
        self.build_lambda_functions()
        self.build_apis()
        self.build_rules()


    def build_tables(self):
        # Create Metadata Table to store whole data regarding instance informations
        self.metadata_table = dynamodb.Table(self, "ECBMetadataTable",
            table_name="ecb_metadata_table",
            partition_key=dynamodb.Attribute(name="category", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="key", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            removal_policy=RemovalPolicy.DESTROY            
        )
        self.metadata_table.auto_scale_write_capacity(
            min_capacity=5,
            max_capacity=5
        ).scale_on_utilization(target_utilization_percent=75)


    def build_lambda_functions(self):      
        
        # Create Spider function to crawl instances data
        self.spider_function =  lambda_.DockerImageFunction(self, "ECBInstanceSpiderFunction",
                                function_name="ecb_instance_spider",
                                memory_size=256,
                                timeout=Duration.minutes(3),
                                tracing=lambda_.Tracing.ACTIVE,
                                code=lambda_.DockerImageCode.from_image_asset("../services/spider_lambda"),
                                environment={
                                    "YARN_DATA_URL": "https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html",
                                    "CORES_DATA_URL": "https://aws.amazon.com/pt/ec2/instance-types/",
                                    "DYN_TABLE_NAME": self.metadata_table.table_name,
                                })        
        
        self.spider_function.apply_removal_policy(RemovalPolicy.DESTROY)
        # Dynamodb permissions
        self.spider_function.add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "dynamodb:PutItem"
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.metadata_table.table_name}",
                ],
            ))
        
        # Create Backend function to connect API and metadata table
        self.backend_lambda =  lambda_.DockerImageFunction(self, "ECBBackendLambda",
                                        function_name="ecb_backend_lambda",
                                        memory_size=128,
                                        tracing=lambda_.Tracing.ACTIVE,
                                        code=lambda_.DockerImageCode.from_image_asset("../services/backend_lambda"),
                                        environment={
                                            "DYN_TABLE_NAME": self.metadata_table.table_name,
                                            "SPI_LBMDA_NAME": self.spider_function.function_name
                                        })
        self.backend_lambda.apply_removal_policy(RemovalPolicy.DESTROY)
        # Dynamodb permissions
        self.backend_lambda.add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[                    
                    "dynamodb:GetItem",
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem"
                ],
                resources=[
                    f"arn:aws:dynamodb:{self.region}:{self.account}:table/{self.metadata_table.table_name}",
                ],
            ))
        # Lambda permissions
        self.backend_lambda.add_to_role_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[                    
                    "lambda:InvokeFunction"
                ],
                resources=[
                    f"arn:aws:lambda:{self.region}:{self.account}:function:{self.spider_function.function_name}",
                ],
            ))
        

    def build_apis(self):
        # Create AGW api rest to serve as a interface between backend and the api user
        self.rest_api = api_gateway.LambdaRestApi(self, "ECBRestApi",
            rest_api_name="ecb_api",            
            deploy=True,
            proxy=False,
            handler=self.backend_lambda
        )
        rsconfig = self.rest_api.root.add_resource("config")
        rsconfig.add_method("GET")
        rsconfig.add_method("POST")
        self.rest_api.apply_removal_policy(RemovalPolicy.DESTROY)


    def build_rules(self):
        # Create CRON Rules to update metadata periodically
        cron_expression = self.node.try_get_context("spider_cron_expression_schedule")
        enable_rule = self.node.try_get_context("enable_spider_scheduler")

        if enable_rule:
            self.spider_cron_rule = events.Rule(self, 'ECBSpiderCronRule',
                                                schedule= events.Schedule.expression("cron(0 5 * * ? *)"))
            self.spider_cron_rule.add_target(targets.LambdaFunction(self.spider_function, retry_attempts=2))
            self.spider_cron_rule.apply_removal_policy(RemovalPolicy.DESTROY)
