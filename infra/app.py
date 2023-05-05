#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_ecb.infra import EmrConfigBuilder

app = cdk.App()
EmrConfigBuilder(app, "EmrConfigBuilder")

app.synth()
