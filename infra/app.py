#!/usr/bin/env python3
import os

import aws_cdk as cdk

from ecb.ecb_config import EmrConfigBuild


app = cdk.App()
EmrConfigBuild(app, "EmrConfigBuild")

app.synth()
