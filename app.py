#!/usr/bin/env python3
import aws_cdk as cdk
from src import config

from src.glue_stack import GlueStackClass

app = cdk.App()


glue_cdk_stack = GlueStackClass(
    scope=app,
    construct_id="cdk-glue-demo",
    account_id=config.ACCOUNT_ID,
    region=config.REGION,
)

app.synth()
