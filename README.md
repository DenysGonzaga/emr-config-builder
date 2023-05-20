# EMR Config Builder

UNDER CONSTRUCTION -> It can be changed over development process.

## Objective

An easy manner to generate Apache Spark optimized AWS EMR cluster configurations using a REST API.
Service built using Python 3.11, Poetry, and AWS CDK.
All variable calculations are based on this AWS [paper](https://aws.amazon.com/pt/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/). </br >

## Requirements

| **Software**    | **Version** |
|-----------------|-------------|
| Python          | 3.11.2      |
| Poetry          | 1.4.2       |
| CDK (on Poetry) | 2.72.1      |
| Docker          | 20.10.24    |
| AWS cli         | 2.11.8      |

## Starting

Set you AWS environment.
```
aws configure
```

Init poetry to create virtual env and install dependencies.
```
cd infra
poetry init
```

## Deploy

Deploy using cdk with poetry.
```
poetry run cdk synth
poetry run cdk deploy
```

## Design

![image](./assets/design/emr_config_builder.png)