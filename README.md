# EMR Config Builder

UNDER CONSTRUCTION -> It can be changed over development process.

The main objective of this project is an easy manner to create a simple rest API using AWS CDK and Python to generate EMR configurations based on this AWS [paper](https://aws.amazon.com/pt/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/). </br >
It can be helpful when you run your EMR clusters and need to generate configurations on the fly using an API.

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