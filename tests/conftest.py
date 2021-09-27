import os
from metadata_generator.metadata_generator import Table, Tables
import pytest
import boto3
from moto import mock_glue
from . import helpers


@pytest.fixture()
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


# このfixtureを使うとうまくいかない......
# テスト関数で@mock_glueをつけるとDBが見つからない。
# @mock_glueをはずと下記になる。
# The security token included in the request is invalid.
@pytest.fixture()
@mock_glue
def create_table(aws_credentials):
    database_name = "member"
    table_name = "members"
    table = Table(database_name, table_name)

    client = boto3.client("glue")
    helpers.create_database(client, database_name)

    table_input = helpers.create_table_input(database_name, table_name)
    helpers.create_table(client, database_name, table_name, table_input)

    return table
