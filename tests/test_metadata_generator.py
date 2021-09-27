from metadata_generator.metadata_generator import Table, Tables
import pytest
from pathlib import Path
import boto3
from moto import mock_glue
from . import helpers


class Test_Table:
    def test_validate_database_name(self):
        table = Table("", "test_table")
        with pytest.raises(ValueError):
            table.validate()

    def test_validate_table_name(self):
        table = Table("test_database", "")
        with pytest.raises(ValueError):
            table.validate()

    @mock_glue
    def test_retrive_glue_columns(self, aws_credentials):
        database_name = "member"
        table_name = "members"
        columns = [
            {"Name": "id", "Type": "string", "Comment": "ID", "Parameters": {}},
            {"Name": "name", "Type": "string", "Comment": "名前", "Parameters": {}},
        ]
        client = boto3.client("glue")
        helpers.create_database(client, database_name)

        table_input = helpers.create_table_input(
            database_name, table_name, columns=columns
        )
        helpers.create_table(client, database_name, table_name, table_input)

        table = Table(database_name, table_name)

        actual = table.retrive_glue_columns()
        assert len(actual) == 2
        assert actual[0] == {
            "Name": "id",
            "Type": "string",
            "Comment": "ID",
            "Parameters": {},
        }
        assert actual[1] == {
            "Name": "name",
            "Type": "string",
            "Comment": "名前",
            "Parameters": {},
        }

    @mock_glue
    def test_markdown(self, aws_credentials):
        database_name = "member"
        table_name = "members"
        columns = [
            {"Name": "id", "Type": "string", "Comment": "ID", "Parameters": {}},
            {"Name": "name", "Type": "string", "Comment": "名前", "Parameters": {}},
        ]
        client = boto3.client("glue")
        helpers.create_database(client, database_name)

        table_input = helpers.create_table_input(
            database_name, table_name, columns=columns
        )
        helpers.create_table(client, database_name, table_name, table_input)

        table = Table(database_name, table_name)

        actual = table.make_markdown()
        expected = """|Name|Comment|
|---|---|
|id|ID|
|name|名前|
"""
        assert actual == expected


class Test_Tables:
    def test_build_tables(self):
        here = Path(__file__).parent
        path = here / "fixtures/testdir"
        exclude_file_suffix = "_column.md"
        actual = Tables.build_tables(path, exclude_file_suffix)
        assert len(actual.data) == 2
        assert actual.data[0] == Table("member", "memberprofile")
        assert actual.data[1] == Table("member", "members")
