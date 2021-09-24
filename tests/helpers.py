import copy

from .fixtures.datacatalog import TABLE_INPUT, DATABASE_INPUT


def create_database_input(database_name):
    database_input = copy.deepcopy(DATABASE_INPUT)
    database_input["Name"] = database_name
    database_input["LocationUri"] = "s3://my-bucket/{database_name}".format(
        database_name=database_name
    )
    return database_input


def create_database(client, database_name, database_input=None):
    if database_input is None:
        database_input = create_database_input(database_name)
    return client.create_database(DatabaseInput=database_input)


def create_table_input(database_name, table_name, columns=[], partition_keys=[]):
    table_input = copy.deepcopy(TABLE_INPUT)
    table_input["Name"] = table_name
    table_input["PartitionKeys"] = partition_keys
    table_input["StorageDescriptor"]["Columns"] = columns
    table_input["StorageDescriptor"][
        "Location"
    ] = "s3://my-bucket/{database_name}/{table_name}".format(
        database_name=database_name, table_name=table_name
    )
    return table_input


def create_table(client, database_name, table_name, table_input=None, **kwargs):
    if table_input is None:
        table_input = create_table_input(database_name, table_name, **kwargs)

    return client.create_table(DatabaseName=database_name, TableInput=table_input)