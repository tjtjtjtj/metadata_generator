TABLE_INPUT = {
    "Owner": "a_fake_owner",
    "Parameters": {
        "EXTERNAL": "TRUE"
    },
    "Retention": 0,
    "StorageDescriptor": {
        "BucketColumns": [],
        "Compressed":
        False,
        "InputFormat":
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "NumberOfBuckets":
        -1,
        "OutputFormat":
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "Parameters": {},
        "SerdeInfo": {
            "Parameters": {
                "serialization.format": "1"
            },
            "SerializationLibrary":
            "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        },
        "SkewedInfo": {
            "SkewedColumnNames": [],
            "SkewedColumnValueLocationMaps": {},
            "SkewedColumnValues": [],
        },
        "SortColumns": [],
        "StoredAsSubDirectories":
        False,
    },
    "TableType": "EXTERNAL_TABLE",
}

DATABASE_INPUT = {
    "Name":
    "testdatabase",
    "Description":
    "a testdatabase",
    "LocationUri":
    "",
    "Parameters": {},
    "CreateTableDefaultPermissions": [
        {
            "Principal": {
                "DataLakePrincipalIdentifier": "a_fake_owner"
            },
            "Permissions": ["ALL"],
        },
    ],
}