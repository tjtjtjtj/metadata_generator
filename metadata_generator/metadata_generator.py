import boto3
from dataclasses import dataclass
from pathlib import Path

here = Path(__file__).parent
DOCS_PATH = here / "docs"
EXCLUDE_FILE_SUFFIX = "_column.md"


def main():
    for table in Tables.build_tables(DOCS_PATH, EXCLUDE_FILE_SUFFIX).data:
        output_markdown = (
            DOCS_PATH / table.database_name / f"{table.table_name}{EXCLUDE_FILE_SUFFIX}"
        )
        with output_markdown.open(mode="w") as f:
            f.write(table.make_markdown())


@dataclass
class Table:
    database_name: str
    table_name: str

    def __post_init__(self):
        self.client = boto3.client("glue")

    def validate(self):
        if not self.database_name:
            raise ValueError("Invalid table.database_name")
        if not self.table_name:
            raise ValueError("Invalid table.table_name")

    def retrive_glue_columns(self):
        res = self.client.get_table(
            DatabaseName=self.database_name, Name=self.table_name
        )
        return res["Table"]["StorageDescriptor"]["Columns"]

    def make_markdown(self):
        markdown = "|Name|Comment|\n" + "|---|---|\n"
        for i in self.retrive_glue_columns():
            markdown += f'|{i["Name"]}|{i["Comment"]}|\n'
        return markdown


@dataclass
class Tables:
    data: list[Table]

    @classmethod
    def build_tables(cls, path, exclude_file_suffix):
        data = []
        for f in list(path.glob("*/*")):
            if not f.name.endswith(exclude_file_suffix):
                database_name = f.parent.stem
                table_name = f.stem
                table = Table(database_name, table_name)
                table.validate()
                data.append(table)
        return cls(data=data)


if __name__ == "__main__":
    main()
