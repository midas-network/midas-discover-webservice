from core.extent_table import ExtentTable
from core.table_maker import TableMaker


extent_table = ExtentTable()

table_maker = TableMaker(extent_table)

table_maker.save_tables("midas-api-data/GRANTS_ENDPOINT.json", export_as="sql")


with open('m') as json_data:
    d = json.load(json_data)

print(d)
