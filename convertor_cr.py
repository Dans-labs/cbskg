import sqlite3
import json
from rdflib import Graph, Namespace, Literal, RDF, RDFS

# Connect to SQLite database
dbname = 'dsc.db'
conn = sqlite3.connect(dbname)
cursor = conn.cursor()

# Replace with your actual table name
table_name = 'var'
cursor.execute(f"PRAGMA table_info('{table_name}')")
columns = cursor.fetchall()

# Create RDF graph
g = Graph()
DB = Namespace("http://cbs.nl/db#")
g.bind("db", DB)

# Add triples for table metadata
table_uri = DB[table_name]
g.add((table_uri, RDF.type, DB.Table))
g.add((table_uri, RDFS.label, Literal(table_name)))

for col_id, name, col_type, not_null, default, primary_key in columns:
    column_uri = DB[name]
    g.add((column_uri, RDF.type, DB.Column))
    g.add((column_uri, RDFS.label, Literal(name)))
    g.add((column_uri, DB.columnType, Literal(col_type)))
    g.add((column_uri, DB.notNull, Literal(bool(not_null))))
    g.add((column_uri, DB.defaultValue, Literal(default if default else "None")))
    g.add((column_uri, DB.primaryKey, Literal(bool(primary_key))))
    g.add((table_uri, DB.hasColumn, column_uri))

# Fetch table data
cursor.execute(f"SELECT * FROM {table_name}")
rows = cursor.fetchall()

# Add table data to the graph
for row_index, row in enumerate(rows):
    row_uri = DB[f"{table_name}_row_{row_index + 1}"]
    g.add((row_uri, RDF.type, DB.Row))
    g.add((table_uri, DB.hasRow, row_uri))
    for col_index, value in enumerate(row):
        col_name = columns[col_index][1]  # Get column name
        column_uri = DB[col_name]
        g.add((row_uri, column_uri, Literal(value)))

# Serialize the graph as RDF triples
output_file = "output_triples.ttl"
g.serialize(output_file, format="turtle")
print(f"RDF triples saved to {output_file}")

# Add SPARQL query to inspect noteDefinition
query = """
SELECT ?subject ?predicate ?object
WHERE {
    ?subject db:noteDefinition ?object .
}
LIMIT 10
"""

print("\nQuerying noteDefinition:")
for row in g.query(query):
    print(f"Subject: {row.subject}")
    print(f"Object: {row.object}")
    print("---")

# Close database connection
conn.close()

def map_sql_to_schema_type(sql_type):
    """Map SQL types to schema.org data types"""
    type_mapping = {
        'INTEGER': 'sc:Integer',
        'TEXT': 'sc:Text',
        'REAL': 'sc:Float',
        'BLOB': 'sc:Text',
        'VARCHAR': 'sc:Text',
        'BOOLEAN': 'sc:Boolean'
    }
    sql_type = sql_type.upper()
    return type_mapping.get(sql_type, 'sc:Text')

# Create Croissant JSON-LD structure
croissant_data = {
    "@context": {
        "sc": "http://schema.org/",
        "cr": "http://mlcommons.org/croissant/"
    },
    "@type": "sc:Dataset",
    "name": table_name,
    "description": f"Database table {table_name} converted to Croissant format",
    "url": f"http://cbs.nl/db#{table_name}",
    "distribution": [],
    "recordSet": []
}

# Create record set structure
record_set = {
    "@type": "cr:RecordSet",
    "name": "default",
    "description": f"Records from {table_name} table",
    "field": []
}

# Add fields based on columns
for col_id, name, col_type, not_null, default, primary_key in columns:
    field = {
        "@type": "cr:Field",
        "name": name,
        "description": f"Column {name} from {table_name}",
        "dataType": map_sql_to_schema_type(col_type),  # You'll need to implement this
        "required": bool(not_null)
    }
    record_set["field"].append(field)

croissant_data["recordSet"].append(record_set)

# Write Croissant JSON-LD to file
output_file = f"{table_name}_croissant.json"
with open(output_file, 'w') as f:
    json.dump(croissant_data, f, indent=2)

print(f"Croissant metadata saved to {output_file}")


