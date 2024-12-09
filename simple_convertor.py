import sqlite3
from rdflib import Graph, Namespace, Literal, RDF, RDFS
from mlcroissant import Dataset, Records, Field
from datetime import datetime
import json

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

def map_sql_to_data_type(sql_type):
    """Map SQL types to Croissant data types"""
    sql_type = sql_type.upper()
    type_mapping = {
        'INTEGER': 'integer',
        'TEXT': 'string',
        'REAL': 'float',
        'BLOB': 'string',
        'VARCHAR': 'string',
        'BOOLEAN': 'boolean'
    }
    return type_mapping.get(sql_type, 'string')

# Create Croissant dataset using mlcroissant
dataset = Dataset({
    "@context": {
        "@vocab": "http://schema.org/",
        "sc": "http://schema.org/",
        "cr": "http://mlcommons.org/croissant/"
    },
    "@type": ["https://schema.org/Dataset", "sc:Dataset"],
    "https://schema.org/name": table_name,
    "datePublished": datetime.now().strftime("%Y-%m-%d"),
    "https://schema.org/version": "1.0.0",
    "license": "https://creativecommons.org/licenses/by/4.0/",
    "citation": {
        "@type": "CreativeWork",
        "name": f"Citation for {table_name} dataset",
        "url": f"http://cbs.nl/db#{table_name}"
    }
})

# Create fields list
fields = []
for col_id, name, col_type, not_null, default, primary_key in columns:
    field = Field(
        name=name,
        description=f"Column {name} from {table_name}",
        data_types=map_sql_to_data_type(col_type)
    )
    fields.append(field)

# Create Records with
records = Records(
    dataset=dataset,
    record_set=fields,
    filters=[],
    debug=False
)

# Save the Croissant metadata
output_file = f"{table_name}_croissant.json"
with open(output_file, 'w') as f:
    json.dump(dataset.jsonld, f, indent=2)  # Use the jsonld property of the Dataset
print(f"Croissant metadata saved to {output_file}")


