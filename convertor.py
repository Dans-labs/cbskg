import sqlite3
from rdflib import Graph, Namespace, Literal, RDF, RDFS
from mlcroissant import Dataset, Records, Field
from datetime import datetime
import json
from pathlib import Path
from typing import List

def convert_db_to_triples_and_croissant(db_path: Path) -> List[str]:
    """
    Convert a SQLite database to RDF triples and Croissant format.
    Returns a list of generated file paths.
    """
    output_files = []
    
    # Connect to SQLite database
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    for table in tables:
        table_name = table[0]
        
        # Get table columns
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
        
        # ... Rest of your existing RDF conversion code ...
        
        # Save RDF triples
        output_triples = db_path.parent / f"{table_name}_triples.ttl"
        g.serialize(output_triples, format="turtle")
        output_files.append(str(output_triples))
        
        # Create Croissant dataset
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
        
        # Create Records
        records = Records(
            dataset=dataset,
            record_set=fields,
            filters=[],
            debug=False
        )
        
        # Save Croissant metadata
        output_croissant = db_path.parent / f"{table_name}_croissant.json"
        with open(output_croissant, 'w') as f:
            json.dump(dataset.jsonld, f, indent=2)
        output_files.append(str(output_croissant))
    
    conn.close()
    return output_files

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


