from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import sqlite3
import tempfile
import os
from pathlib import Path
import shutil
from typing import List
import uvicorn

from convertor import convert_db_to_triples_and_croissant

app = FastAPI(
    title="Database Converter API",
    description="Convert SQLite databases to RDF triples and Croissant format",
    version="1.0.0"
)

# Create a temporary directory for uploaded files
UPLOAD_DIR = Path("temp_uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

@app.post("/convert/", response_model=List[str])
async def convert_database(file: UploadFile = File(...)):
    """
    Upload a SQLite database and convert it to RDF triples and Croissant format.
    Returns paths to the generated files.
    """
    if not file.filename.endswith('.db'):
        raise HTTPException(status_code=400, detail="File must be a SQLite database (.db)")
    
    try:
        # Save uploaded file
        db_path = UPLOAD_DIR / file.filename
        with open(db_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # Convert the database
        output_files = convert_db_to_triples_and_croissant(db_path)
        
        return output_files
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        # Clean up uploaded file
        if db_path.exists():
            db_path.unlink()

@app.get("/download/{filename}")
async def download_file(filename: str):
    """Download a converted file."""
    file_path = UPLOAD_DIR / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type='application/octet-stream'
    )

@app.on_event("shutdown")
async def cleanup():
    """Clean up temporary files on shutdown."""
    shutil.rmtree(UPLOAD_DIR, ignore_errors=True)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 