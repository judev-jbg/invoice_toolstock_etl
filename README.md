# SQL Invoice ETL

A Python-based ETL pipeline that extracts invoice data directly from SQL Server, transforms it into structured JSON format, and uploads individual invoice files to Google Drive using the Google Drive API.

## Features

- Direct SQL Server database connection
- Transforms invoice data into structured JSON format
- Calculates IVA totals automatically
- Uploads individual invoice files to Google Drive
- Comprehensive logging and error handling
- Avoids duplicate uploads

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```
