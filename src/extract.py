"""
Extract module for the SQL-based ETL process.
Handles database connection and data extraction.
"""
import pandas as pd
import pyodbc
from config.config import DB_CONFIG, INVOICE_QUERY
from utils.logging_config import logger

def get_connection_string():
    """
    Creates a connection string for SQL Server.
    
    Returns:
        str: Connection string for pyodbc
    """
    if DB_CONFIG['trusted_connection'].lower() == 'yes':
        return (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"Trusted_Connection=yes;"
        )
    else:
        return (
            f"DRIVER={DB_CONFIG['driver']};"
            f"SERVER={DB_CONFIG['server']};"
            f"DATABASE={DB_CONFIG['database']};"
            f"UID={DB_CONFIG['username']};"
            f"PWD={DB_CONFIG['password']};"
        )

def extract_invoice_data():
    """
    Extracts invoice data from SQL Server database.
    
    Returns:
        pd.DataFrame: DataFrame with invoice data or None if error occurs
    """
    connection = None
    try:
        logger.info("Connecting to SQL Server database")
        connection_string = get_connection_string()
        connection = pyodbc.connect(connection_string)
        
        logger.info("Executing invoice query")
        df = pd.read_sql(INVOICE_QUERY, connection)
        
        logger.info(f"Successfully extracted {len(df)} rows from database")
        return df
        
    except pyodbc.Error as e:
        logger.error(f"Database error during extraction: {str(e)}")
        return None
        
    except Exception as e:
        logger.error(f"Error during data extraction: {str(e)}", exc_info=True)
        return None
        
    finally:
        if connection:
            connection.close()
            logger.info("Database connection closed")

def test_connection():
    """
    Tests the database connection.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    connection = None
    try:
        logger.info("Testing database connection")
        connection_string = get_connection_string()
        connection = pyodbc.connect(connection_string)
        
        # Test with a simple query
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        
        logger.info("Database connection test successful")
        return True
        
    except Exception as e:
        logger.error(f"Database connection test failed: {str(e)}")
        return False
        
    finally:
        if connection:
            connection.close()