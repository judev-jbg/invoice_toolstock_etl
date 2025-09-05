"""
Main module for the SQL-based ETL process.
Orchestrates the extraction, transformation, and loading of invoice data.
"""
import time
from src.extract import extract_invoice_data, test_connection
from src.transform import transform_to_invoices
from src.load import load_invoices_to_drive
from utils.logging_config import logger

def main():
    """
    Orchestrates the ETL process for SQL Server to Google Drive.
    """
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("STARTING SQL-BASED INVOICE ETL PROCESS")
    logger.info("=" * 60)
    
    try:
        # Test database connection first
        logger.info("Testing database connection...")
        if not test_connection():
            logger.error("❌ Database connection failed. Aborting ETL process.")
            return False
        
        # Extract data from SQL Server
        logger.info("📤 Extracting invoice data from SQL Server...")
        df = extract_invoice_data()
        
        if df is None or df.empty:
            logger.error("❌ No data extracted from database. Aborting ETL process.")
            return False
        
        logger.info(f"✅ Successfully extracted {len(df)} rows from database")
        
        # Transform data to invoice structure
        logger.info("🔄 Transforming data to invoice JSON structure...")
        invoices = transform_to_invoices(df)
        
        if not invoices:
            logger.error("❌ No invoices generated during transformation. Aborting ETL process.")
            return False
        
        logger.info(f"✅ Successfully transformed data into {len(invoices)} invoices")
        
        # Load invoices to Google Drive
        logger.info("📤 Uploading invoices to Google Drive...")
        success = load_invoices_to_drive(invoices)
        
        
        # Calculate execution time
        execution_time = time.time() - start_time
        
        if success:
            logger.info("=" * 60)
            logger.info("🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
            logger.info(f"⏱️  Execution time: {execution_time:.2f} seconds")
            logger.info(f"📊 Processed {len(invoices)} invoices")
            logger.info("=" * 60)
            return True
        else:
            logger.error("=" * 60)
            logger.error("❌ ETL PROCESS FAILED DURING UPLOAD")
            logger.error(f"⏱️  Execution time: {execution_time:.2f} seconds")
            logger.error("=" * 60)
            return False
            
    except Exception as e:
        execution_time = time.time() - start_time
        logger.critical("=" * 60)
        logger.critical("💥 CRITICAL ERROR IN ETL PROCESS")
        logger.critical(f"⏱️  Execution time: {execution_time:.2f} seconds")
        logger.critical(f"🔥 Error: {str(e)}")
        logger.critical("=" * 60, exc_info=True)
        return False

if __name__ == "__main__":
    try:
        success = main()
        exit_code = 0 if success else 1
        exit(exit_code)
    except KeyboardInterrupt:
        logger.info("🛑 ETL process interrupted by user")
        exit(130)
    except Exception as e:
        logger.critical(f"💥 Unhandled exception: {str(e)}", exc_info=True)
        exit(1)