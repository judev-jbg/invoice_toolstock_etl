"""
Load module for the SQL-based ETL process.
Handles uploading invoices to Google Drive.
"""
from src.drive_manager import DriveManager
from config.config import DRIVE_FOLDER, OUTPUT_FILENAME_TEMPLATE, TOKEN_PATH
from src.transform import generate_reference
from utils.logging_config import logger

def load_invoices_to_drive(invoices):
    """
    Uploads invoice data to Google Drive.
    
    Args:
        invoices (list): List of invoice dictionaries
        
    Returns:
        bool: True if all uploads successful, False otherwise
    """
    if not invoices:
        logger.warning("No invoices to upload")
        return True
    
    logger.info(f"Starting upload of {len(invoices)} invoices to Google Drive")
    
    # Initialize DriveManager
    drive_manager = DriveManager(token_path=str(TOKEN_PATH))
    
    try:
        # Authenticate
        if drive_manager.is_authenticated():
            print("✅ Ya autenticado, conectando...")
        else:
            print("🔐 Primera vez - se abrirá el navegador")
        
        drive_manager.authenticate()
        
        # Test connection
        if not drive_manager.test_connection():
            logger.error("❌ Error conectando con Google Drive")
            return False
        
        successful_uploads = 0
        failed_uploads = 0
        
        for invoice in invoices:
            try:
                # Generate filename
                reference = generate_reference(invoice)
                filename = OUTPUT_FILENAME_TEMPLATE.format(reference=reference)
                
                # Upload invoice
                success = drive_manager.upload_invoice_json(
                    invoice_data=invoice,
                    filename=filename,
                    folder_path=DRIVE_FOLDER
                )
                
                if success:
                    successful_uploads += 1
                else:
                    failed_uploads += 1
                    
            except Exception as e:
                logger.error(f"Error uploading invoice {invoice.get('id', 'unknown')}: {str(e)}")
                failed_uploads += 1

        drive_manager.cleanup_delayed_files()
        
        # Log results
        total_invoices = len(invoices)
        logger.info(f"📊 RESULTADOS DE SUBIDA:")
        logger.info(f"  ✅ Exitosos: {successful_uploads}")
        logger.info(f"  ❌ Fallidos: {failed_uploads}")
        logger.info(f"  📋 Total: {total_invoices}")
        
        if successful_uploads == total_invoices:
            logger.info("🎉 ¡Todas las facturas subidas correctamente!")
            return True
        elif successful_uploads > 0:
            logger.warning("⚠️ Algunas facturas subidas, pero hubo errores")
            return True
        else:
            logger.error("❌ No se pudieron subir facturas a Google Drive")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error general subiendo facturas: {e}")
        return False