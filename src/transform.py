"""
Transform module for the SQL-based ETL process.
Handles data transformation to invoice JSON structure.
"""
import pandas as pd
import numpy as np
from utils.logging_config import logger

def transform_to_invoices(df):
    """
    Transforms the SQL result DataFrame into a list of invoice dictionaries.
    
    Args:
        df (pd.DataFrame): DataFrame from SQL query
        
    Returns:
        list: List of invoice dictionaries or None if error occurs
    """
    try:
        logger.info("Starting data transformation")
        
        if df is None or df.empty:
            logger.warning("No data to transform")
            return []
        
        invoices = []
        
        # Get unique invoice IDs
        unique_invoices = df['id'].unique()
        logger.info(f"Found {len(unique_invoices)} unique invoices to process")
        
        for invoice_id in unique_invoices:
            if pd.isna(invoice_id):
                continue
                
            # Filter data for this invoice
            invoice_rows = df[df['id'] == invoice_id]
            
            if invoice_rows.empty:
                continue
                
            # Get common invoice data from first row
            first_row = invoice_rows.iloc[0]
            
            # Calculate totals
            total_iva_excl = calculate_total_iva_excl(invoice_rows)
            total_iva_incl = calculate_total_iva_incl(invoice_rows)
            total_iva = total_iva_incl - total_iva_excl
            
            # Create invoice structure
            invoice = {
                "id": convert_to_native_type(first_row['id']),
                "num_factura": convert_to_native_type(first_row['num_factura']),
                "año_factura": convert_to_native_type(first_row['año_factura']),
                "fecha_factura": convert_to_native_type(first_row['fecha_factura']),
                "total_iva_excl": round(total_iva_excl, 2),
                "total_iva": round(total_iva, 2),
                "total_iva_incl": round(total_iva_incl, 2),
                "observaciones": convert_to_native_type(first_row['observaciones']) or "",
                "num_albaran": convert_to_native_type(first_row['num_albaran']),
                "fecha_albaran": convert_to_native_type(first_row['fecha_albaran']),
                "id_pedido": convert_to_native_type(first_row['id_pedido']),
                "num_pedido": convert_to_native_type(first_row['num_pedido']),
                "año_pedido": convert_to_native_type(first_row['año_pedido']),
                "fecha_pedido": convert_to_native_type(first_row['fecha_pedido']),
                "id_pedido_cliente": convert_to_native_type(first_row['id_pedido_cliente']),
                "id_cliente": convert_to_native_type(first_row['id_cliente']),
                "cliente": convert_to_native_type(first_row['cliente']),
                "direccion": convert_to_native_type(first_row['direccion']),
                "cod_postal": convert_to_native_type(first_row['cod_postal']),
                "ciudad": convert_to_native_type(first_row['ciudad']),
                "provincia": convert_to_native_type(first_row['provincia']),
                "pais": convert_to_native_type(first_row['pais']),
                "nif": convert_to_native_type(first_row['nif']),
                "products": []
            }
            
            # Add products
            for _, row in invoice_rows.iterrows():
                product = {
                    "product": {
                        "id_articulo": convert_to_native_type(row['id_articulo']),
                        "descripcion": convert_to_native_type(row['descripcion']),
                        "cantidad": convert_to_native_type(row['cantidad']),
                        "precio": convert_to_native_type(row['precio']),
                        "descuento": convert_to_native_type(row['descuento']),
                        "total": convert_to_native_type(row['total'])
                    }
                }
                invoice["products"].append(product)
            
            invoices.append(invoice)
            logger.debug(f"Processed invoice {invoice_id} with {len(invoice_rows)} products")
        
        logger.info(f"Transformation completed: {len(invoices)} invoices processed")
        return invoices
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}", exc_info=True)
        return None

def calculate_total_iva_excl(invoice_rows):
    """
    Calculates total excluding IVA.
    
    Args:
        invoice_rows (pd.DataFrame): Rows for a single invoice
        
    Returns:
        float: Total excluding IVA
    """
    return invoice_rows['total'].sum()

def calculate_total_iva_incl(invoice_rows):
    """
    Calculates total including IVA.
    
    Args:
        invoice_rows (pd.DataFrame): Rows for a single invoice
        
    Returns:
        float: Total including IVA
    """
    total_with_iva = 0
    for _, row in invoice_rows.iterrows():
        line_total = row['total']
        iva_rate = row['iva'] if not pd.isna(row['iva']) else 1.0
        total_with_iva += line_total * iva_rate
    
    return total_with_iva

def convert_to_native_type(value):
    """
    Converts numpy/pandas values to native Python types for JSON serialization.
    
    Args:
        value: Value to convert
        
    Returns:
        Value converted to native Python type
    """
    if pd.isna(value):
        return None
    elif isinstance(value, (np.integer, np.int64)):
        return int(value)
    elif isinstance(value, (np.floating, np.float64)):
        return float(value)
    elif isinstance(value, np.ndarray):
        return value.tolist()
    else:
        return value

def generate_reference(invoice):
    """
    Generates a reference for the invoice filename.
    
    Args:
        invoice (dict): Invoice dictionary
        
    Returns:
        str: Reference string for filename
    """
    id_pedido_cliente = invoice.get('id_pedido_cliente')
    if id_pedido_cliente:
        return str(id_pedido_cliente)
    else:
        return f"id_factura_{invoice['id']}"