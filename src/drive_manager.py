"""
Google Drive manager for uploading invoice files.
Adapted from the existing DriveManager service.
"""
import os
import json
from pathlib import Path
import tempfile
import uuid
from typing import Optional
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError
import time
from utils.logging_config import logger

class DriveManager:
    """Clase para manejar la API de Google Drive"""
    
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(self, credentials_path: str = 'credentials.json', token_path: str = 'token.json'):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None
        self._folder_cache = {}
        self._delayed_cleanup_files = []

    
    def authenticate(self):
        """Autentica y crea el servicio de Google Drive con manejo robusto de tokens"""
        creds = None
        
        # Cargar token existente
        if not os.path.exists(self.token_path):
            raise FileNotFoundError(
                f"❌ No se encontró el token: {self.token_path}\n"
                f"Ejecuta 'python generar_token.py' primero para generar el token."
            )
        
        try:
            creds = Credentials.from_authorized_user_file(self.token_path, self.SCOPES)
            logger.info("🔑 Token cargado desde archivo")
        except Exception as e:
            raise Exception(f"❌ Error cargando token: {e}")
                
        # Manejar credenciales inválidas o expiradas
        if not creds.valid:
            if creds.expired and creds.refresh_token:
                try:
                    logger.info("🔄 Refrescando token expirado...")
                    creds.refresh(Request())
                    
                    # Guardar token refrescado
                    with open(self.token_path, 'w') as token:
                        token.write(creds.to_json())
                        
                    logger.info("✅ Token refrescado exitosamente")
                    
                except Exception as refresh_error:
                    raise Exception(
                        f"❌ Error refrescando token: {refresh_error}\n"
                        f"Ejecuta 'python generar_token.py' para generar un nuevo token."
                    )
            else:
                raise Exception(
                    "❌ Token inválido sin refresh token disponible.\n"
                    "Ejecuta 'python generar_token.py' para generar un nuevo token."
                )
        
        # Crear servicio
        try:
            self.service = build('drive', 'v3', credentials=creds)
            
            # Validar conexión y obtener info del usuario
            about_info = self.service.about().get(fields="user,storageQuota").execute()
            user_email = about_info.get('user', {}).get('emailAddress', 'N/A')
            
            logger.info("✅ Autenticación con Google Drive exitosa")
            logger.info(f"👤 Usuario: {user_email}")
            
            return self.service
            
        except Exception as e:
            logger.error(f"❌ Error creando servicio de Drive: {e}")
            raise
    
    def is_authenticated(self) -> bool:
        """Verifica si ya existe una autenticación válida sin hacer llamadas a la API"""
        if not os.path.exists(self.token_path):
            return False
        
        try:
            creds = Credentials.from_authorized_user_file(self.token_path, self.SCOPES)
            # Verificar si el token está válido o si se puede refrescar
            return creds and (creds.valid or (creds.expired and creds.refresh_token))
        except Exception:
            return False
    
    def validate_connection(self) -> bool:
        """Valida que la conexión actual sea válida"""
        try:
            if not self.service:
                self.authenticate()
            
            # Prueba simple para validar la conexión
            self.service.about().get(fields="user").execute()
            return True
            
        except HttpError as e:
            if e.resp.status in [401, 403]:  # Unauthorized o Forbidden
                logger.warning("🔒 Token inválido, requiere re-autenticación")
                # Limpiar token corrupto
                if os.path.exists(self.token_path):
                    os.remove(self.token_path)
                self.service = None
                return False
            else:
                logger.error(f"❌ Error de conexión HTTP: {e}")
                return False
        except Exception as e:
            logger.error(f"❌ Conexión inválida: {e}")
            return False
    
    def force_reauthentication(self):
        """Fuerza una nueva autenticación eliminando tokens existentes"""
        logger.info("🔄 Forzando re-autenticación...")
        
        # Eliminar token existente
        if os.path.exists(self.token_path):
            os.remove(self.token_path)
            logger.info(f"🗑️ Token eliminado: {self.token_path}")
        
        # Limpiar servicio actual
        self.service = None
        
        # Re-autenticar
        return self.authenticate()
    
    # Método mejorado para manejo de errores en uploads
    def upload_invoice_json(self, invoice_data: dict, filename: str, folder_path: str) -> bool:
        """Sube un archivo JSON de factura a Google Drive con mejor manejo de errores"""
        
        # Validar conexión antes de proceder
        if not self.validate_connection():
            logger.info("🔄 Re-autenticando para subir archivo...")
            try:
                self.authenticate()
            except Exception as e:
                logger.error(f"❌ Error en re-autenticación: {e}")
                return False
        
        temp_file = None
        try:
            folder_id = self.get_folder_id(folder_path)
            if not folder_id:
                logger.error(f"❌ No se pudo obtener/crear la carpeta: {folder_path}")
                return False
            
            # Verificar si el archivo ya existe
            existing_file_id = self._get_file_id_in_folder(filename, folder_id)
            if existing_file_id:
                logger.info(f"📄 Archivo ya existe, saltando: {filename}")
                return True
            
            # Crear archivo temporal
            temp_file = tempfile.NamedTemporaryFile(
                mode='w', 
                suffix='.json', 
                prefix=f'invoice_upload_{uuid.uuid4().hex[:8]}_',
                delete=False,
                encoding='utf-8'
            )
            
            # Escribir datos JSON
            json.dump(invoice_data, temp_file, ensure_ascii=False, indent=2)
            temp_file.flush()
            temp_file.close()
            
            # Preparar upload
            media = MediaFileUpload(
                temp_file.name,
                mimetype='application/json',
                resumable=True,
                chunksize=1024*1024*8
            )
            
            file_metadata = {
                'name': filename,
                'parents': [folder_id]
            }
            
            request = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            )
            
            # Ejecutar upload
            response = self._execute_upload_with_retry(request, filename)
            
            if response:
                logger.info(f"✅ Archivo {filename} subido exitosamente")
                return True
            else:
                logger.error(f"❌ Error subiendo {filename}")
                return False
                
        except HttpError as e:
            if e.resp.status in [401, 403]:
                logger.warning("🔒 Error de autorización durante upload, reintentando...")
                if self.force_reauthentication():
                    # Reintentar upload una vez más
                    return self.upload_invoice_json(invoice_data, filename, folder_path)
            logger.error(f"❌ Error HTTP en upload: {e}")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error en upload_invoice_json: {e}")
            return False
        
        finally:
            if temp_file and os.path.exists(temp_file.name):
                self._cleanup_temp_file(temp_file.name, filename)
    
    # Resto de métodos permanecen igual...
    def get_folder_id(self, folder_path: str, create_if_not_exists: bool = True) -> Optional[str]:
        """Obtiene el ID de una carpeta por su ruta"""
        if folder_path in self._folder_cache:
            return self._folder_cache[folder_path]
        
        try:
            query = f"name='{folder_path}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            folders = results.get('files', [])
            
            if folders:
                folder_id = folders[0]['id']
                self._folder_cache[folder_path] = folder_id
                logger.info(f"📁 Carpeta encontrada: {folder_path}")
                return folder_id
            
            elif create_if_not_exists:
                folder_metadata = {
                    'name': folder_path,
                    'mimeType': 'application/vnd.google-apps.folder'
                }
                folder = self.service.files().create(body=folder_metadata, fields='id').execute()
                folder_id = folder.get('id')
                self._folder_cache[folder_path] = folder_id
                logger.info(f"📁 Carpeta creada: {folder_path}")
                return folder_id
            
            return None
            
        except HttpError as error:
            logger.error(f"❌ Error buscando/creando carpeta {folder_path}: {error}")
            return None
    
    def _get_file_id_in_folder(self, filename: str, folder_id: str) -> Optional[str]:
        """Busca un archivo por nombre dentro de una carpeta específica"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            results = self.service.files().list(q=query, fields="files(id, name)").execute()
            files = results.get('files', [])
            return files[0]['id'] if files else None
        except HttpError:
            return None
    
    def _execute_upload_with_retry(self, request, filename: str, max_retries: int = 3):
        """Ejecuta upload con reintentos"""
        for attempt in range(max_retries):
            try:
                response = None
                while response is None:
                    status, response = request.next_chunk()
                    if status:
                        progress = int(status.progress() * 100)
                        logger.debug(f"📊 Progreso {filename}: {progress}%")
                
                return response
                
            except HttpError as error:
                if error.resp.status == 429:  # Rate limit
                    wait_time = (2 ** attempt) + 1
                    logger.warning(f"⏳ Rate limit alcanzado. Esperando {wait_time}s...")
                    time.sleep(wait_time)
                elif error.resp.status >= 500:  # Server errors
                    wait_time = (2 ** attempt) + 1
                    logger.warning(f"⏳ Error servidor. Reintentando en {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"❌ Error HTTP: {error}")
                    break
            except Exception as e:
                logger.error(f"❌ Error en intento {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def _cleanup_temp_file(self, temp_file_path: str, filename: str, max_retries: int = 5):
        """Elimina archivo temporal con reintentos y delay"""
        for attempt in range(max_retries):
            try:
                time.sleep(0.1 * (attempt + 1))
                os.unlink(temp_file_path)
                logger.debug(f"🧹 Archivo temporal eliminado: {filename}")
                return
                
            except PermissionError as e:
                if attempt < max_retries - 1:
                    logger.debug(f"🔄 Reintentando eliminación de archivo temporal (intento {attempt + 1})")
                    continue
                else:
                    self._schedule_delayed_cleanup(temp_file_path)
                    logger.warning(f"⚠️ Archivo temporal programado para eliminación diferida: {os.path.basename(temp_file_path)}")
                    
            except Exception as cleanup_error:
                logger.warning(f"⚠️ Error eliminando archivo temporal: {cleanup_error}")
                break
    
    def _schedule_delayed_cleanup(self, temp_file_path: str):
        """Programa un archivo para eliminación diferida"""
        if not hasattr(self, '_delayed_cleanup_files'):
            self._delayed_cleanup_files = []
        
        self._delayed_cleanup_files.append(temp_file_path)

    def cleanup_delayed_files(self):
        """Intenta eliminar archivos que no se pudieron eliminar anteriormente"""
        if not hasattr(self, '_delayed_cleanup_files') or not self._delayed_cleanup_files:
            return
        
        logger.info(f"🧹 Intentando limpiar {len(self._delayed_cleanup_files)} archivos temporales pendientes...")
        
        remaining_files = []
        cleaned_count = 0
        
        for temp_file_path in self._delayed_cleanup_files:
            try:
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    cleaned_count += 1
                    logger.debug(f"🧹 Archivo temporal eliminado (diferido): {os.path.basename(temp_file_path)}")
            except Exception:
                remaining_files.append(temp_file_path)
        
        self._delayed_cleanup_files = remaining_files
        
        if cleaned_count > 0:
            logger.info(f"✅ Se eliminaron {cleaned_count} archivos temporales pendientes")
        
        if remaining_files:
            logger.warning(f"⚠️ Quedan {len(remaining_files)} archivos temporales sin eliminar")
    
    def test_connection(self) -> bool:
        """Prueba la conexión con Google Drive"""
        return self.validate_connection()
