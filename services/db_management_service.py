"""
Database Management Service - Handles database creation and table management operations.
"""

import os
import sqlite3
import shutil
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import text, inspect, create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
from models import db
from models.base_models import TableMetadata, UploadHistory
import logging

logger = logging.getLogger(__name__)

class DatabaseManagementService:
    """Service class for database management operations."""
    
    def __init__(self):
        self.instance_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'instance')
        os.makedirs(self.instance_folder, exist_ok=True)
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List all available databases in the instance folder."""
        databases = []
        
        try:
            # Get current database name from config
            from flask import current_app
            current_db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
            current_db_name = None
            
            if 'sqlite:///' in current_db_uri:
                current_db_name = os.path.basename(current_db_uri.replace('sqlite:///', ''))
            
            # Scan instance folder for .db files
            for filename in os.listdir(self.instance_folder):
                if filename.endswith('.db'):
                    db_path = os.path.join(self.instance_folder, filename)
                    
                    # Get database stats
                    stat = os.stat(db_path)
                    size = stat.st_size
                    modified = datetime.fromtimestamp(stat.st_mtime)
                    
                    # Count tables in database
                    table_count = 0
                    try:
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT IN ('sqlite_sequence', 'table_metadata', 'upload_history')")
                        table_count = cursor.fetchone()[0]
                        conn.close()
                    except Exception as e:
                        logger.error(f"Error counting tables in {filename}: {e}")
                    
                    databases.append({
                        'name': filename,
                        'display_name': filename.replace('.db', ''),
                        'path': db_path,
                        'size': size,
                        'size_formatted': self._format_file_size(size),
                        'modified': modified,
                        'table_count': table_count,
                        'is_current': filename == current_db_name
                    })
            
            # Sort by modification date (newest first)
            databases.sort(key=lambda x: x['modified'], reverse=True)
            
        except Exception as e:
            logger.error(f"Error listing databases: {e}")
        
        return databases
    
    def create_database(self, database_name: str) -> Dict[str, Any]:
        """Create a new database."""
        try:
            # Sanitize database name
            database_name = self._sanitize_filename(database_name)
            if not database_name.endswith('.db'):
                database_name += '.db'
            
            db_path = os.path.join(self.instance_folder, database_name)
            
            # Check if database already exists
            if os.path.exists(db_path):
                return {
                    'success': False,
                    'error': f'Database "{database_name}" already exists'
                }
            
            # Create new SQLite database
            conn = sqlite3.connect(db_path)
            
            # Create basic tables structure
            cursor = conn.cursor()
            
            # Create upload_history table
            cursor.execute('''
                CREATE TABLE upload_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename VARCHAR(255) NOT NULL,
                    original_filename VARCHAR(255) NOT NULL,
                    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    file_type VARCHAR(20),
                    total_sheets INTEGER DEFAULT 0,
                    total_records INTEGER DEFAULT 0,
                    file_size INTEGER DEFAULT 0,
                    status VARCHAR(50) DEFAULT 'processing',
                    error_message TEXT
                )
            ''')
            
            # Create table_metadata table
            cursor.execute('''
                CREATE TABLE table_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name VARCHAR(255) NOT NULL UNIQUE,
                    original_sheet_name VARCHAR(255) NOT NULL,
                    upload_id INTEGER NOT NULL,
                    column_count INTEGER DEFAULT 0,
                    row_count INTEGER DEFAULT 0,
                    created_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (upload_id) REFERENCES upload_history (id)
                )
            ''')
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'message': f'Database "{database_name}" created successfully',
                'database_name': database_name,
                'path': db_path
            }
            
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return {
                'success': False,
                'error': f'Failed to create database: {str(e)}'
            }
    
    def switch_database(self, database_name: str) -> Dict[str, Any]:
        """Switch to a different database."""
        try:
            if not database_name.endswith('.db'):
                database_name += '.db'
            
            db_path = os.path.join(self.instance_folder, database_name)
            
            if not os.path.exists(db_path):
                return {
                    'success': False,
                    'error': f'Database "{database_name}" not found'
                }
            
            # Update the app configuration to use the new database
            from flask import current_app
            new_db_uri = f'sqlite:///{db_path}'
            
            # Update the configuration
            current_app.config['SQLALCHEMY_DATABASE_URI'] = new_db_uri
            current_app.config['DATABASE_URL'] = new_db_uri
            
            # Update the database connection
            db.engine.dispose()  # Close existing connections
            
            # The app will need to be restarted for full effect, but we can provide immediate feedback
            return {
                'success': True,
                'message': f'Database switched to "{database_name}". The page will now reload to apply the changes.',
                'reload_required': True,
                'database_name': database_name
            }
            
        except Exception as e:
            logger.error(f"Error switching database: {e}")
            return {
                'success': False,
                'error': f'Failed to switch database: {str(e)}'
            }
    
    def delete_database(self, database_name: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a database (requires confirmation)."""
        try:
            if not confirm:
                return {
                    'success': False,
                    'error': 'Database deletion requires confirmation'
                }
            
            if not database_name.endswith('.db'):
                database_name += '.db'
            
            db_path = os.path.join(self.instance_folder, database_name)
            
            if not os.path.exists(db_path):
                return {
                    'success': False,
                    'error': f'Database "{database_name}" not found'
                }
            
            # Check if it's the current database
            from flask import current_app
            current_db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
            if db_path in current_db_uri:
                return {
                    'success': False,
                    'error': 'Cannot delete the currently active database'
                }
            
            # Create backup before deletion
            backup_path = db_path + f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
            shutil.copy2(db_path, backup_path)
            
            # Delete the database
            os.remove(db_path)
            
            return {
                'success': True,
                'message': f'Database "{database_name}" deleted successfully',
                'backup_path': backup_path
            }
            
        except Exception as e:
            logger.error(f"Error deleting database: {e}")
            return {
                'success': False,
                'error': f'Failed to delete database: {str(e)}'
            }
    
    def rename_table(self, old_name: str, new_name: str) -> Dict[str, Any]:
        """Rename a table in the current database."""
        try:
            # Sanitize new table name
            new_name = self._sanitize_table_name(new_name)
            
            # Check if old table exists
            inspector = inspect(db.engine)
            if old_name not in inspector.get_table_names():
                return {
                    'success': False,
                    'error': f'Table "{old_name}" not found'
                }
            
            # Check if new name already exists
            if new_name in inspector.get_table_names():
                return {
                    'success': False,
                    'error': f'Table "{new_name}" already exists'
                }
            
            # Rename the table
            with db.engine.connect() as conn:
                # SQLite syntax for renaming table
                conn.execute(text(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"'))
                conn.commit()
            
            # Update metadata
            metadata_record = TableMetadata.query.filter_by(table_name=old_name).first()
            if metadata_record:
                metadata_record.table_name = new_name
                db.session.commit()
            
            return {
                'success': True,
                'message': f'Table renamed from "{old_name}" to "{new_name}" successfully',
                'old_name': old_name,
                'new_name': new_name
            }
            
        except Exception as e:
            logger.error(f"Error renaming table: {e}")
            db.session.rollback()
            return {
                'success': False,
                'error': f'Failed to rename table: {str(e)}'
            }
    
    def duplicate_table(self, source_name: str, target_name: str, copy_data: bool = True) -> Dict[str, Any]:
        """Duplicate a table with optional data copying."""
        try:
            # Sanitize target table name
            target_name = self._sanitize_table_name(target_name)
            
            # Check if source table exists
            inspector = inspect(db.engine)
            if source_name not in inspector.get_table_names():
                return {
                    'success': False,
                    'error': f'Source table "{source_name}" not found'
                }
            
            # Check if target name already exists
            if target_name in inspector.get_table_names():
                return {
                    'success': False,
                    'error': f'Target table "{target_name}" already exists'
                }
            
            with db.engine.connect() as conn:
                if copy_data:
                    # Create table with data
                    conn.execute(text(f'CREATE TABLE "{target_name}" AS SELECT * FROM "{source_name}"'))
                else:
                    # Create table structure only
                    conn.execute(text(f'CREATE TABLE "{target_name}" AS SELECT * FROM "{source_name}" WHERE 0'))
                conn.commit()
            
            # Create metadata record for the new table
            source_metadata = TableMetadata.query.filter_by(table_name=source_name).first()
            if source_metadata:
                new_metadata = TableMetadata(
                    table_name=target_name,
                    original_sheet_name=f"{source_metadata.original_sheet_name} (Copy)",
                    upload_id=source_metadata.upload_id,
                    column_count=source_metadata.column_count,
                    row_count=source_metadata.row_count if copy_data else 0
                )
                db.session.add(new_metadata)
                db.session.commit()
            
            return {
                'success': True,
                'message': f'Table duplicated from "{source_name}" to "{target_name}" successfully',
                'source_name': source_name,
                'target_name': target_name,
                'data_copied': copy_data
            }
            
        except Exception as e:
            logger.error(f"Error duplicating table: {e}")
            db.session.rollback()
            return {
                'success': False,
                'error': f'Failed to duplicate table: {str(e)}'
            }
    
    def delete_table(self, table_name: str, confirm: bool = False) -> Dict[str, Any]:
        """Delete a table (requires confirmation)."""
        try:
            logger.info(f"Delete table request: {table_name}, confirm: {confirm}")
            
            if not confirm:
                return {
                    'success': False,
                    'error': 'Table deletion requires confirmation'
                }
            
            # Check if table exists
            inspector = inspect(db.engine)
            existing_tables = inspector.get_table_names()
            logger.info(f"Existing tables: {existing_tables}")
            
            if table_name not in existing_tables:
                return {
                    'success': False,
                    'error': f'Table "{table_name}" not found. Available tables: {", ".join(existing_tables)}'
                }
            
            # Delete the table
            logger.info(f"Dropping table: {table_name}")
            with db.engine.connect() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
                conn.commit()
                logger.info(f"Table {table_name} dropped successfully")
            
            # Remove metadata
            try:
                metadata_record = TableMetadata.query.filter_by(table_name=table_name).first()
                if metadata_record:
                    db.session.delete(metadata_record)
                    db.session.commit()
                    logger.info(f"Metadata for table {table_name} removed")
                else:
                    logger.info(f"No metadata found for table {table_name}")
            except Exception as meta_error:
                logger.warning(f"Could not remove metadata for {table_name}: {meta_error}")
                # Don't fail the whole operation for metadata cleanup issues
            
            return {
                'success': True,
                'message': f'Table "{table_name}" deleted successfully'
            }
            
        except Exception as e:
            logger.error(f"Error deleting table {table_name}: {e}", exc_info=True)
            db.session.rollback()
            return {
                'success': False,
                'error': f'Failed to delete table "{table_name}": {str(e)}'
            }
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for database creation."""
        # Remove invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove leading/trailing spaces and dots
        filename = filename.strip('. ')
        
        # Ensure it's not empty
        if not filename:
            filename = 'new_database'
        
        return filename
    
    def _sanitize_table_name(self, table_name: str) -> str:
        """Sanitize table name for SQL operations."""
        # Replace invalid characters with underscore
        sanitized = ''
        for char in table_name:
            if char.isalnum() or char == '_':
                sanitized += char
            else:
                sanitized += '_'
        
        # Ensure it starts with a letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = 'table_' + sanitized
        
        # Ensure it's not empty
        if not sanitized:
            sanitized = 'new_table'
        
        return sanitized
    
    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human-readable format."""
        if size_bytes == 0:
            return "0 B"
        
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f} {size_names[i]}"
    
    def move_table_to_database(self, table_name: str, target_db_name: str, 
                              action: str = 'move', progress_callback=None, app_config=None) -> Dict[str, Any]:
        """
        Move or copy a table to another database with progress tracking.
        
        Args:
            table_name: Name of the table to move/copy
            target_db_name: Name of the target database (without .db extension)
            action: 'move' (default) or 'copy'
            progress_callback: Function to call with progress updates
            app_config: Flask app configuration (to avoid context issues)
        """
        def update_progress(stage: str, percent: int, message: str = ""):
            if progress_callback:
                progress_callback({
                    'stage': stage,
                    'percent': percent,
                    'message': message,
                    'table_name': table_name,
                    'target_database': target_db_name
                })
        
        try:
            update_progress('validation', 5, 'Validating table and target database...')
            
            # Validate source table exists
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return {'success': False, 'error': f'Table "{table_name}" not found'}
            
            # Prepare target database path
            target_db_path = os.path.join(self.instance_folder, f"{target_db_name}.db")
            
            if not os.path.exists(target_db_path):
                return {'success': False, 'error': f'Target database "{target_db_name}" not found'}
            
            update_progress('setup', 15, 'Setting up database connections...')
            
            # Create engines for both databases
            source_engine = db.engine
            target_engine = create_engine(f'sqlite:///{target_db_path}')
            
            update_progress('schema', 25, 'Copying table schema...')
            
            # Get table structure
            source_metadata = MetaData()
            source_metadata.reflect(bind=source_engine, only=[table_name])
            source_table = source_metadata.tables[table_name]
            
            # Create table in target database
            target_metadata = MetaData()
            target_metadata.reflect(bind=target_engine)
            
            # Check if table already exists in target
            if table_name in target_metadata.tables:
                return {'success': False, 'error': f'Table "{table_name}" already exists in target database'}
            
            # Create table structure in target database
            # Use a simpler approach: get the CREATE TABLE statement and execute it
            with source_engine.connect() as source_conn:
                # Get the table schema using SQL
                result = source_conn.execute(text(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"))
                create_sql = result.fetchone()
                
                if not create_sql:
                    return {'success': False, 'error': f'Could not get schema for table "{table_name}"'}
                
                # Execute the CREATE TABLE statement in target database
                with target_engine.connect() as target_conn:
                    target_conn.execute(text(create_sql[0]))
                    target_conn.commit()
            
            update_progress('data_prep', 35, 'Preparing data transfer...')
            
            # Copy data
            with source_engine.connect() as source_conn:
                with target_engine.connect() as target_conn:
                    # Get all data from source table
                    result = source_conn.execute(text(f"SELECT * FROM {table_name}"))
                    rows = result.fetchall()
                    columns = result.keys()
                    
                    total_rows = len(rows)
                    logger.info(f"Found {total_rows} rows to transfer from {table_name}")
                    
                    update_progress('data_transfer', 40, f'Transferring {total_rows} rows...')
                    
                    if rows:
                        # Prepare insert statement with proper parameter binding
                        column_list = ', '.join(columns)
                        placeholders = ', '.join([':' + col for col in columns])
                        insert_sql = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"
                        
                        logger.info(f"Insert SQL: {insert_sql}")
                        
                        # Convert rows to dictionaries for proper parameter binding
                        data_dicts = []
                        for i, row in enumerate(rows):
                            if i % 1000 == 0:  # Update progress every 1000 rows
                                progress_percent = 40 + int((i / total_rows) * 30)
                                update_progress('data_prep', progress_percent, f'Converting row {i+1} of {total_rows}...')
                            
                            row_dict = {}
                            for j, col in enumerate(columns):
                                value = row[j]
                                # Handle None values and type conversion
                                if value is None:
                                    row_dict[col] = None
                                elif isinstance(value, str):
                                    # Handle string values - escape special characters if needed
                                    try:
                                        # Clean up problematic characters
                                        cleaned_value = value.replace('\x00', '').replace('\r\n', '\n')
                                        
                                        # Handle datetime strings - SQLite can handle ISO format directly
                                        if len(cleaned_value) > 10 and ('T' in cleaned_value or ' ' in cleaned_value) and (':' in cleaned_value):
                                            # This looks like a datetime string
                                            # Remove microseconds if present (SQLite doesn't always handle them well)
                                            if '.000000' in cleaned_value:
                                                cleaned_value = cleaned_value.replace('.000000', '')
                                            row_dict[col] = cleaned_value
                                        else:
                                            row_dict[col] = cleaned_value
                                    except Exception as str_error:
                                        logger.warning(f"Error processing string value for column {col}: {str_error}")
                                        row_dict[col] = str(value)[:1000]  # Truncate if too long
                                elif hasattr(value, 'isoformat'):
                                    # Convert datetime objects to ISO format strings
                                    row_dict[col] = value.isoformat()
                                else:
                                    # For all other types (int, float, etc.)
                                    row_dict[col] = value
                            data_dicts.append(row_dict)
                        
                        update_progress('data_insert', 70, 'Inserting data into target database...')
                        
                        # Insert data into target in batches
                        batch_size = 1000
                        total_inserted = 0
                        total_batches = (len(data_dicts) + batch_size - 1) // batch_size
                        
                        for i in range(0, len(data_dicts), batch_size):
                            batch = data_dicts[i:i + batch_size]
                            batch_num = i // batch_size + 1
                            
                            try:
                                target_conn.execute(text(insert_sql), batch)
                                total_inserted += len(batch)
                                
                                # Update progress
                                progress_percent = 70 + int((batch_num / total_batches) * 20)
                                update_progress('data_insert', progress_percent, 
                                              f'Inserted batch {batch_num}/{total_batches} ({total_inserted} rows)')
                                
                                logger.info(f"Inserted batch {batch_num}: {len(batch)} rows")
                            except Exception as batch_error:
                                logger.error(f"Error inserting batch {batch_num}: {str(batch_error)}")
                                logger.error(f"Batch error type: {type(batch_error).__name__}")
                                update_progress('data_insert', progress_percent, 
                                              f'Error in batch {batch_num}, trying individual rows...')
                                
                                # Try inserting rows one by one to identify problematic rows
                                for j, row_data in enumerate(batch):
                                    try:
                                        target_conn.execute(text(insert_sql), row_data)
                                        total_inserted += 1
                                    except Exception as row_error:
                                        logger.error(f"Error inserting row {i+j+1}: {str(row_error)}")
                                        logger.error(f"Row error type: {type(row_error).__name__}")
                                        logger.error(f"Problematic row data: {row_data}")
                                        
                                        # Try to sanitize the problematic row
                                        sanitized_row = {}
                                        for key, value in row_data.items():
                                            try:
                                                if value is None:
                                                    sanitized_row[key] = None
                                                elif isinstance(value, (int, float)):
                                                    sanitized_row[key] = value
                                                else:
                                                    # Convert everything else to string and clean it
                                                    sanitized_value = str(value).replace('\x00', '').replace('\r\n', '\n')
                                                    sanitized_row[key] = sanitized_value
                                            except Exception:
                                                sanitized_row[key] = None  # Set to None if can't sanitize
                                        
                                        # Try again with sanitized data
                                        try:
                                            target_conn.execute(text(insert_sql), sanitized_row)
                                            total_inserted += 1
                                            logger.info(f"Successfully inserted row {i+j+1} after sanitization")
                                        except Exception as final_error:
                                            logger.error(f"Final error for row {i+j+1}: {str(final_error)}")
                                            logger.error(f"Skipping row {i+j+1} - could not insert even after sanitization")
                        
                        target_conn.commit()
                        logger.info(f"Successfully transferred {total_inserted} rows")
                        
                        update_progress('cleanup', 90, 'Finalizing transfer...')
            
            # Handle metadata transfer
            source_metadata_record = TableMetadata.query.filter_by(table_name=table_name).first()
            if source_metadata_record:
                # Create metadata record for target database
                # Note: We'll need to update this when switching to target database
                pass
            
            # Remove from source if moving (not copying)
            if action == 'move':
                update_progress('cleanup', 95, 'Removing table from source database...')
                with source_engine.connect() as source_conn:
                    source_conn.execute(text(f"DROP TABLE {table_name}"))
                    source_conn.commit()
                
                # Remove metadata record
                if source_metadata_record:
                    db.session.delete(source_metadata_record)
                    db.session.commit()
            
            update_progress('complete', 100, f'Table {action} completed successfully!')
            
            operation = 'moved' if action == 'move' else 'copied'
            
            # Get row count for response
            transferred_rows = 0
            if 'data_dicts' in locals():
                transferred_rows = len(data_dicts)
            elif 'rows' in locals():
                transferred_rows = len(rows)
            
            return {
                'success': True,
                'message': f'Table "{table_name}" {operation} to database "{target_db_name}"',
                'action': action,
                'source_table': table_name,
                'target_database': target_db_name,
                'rows_transferred': transferred_rows
            }
            
        except Exception as e:
            logger.error(f"Error moving table {table_name} to {target_db_name}: {str(e)}")
            if progress_callback:
                progress_callback({
                    'stage': 'error',
                    'percent': 0,
                    'message': f'Error: {str(e)}',
                    'table_name': table_name,
                    'target_database': target_db_name
                })
            return {'success': False, 'error': str(e)}
    
    def list_tables_in_database(self, db_name: str) -> List[Dict[str, Any]]:
        """List all tables in a specific database."""
        try:
            db_path = os.path.join(self.instance_folder, f"{db_name}.db")
            
            if not os.path.exists(db_path):
                return []
            
            # Create engine for the target database
            engine = create_engine(f'sqlite:///{db_path}')
            inspector = inspect(engine)
            
            tables = []
            for table_name in inspector.get_table_names():
                # Skip system tables
                if table_name in ['table_metadata', 'upload_history', 'sqlite_sequence']:
                    continue
                    
                # Get row count
                with engine.connect() as conn:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                    row_count = result.scalar()
                
                # Get column count
                columns = inspector.get_columns(table_name)
                
                tables.append({
                    'name': table_name,
                    'row_count': row_count,
                    'column_count': len(columns),
                    'database': db_name
                })
            
            return tables
            
        except Exception as e:
            logger.error(f"Error listing tables in database {db_name}: {str(e)}")
            return []
    
    def get_database_selection_info(self) -> Dict[str, Any]:
        """Get information for database selection during upload."""
        try:
            databases = self.list_databases()
            current_db = None
            
            # Get current database name
            from flask import current_app
            current_db_uri = current_app.config.get('SQLALCHEMY_DATABASE_URI', '')
            if 'sqlite:///' in current_db_uri:
                current_db = os.path.basename(current_db_uri.replace('sqlite:///', ''))
                current_db = current_db.replace('.db', '')
            
            return {
                'databases': databases,
                'current_database': current_db,
                'total_databases': len(databases)
            }
            
        except Exception as e:
            logger.error(f"Error getting database selection info: {str(e)}")
            return {'databases': [], 'current_database': None, 'total_databases': 0}
