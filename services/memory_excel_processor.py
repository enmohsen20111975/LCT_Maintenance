from typing import List, Dict, Any, Optional, Tuple
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import (
    Table, Column, Integer, String, Float, DateTime, Text, 
    MetaData, create_engine, inspect
)
from sqlalchemy.exc import SQLAlchemyError
from models import db
from models.base_models import UploadHistory, TableMetadata
import tempfile
import io

class InMemoryExcelProcessor:
    """Enhanced Excel processor that processes files in memory without saving to disk."""
    
    def __init__(self, db_session=None):
        self.db = db_session or db
        self.allowed_extensions = {'xlsx', 'xls', 'xlsm'}  # Added .xlsm support
    
    def allowed_file(self, filename: str) -> bool:
        """Check if file has allowed extension."""
        return '.' in filename and filename.rsplit('.', 1)[1].lower() in self.allowed_extensions
    
    def _safe_datetime_convert(self, value):
        """Safely convert values that might cause datetime issues."""
        if pd.isna(value):
            return None
        
        # Handle pandas NaT (Not a Time) values
        if hasattr(value, '__class__') and 'NaTType' in str(value.__class__):
            return None
        
        # Handle numpy dtypes
        if hasattr(value, 'item'):
            try:
                return value.item()
            except (ValueError, OverflowError, TypeError):
                return None
        
        # Handle datetime objects that might be problematic
        if hasattr(value, 'strftime'):
            try:
                # Test if the datetime can be used
                _ = value.strftime('%Y-%m-%d')
                return value
            except (ValueError, OverflowError, TypeError):
                return None
        
        return value

    def get_excel_worksheets_from_file(self, file: FileStorage) -> Dict[str, Dict[str, Any]]:
        """
        Read Excel file from memory and return information about each worksheet.
        Returns dict with sheet names as keys and metadata as values.
        """
        try:
            # Read file content into memory
            file_content = file.read()
            file.seek(0)  # Reset file pointer for future reads
            
            # Create a BytesIO object to work with pandas
            file_buffer = io.BytesIO(file_content)
            
            # Read all sheets without loading full data
            xl_file = pd.ExcelFile(file_buffer, engine='openpyxl')
            sheets_info = {}
            
            for sheet_name in xl_file.sheet_names:
                try:
                    # Read just the first few rows to get column info and check data
                    df_sample = pd.read_excel(file_buffer, sheet_name=sheet_name, nrows=5, engine='openpyxl')
                    
                    # Read full sheet to get accurate row count
                    df_full = pd.read_excel(file_buffer, sheet_name=sheet_name, engine='openpyxl')
                    
                    # Reset buffer position for next sheet
                    file_buffer.seek(0)
                    
                    sheets_info[sheet_name] = {
                        'columns': list(df_sample.columns),
                        'column_count': len(df_sample.columns),
                        'row_count': len(df_full),
                        'sample_data': df_sample.head(3).to_dict('records') if not df_sample.empty else [],
                        'has_data': not df_full.empty,
                        'error': None
                    }
                    
                except Exception as e:
                    sheets_info[sheet_name] = {
                        'columns': [],
                        'column_count': 0,
                        'row_count': 0,
                        'sample_data': [],
                        'has_data': False,
                        'error': str(e)
                    }
            
            return sheets_info
            
        except Exception as e:
            raise Exception(f"Error reading Excel file: {str(e)}")
    
    def import_worksheet_to_table(self, file: FileStorage, worksheet_name: str, 
                                import_mode: str, target_table: str = None, 
                                new_table_name: str = None) -> Dict[str, Any]:
        """
        Import a specific worksheet to database table without saving file to disk.
        """
        try:
            # Read file content into memory
            file_content = file.read()
            file.seek(0)  # Reset file pointer
            
            # Create a BytesIO object to work with pandas
            file_buffer = io.BytesIO(file_content)
            
            # Read the specific worksheet
            df = pd.read_excel(file_buffer, sheet_name=worksheet_name, engine='openpyxl')
            
            if df.empty:
                return {
                    'success': False,
                    'message': f'Worksheet "{worksheet_name}" is empty',
                    'table_name': None,
                    'records_imported': 0
                }
            
            # Clean the dataframe
            df = self._clean_dataframe(df)
            
            # Determine table name
            if import_mode == 'new':
                table_name = new_table_name or f"table_{worksheet_name.lower().replace(' ', '_')}"
            elif import_mode == 'replace':
                table_name = target_table
            else:  # append mode
                table_name = target_table
            
            # Ensure table name is valid
            table_name = self._sanitize_table_name(table_name)
            
            # Handle different import modes
            if import_mode == 'new':
                success = self._create_new_table(df, table_name, worksheet_name, file.filename)
            elif import_mode == 'replace':
                success = self._replace_table_data(df, table_name, worksheet_name)
            else:  # append
                success = self._append_to_table(df, table_name, worksheet_name)
            
            if success:
                return {
                    'success': True,
                    'message': f'Successfully imported {len(df)} records',
                    'table_name': table_name,
                    'records_imported': len(df)
                }
            else:
                return {
                    'success': False,
                    'message': 'Failed to import data',
                    'table_name': table_name,
                    'records_imported': 0
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error importing worksheet: {str(e)}',
                'table_name': None,
                'records_imported': 0
            }
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the dataframe for database insertion."""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Clean column names
        df.columns = [self._sanitize_column_name(str(col)) for col in df.columns]
        
        # Handle datetime conversion safely
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(self._safe_datetime_convert)
        
        return df
    
    def _sanitize_table_name(self, name: str) -> str:
        """Sanitize table name for database."""
        import re
        # Remove special characters and spaces, keep only alphanumeric and underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        # Ensure it starts with a letter
        if not sanitized[0].isalpha():
            sanitized = 'table_' + sanitized
        return sanitized.lower()
    
    def _sanitize_column_name(self, name: str) -> str:
        """Sanitize column name for database."""
        import re
        # Remove special characters and spaces, replace with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', str(name))
        # Remove multiple consecutive underscores
        sanitized = re.sub(r'_+', '_', sanitized)
        # Remove leading/trailing underscores
        sanitized = sanitized.strip('_')
        # Ensure it starts with a letter
        if not sanitized or not sanitized[0].isalpha():
            sanitized = 'col_' + sanitized
        return sanitized.lower()
    
    def _create_new_table(self, df: pd.DataFrame, table_name: str, 
                         sheet_name: str, filename: str) -> bool:
        """Create a new table with the dataframe data."""
        try:
            # Create SQLAlchemy table dynamically
            metadata = MetaData()
            columns = [Column('id', Integer, primary_key=True, autoincrement=True)]
            
            for col_name in df.columns:
                col_type = self._infer_column_type(df[col_name])
                columns.append(Column(col_name, col_type))
            
            table = Table(table_name, metadata, *columns)
            
            # Create the table in database
            table.create(self.db.engine, checkfirst=True)
            
            # Insert data
            data_dicts = df.to_dict('records')
            if data_dicts:
                self.db.session.execute(table.insert().values(data_dicts))
            
            # Create metadata record
            metadata_record = TableMetadata(
                table_name=table_name,
                original_sheet_name=sheet_name,
                original_filename=filename,
                column_count=len(df.columns),
                row_count=len(df),
                created_date=datetime.now()
            )
            
            self.db.session.add(metadata_record)
            self.db.session.commit()
            
            return True
            
        except Exception as e:
            self.db.session.rollback()
            print(f"Error creating new table: {str(e)}")
            return False
    
    def _replace_table_data(self, df: pd.DataFrame, table_name: str, sheet_name: str) -> bool:
        """Replace data in existing table."""
        try:
            # Clear existing data
            self.db.session.execute(f"DELETE FROM {table_name}")
            
            # Insert new data
            data_dicts = df.to_dict('records')
            if data_dicts:
                # Get table object
                metadata = MetaData()
                metadata.reflect(bind=self.db.engine)
                table = metadata.tables[table_name]
                
                self.db.session.execute(table.insert().values(data_dicts))
            
            # Update metadata
            metadata_record = TableMetadata.query.filter_by(table_name=table_name).first()
            if metadata_record:
                metadata_record.row_count = len(df)
                metadata_record.updated_date = datetime.now()
            
            self.db.session.commit()
            return True
            
        except Exception as e:
            self.db.session.rollback()
            print(f"Error replacing table data: {str(e)}")
            return False
    
    def _append_to_table(self, df: pd.DataFrame, table_name: str, sheet_name: str) -> bool:
        """Append data to existing table."""
        try:
            # Get table object
            metadata = MetaData()
            metadata.reflect(bind=self.db.engine)
            table = metadata.tables[table_name]
            
            # Insert new data
            data_dicts = df.to_dict('records')
            if data_dicts:
                self.db.session.execute(table.insert().values(data_dicts))
            
            # Update metadata
            metadata_record = TableMetadata.query.filter_by(table_name=table_name).first()
            if metadata_record:
                metadata_record.row_count += len(df)
                metadata_record.updated_date = datetime.now()
            
            self.db.session.commit()
            return True
            
        except Exception as e:
            self.db.session.rollback()
            print(f"Error appending to table: {str(e)}")
            return False
    
    def _infer_column_type(self, series: pd.Series):
        """Infer the best SQLAlchemy column type for a pandas series."""
        from sqlalchemy import Integer, Float, DateTime, String, Text
        
        # Remove null values for type inference
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return Text
        
        # Check if it's numeric
        if pd.api.types.is_numeric_dtype(non_null_series):
            if pd.api.types.is_integer_dtype(non_null_series):
                return Integer
            else:
                return Float
        
        # Check if it's datetime
        if pd.api.types.is_datetime64_any_dtype(non_null_series):
            return DateTime
        
        # Check string length to decide between String and Text
        if pd.api.types.is_string_dtype(non_null_series):
            max_length = non_null_series.astype(str).str.len().max()
            if max_length <= 255:
                return String(255)
            else:
                return Text
        
        # Default to Text for everything else
        return Text
    
    def get_existing_tables(self) -> List[Dict[str, Any]]:
        """Get list of existing tables from metadata."""
        tables = TableMetadata.query.order_by(TableMetadata.created_date.desc()).all()
        return [table.to_dict() for table in tables]
