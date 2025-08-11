import pandas as pd
import os
import re
from typing import List, Dict, Tuple, Any, Optional
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from models import db
from models.base_models import UploadHistory, TableMetadata
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Table, inspect
from sqlalchemy.dialects import sqlite, postgresql
from datetime import datetime

class EnhancedExcelProcessor:
    """Enhanced service class for Excel file processing with worksheet selection and table replacement."""
    
    def __init__(self, upload_folder: str):
        self.upload_folder = upload_folder
        os.makedirs(upload_folder, exist_ok=True)
    
    def _safe_datetime_convert(self, value):
        """Safely convert datetime values, handling NaT."""
        try:
            if pd.isna(value) or pd.isnull(value):
                return None
            if hasattr(value, 'strftime'):
                return value.strftime('%Y-%m-%d %H:%M:%S') if value is not pd.NaT else None
            return str(value) if value is not None else None
        except:
            return None
    
    def allowed_file(self, filename: str) -> bool:
        """Check if file extension is allowed."""
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in {'xlsx', 'xls', 'xlsm'}
    
    def sanitize_table_name(self, name: str) -> str:
        """Convert sheet name to valid table name."""
        # Remove special characters and replace with underscore
        name = re.sub(r'[^\w\s]', '_', name)
        # Replace spaces with underscore
        name = re.sub(r'\s+', '_', name)
        # Remove multiple underscores
        name = re.sub(r'_+', '_', name)
        # Ensure it starts with letter or underscore
        if name and name[0].isdigit():
            name = 'table_' + name
        # Convert to lowercase
        name = name.lower().strip('_')
        # Ensure it's not empty
        if not name:
            name = 'unnamed_table'
        return name
    
    def sanitize_column_name(self, name: str) -> str:
        """Convert column header to valid column name."""
        if pd.isna(name) or str(name).strip() == '':
            return 'unnamed_column'
        
        name = str(name)
        # Remove special characters and replace with underscore
        name = re.sub(r'[^\w\s]', '_', name)
        # Replace spaces with underscore
        name = re.sub(r'\s+', '_', name)
        # Remove multiple underscores
        name = re.sub(r'_+', '_', name)
        # Ensure it starts with letter or underscore
        if name and name[0].isdigit():
            name = 'col_' + name
        # Convert to lowercase
        name = name.lower().strip('_')
        # Ensure it's not empty
        if not name:
            name = 'unnamed_column'
        return name
    
    def safe_value_conversion(self, value):
        """Safely convert pandas values to Python types, handling NaT and NaN."""
        # Handle None first
        if value is None:
            return None
            
        # Handle pandas NaT (Not a Time) and NaN
        if pd.isna(value):
            return None
        
        # Handle pandas NaT specifically by checking for pd.NaT
        try:
            if pd.api.types.is_datetime64_any_dtype(type(value)) and pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
            
        # Handle pandas Timestamp
        if hasattr(value, 'to_pydatetime'):
            try:
                return value.to_pydatetime()
            except (ValueError, OverflowError, TypeError):
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

    def infer_column_type(self, series: pd.Series) -> Any:
        """Infer the best SQLAlchemy column type for a pandas series."""
        # Remove null values for type inference
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return Text
        
        # Check if it's numeric
        if pd.api.types.is_numeric_dtype(non_null_series):
            # Check if it's integer
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
    
    def save_uploaded_file(self, file: FileStorage) -> str:
        """Save uploaded file and return the file path."""
        if file and self.allowed_file(file.filename):
            # Create unique filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{timestamp}_{secure_filename(file.filename)}"
            file_path = os.path.abspath(os.path.join(self.upload_folder, filename))
            file.save(file_path)
            return file_path
        raise ValueError("Invalid file type")

    def get_excel_worksheets_from_memory(self, file: FileStorage) -> Dict[str, Dict[str, Any]]:
        """
        Read Excel file from memory without saving to disk and return information about each worksheet.
        Returns dict with sheet names as keys and metadata as values.
        """
        try:
            import io
            
            # Read file content into memory
            file_content = file.read()
            file.seek(0)  # Reset file pointer for future reads
            
            # Create a BytesIO object to work with pandas
            file_buffer = io.BytesIO(file_content)
            
            # Read all sheets without loading full data first
            xl_file = pd.ExcelFile(file_buffer, engine='openpyxl')
            sheets_info = {}
            
            for sheet_name in xl_file.sheet_names:
                try:
                    # Reset buffer position
                    file_buffer.seek(0)
                    
                    # Read just the first few rows to get column info and check data
                    df_sample = pd.read_excel(file_buffer, sheet_name=sheet_name, nrows=5, engine='openpyxl')
                    
                    # Reset and read full sheet to get accurate row count
                    file_buffer.seek(0)
                    df_full = pd.read_excel(file_buffer, sheet_name=sheet_name, engine='openpyxl')
                    
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
    
    def get_excel_worksheets(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        Read Excel file and return information about each worksheet.
        Returns dict with sheet names as keys and metadata as values.
        """
        try:
            # Read all sheets without loading full data
            xl_file = pd.ExcelFile(file_path, engine='openpyxl')
            sheets_info = {}
            
            for sheet_name in xl_file.sheet_names:
                try:
                    # Read only first few rows to get column info
                    df_sample = pd.read_excel(file_path, sheet_name=sheet_name, nrows=5, engine='openpyxl')
                    
                    # Clean and handle all data types properly
                    for col in df_sample.columns:
                        if pd.api.types.is_datetime64_any_dtype(df_sample[col]):
                            # Convert datetime columns, handling NaT values
                            df_sample[col] = df_sample[col].apply(self._safe_datetime_convert)
                        elif pd.api.types.is_numeric_dtype(df_sample[col]):
                            # Handle NaN in numeric columns
                            df_sample[col] = df_sample[col].fillna(0)
                        else:
                            # Handle object/string columns, convert everything to string
                            df_sample[col] = df_sample[col].astype(str).fillna('')
                    
                    # Convert sample data to records with proper type handling
                    try:
                        sample_records = []
                        for _, row in df_sample.head(3).iterrows():
                            record = {}
                            for col in df_sample.columns:
                                value = row[col]
                                # Ensure all values are JSON serializable
                                if pd.isna(value):
                                    record[col] = None
                                elif isinstance(value, (int, float, str, bool)):
                                    record[col] = value
                                else:
                                    record[col] = str(value)
                            sample_records.append(record)
                    except Exception as sample_error:
                        print(f"Error creating sample data for {sheet_name}: {sample_error}")
                        sample_records = []
                    
                    # Get basic info - use a more efficient method for row counting
                    try:
                        # Try to get row count without loading full data
                        df_info = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl', nrows=0)
                        # Read with chunk to get actual row count efficiently
                        total_rows = 0
                        chunk_size = 1000
                        for chunk in pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl', chunksize=chunk_size):
                            total_rows += len(chunk)
                    except:
                        # Fallback method
                        total_rows = len(pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl'))
                    
                    sheets_info[sheet_name] = {
                        'columns': list(df_sample.columns),
                        'column_count': len(df_sample.columns),
                        'row_count': total_rows,
                        'sample_data': sample_records,
                        'suggested_table_name': self.sanitize_table_name(sheet_name)
                    }
                except Exception as e:
                    print(f"Error processing sheet {sheet_name}: {str(e)}")  # Debug info
                    sheets_info[sheet_name] = {
                        'error': f"Could not read sheet: {str(e)}",
                        'columns': [],
                        'column_count': 0,
                        'row_count': 0,
                        'sample_data': [],
                        'suggested_table_name': self.sanitize_table_name(sheet_name)
                    }
            
            return sheets_info
            
        except Exception as e:
            raise Exception(f"Could not read Excel file: {str(e)}")
    
    def get_existing_tables(self) -> List[Dict[str, Any]]:
        """Get list of existing database tables."""
        try:
            inspector = inspect(db.engine)
            table_names = inspector.get_table_names()
            
            # Filter out system tables
            user_tables = []
            for table_name in table_names:
                if table_name not in ['upload_history', 'table_metadata']:
                    # Get table info
                    columns = inspector.get_columns(table_name)
                    
                    # Get row count
                    try:
                        result = db.session.execute(db.text(f"SELECT COUNT(*) FROM {table_name}"))
                        row_count = result.scalar()
                    except:
                        row_count = 0
                    
                    user_tables.append({
                        'name': table_name,
                        'columns': [col['name'] for col in columns],
                        'column_count': len(columns),
                        'row_count': row_count
                    })
            
            return user_tables
            
        except Exception as e:
            raise Exception(f"Error getting existing tables: {str(e)}")
    
    def compare_table_structure(self, worksheet_columns: List[str], table_name: str) -> Dict[str, Any]:
        """Compare worksheet columns with existing table structure."""
        try:
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return {
                    'compatible': False,
                    'reason': 'Table does not exist',
                    'missing_columns': [],
                    'extra_columns': [],
                    'suggestions': []
                }
            
            # Get existing table columns
            table_columns = inspector.get_columns(table_name)
            table_column_names = [col['name'] for col in table_columns if col['name'] != 'id']
            
            # Sanitize worksheet columns
            sanitized_worksheet_columns = [self.sanitize_column_name(col) for col in worksheet_columns]
            
            # Compare
            missing_in_table = set(sanitized_worksheet_columns) - set(table_column_names)
            extra_in_table = set(table_column_names) - set(sanitized_worksheet_columns)
            
            compatible = len(missing_in_table) == 0 and len(extra_in_table) == 0
            
            return {
                'compatible': compatible,
                'table_columns': table_column_names,
                'worksheet_columns': sanitized_worksheet_columns,
                'missing_columns': list(missing_in_table),
                'extra_columns': list(extra_in_table),
                'suggestions': self._generate_column_mapping_suggestions(worksheet_columns, table_column_names)
            }
            
        except Exception as e:
            return {
                'compatible': False,
                'reason': f'Error comparing structures: {str(e)}',
                'missing_columns': [],
                'extra_columns': [],
                'suggestions': []
            }
    
    def _generate_column_mapping_suggestions(self, worksheet_cols: List[str], table_cols: List[str]) -> List[Dict[str, str]]:
        """Generate suggestions for column mapping."""
        suggestions = []
        
        for ws_col in worksheet_cols:
            sanitized_ws_col = self.sanitize_column_name(ws_col)
            
            # Find best match in table columns
            best_match = None
            best_score = 0
            
            for table_col in table_cols:
                # Simple similarity score
                score = self._calculate_similarity(sanitized_ws_col.lower(), table_col.lower())
                if score > best_score and score > 0.5:  # Threshold for suggestions
                    best_score = score
                    best_match = table_col
            
            suggestions.append({
                'worksheet_column': ws_col,
                'sanitized_name': sanitized_ws_col,
                'suggested_table_column': best_match,
                'confidence': best_score
            })
        
        return suggestions
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate simple similarity score between two strings."""
        if str1 == str2:
            return 1.0
        
        # Check if one contains the other
        if str1 in str2 or str2 in str1:
            return 0.8
        
        # Simple character overlap
        set1 = set(str1.lower())
        set2 = set(str2.lower())
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        
        return intersection / union if union > 0 else 0.0
    
    def replace_table_data(self, file_path: str, worksheet_name: str, target_table: str, 
                          column_mapping: Optional[Dict[str, str]] = None) -> Tuple[bool, str, int]:
        """
        Replace data in existing table with worksheet data.
        Returns (success, message, records_imported).
        """
        try:
            # Read the specific worksheet
            df = pd.read_excel(file_path, sheet_name=worksheet_name, engine='openpyxl')
            
            if df.empty:
                return False, "Worksheet is empty", 0
            
            # Clean the DataFrame
            df = df.dropna(how='all')  # Remove completely empty rows
            df = df.loc[:, ~df.columns.duplicated()]  # Remove duplicate columns
            
            # Clean datetime columns to handle NaT values properly
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Use the safe datetime converter
                    df[col] = df[col].apply(self._safe_datetime_convert)
            
            if df.empty:
                return False, "No valid data found in worksheet", 0
            
            # Apply column mapping if provided
            if column_mapping:
                df = df.rename(columns=column_mapping)
            else:
                # Sanitize column names
                df.columns = [self.sanitize_column_name(col) for col in df.columns]
            
            # Check if table exists
            inspector = inspect(db.engine)
            if target_table not in inspector.get_table_names():
                return False, f"Target table '{target_table}' does not exist", 0
            
            # Get table columns (excluding 'id')
            table_columns = inspector.get_columns(target_table)
            table_column_names = [col['name'] for col in table_columns if col['name'] != 'id']
            
            # Ensure DataFrame has all required columns
            for col in table_column_names:
                if col not in df.columns:
                    df[col] = None  # Add missing columns with null values
            
            # Remove extra columns
            df = df[table_column_names]
            
            # Clear existing data
            db.session.execute(db.text(f"DELETE FROM {target_table}"))
            
            # Insert new data
            data_records = []
            for _, row in df.iterrows():
                record = {}
                for col in table_column_names:
                    value = row[col]
                    # Use safe value conversion to handle NaT and other pandas edge cases
                    record[col] = self.safe_value_conversion(value)
                data_records.append(record)
            
            if data_records:
                # Get table object for insert
                table = Table(target_table, db.metadata, autoload_with=db.engine)
                db.session.execute(table.insert(), data_records)
            
            # Update table metadata
            table_meta = TableMetadata.query.filter_by(table_name=target_table).first()
            if table_meta:
                table_meta.row_count = len(data_records)
            
            db.session.commit()
            
            return True, f"Successfully imported {len(data_records)} records", len(data_records)
            
        except Exception as e:
            db.session.rollback()
            return False, f"Import failed: {str(e)}", 0
    
    def create_new_table_from_worksheet(self, file_path: str, worksheet_name: str, 
                                      table_name: str) -> Tuple[bool, str, int]:
        """
        Create a new table from worksheet data.
        Returns (success, message, records_imported).
        """
        try:
            # Read the worksheet
            df = pd.read_excel(file_path, sheet_name=worksheet_name, engine='openpyxl')
            
            if df.empty:
                return False, "Worksheet is empty", 0
            
            # Clean the DataFrame
            df = df.dropna(how='all')
            df = df.loc[:, ~df.columns.duplicated()]
            
            # Clean datetime columns to handle NaT values properly
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    # Use the safe datetime converter
                    df[col] = df[col].apply(self._safe_datetime_convert)
            
            if df.empty:
                return False, "No valid data found in worksheet", 0
            
            # Create table
            columns = [Column('id', Integer, primary_key=True, autoincrement=True)]
            
            # Process each column
            for col in df.columns:
                sanitized_name = self.sanitize_column_name(col)
                
                # Ensure unique column names
                original_name = sanitized_name
                counter = 1
                while any(c.name == sanitized_name for c in columns):
                    sanitized_name = f"{original_name}_{counter}"
                    counter += 1
                
                col_type = self.infer_column_type(df[col])
                columns.append(Column(sanitized_name, col_type))
            
            # Create table
            table = Table(table_name, db.metadata, *columns, extend_existing=True)
            table.create(db.engine, checkfirst=True)
            
            # Insert data
            data_records = []
            column_names = [col.name for col in columns if col.name != 'id']
            
            for _, row in df.iterrows():
                record = {}
                for i, col in enumerate(df.columns):
                    sanitized_col = column_names[i]  # Corresponding sanitized column name
                    value = row[col]
                    # Use safe value conversion to handle NaT and other pandas edge cases
                    record[sanitized_col] = self.safe_value_conversion(value)
                data_records.append(record)
            
            if data_records:
                db.session.execute(table.insert(), data_records)
            
            # Create table metadata
            table_metadata = TableMetadata(
                table_name=table_name,
                original_sheet_name=worksheet_name,
                upload_id=0,  # We'll need to track this properly
                column_count=len(df.columns),
                row_count=len(df)
            )
            db.session.add(table_metadata)
            
            db.session.commit()
            
            return True, f"Successfully created table '{table_name}' with {len(data_records)} records", len(data_records)
            
        except Exception as e:
            db.session.rollback()
            return False, f"Table creation failed: {str(e)}", 0

    def import_worksheet_from_memory(self, file: FileStorage, worksheet_name: str, 
                                   import_mode: str, target_table: str = None, 
                                   new_table_name: str = None) -> Dict[str, Any]:
        """
        Import a specific worksheet to database table from memory without saving file to disk.
        """
        try:
            import io
            
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
            df = self._clean_dataframe_memory(df)
            
            # Determine table name
            if import_mode == 'new':
                table_name = new_table_name or f"table_{worksheet_name.lower().replace(' ', '_')}"
            elif import_mode == 'replace':
                table_name = target_table
            else:  # append mode
                table_name = target_table
            
            # Ensure table name is valid
            table_name = self.sanitize_table_name(table_name)
            
            # Handle different import modes
            if import_mode == 'new':
                success, message, records_imported = self._create_new_table_memory(df, table_name, worksheet_name, file.filename)
            elif import_mode == 'replace':
                success, message, records_imported = self.replace_table_data(None, worksheet_name, table_name, df_override=df)
            else:  # append
                success, message, records_imported = self.append_to_table(None, worksheet_name, table_name, df_override=df)
            
            return {
                'success': success,
                'message': message,
                'table_name': table_name,
                'records_imported': records_imported
            }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error importing worksheet: {str(e)}',
                'table_name': None,
                'records_imported': 0
            }
    
    def _clean_dataframe_memory(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the dataframe for database insertion (memory version)."""
        # Remove completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # Clean column names
        df.columns = [self.sanitize_column_name(str(col)) for col in df.columns]
        
        # Handle datetime conversion safely
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(self._safe_datetime_convert)
        
        return df
    
    def _create_new_table_memory(self, df: pd.DataFrame, table_name: str, 
                               sheet_name: str, filename: str) -> Tuple[bool, str, int]:
        """Create a new table with the dataframe data (memory version)."""
        try:
            # Clean column names to ensure they are database-safe
            df.columns = [self.sanitize_column_name(str(col)) for col in df.columns]
            
            # Create table structure
            metadata = self.create_table_structure(table_name, df)
            table = metadata.tables[table_name]
            
            # Create table in database
            table.create(db.engine, checkfirst=True)
            
            # Prepare data for insertion
            data_records = []
            for _, row in df.iterrows():
                record = {}
                for col in df.columns:
                    value = row[col]
                    # Apply safe datetime conversion
                    cleaned_value = self._safe_datetime_convert(value)
                    record[col] = cleaned_value
                data_records.append(record)
            
            # Insert data
            if data_records:
                db.session.execute(table.insert().values(data_records))
            
            # Create metadata record
            table_metadata = TableMetadata(
                table_name=table_name,
                original_sheet_name=sheet_name,
                original_filename=filename,
                column_count=len(df.columns),
                row_count=len(df),
                created_date=datetime.now()
            )
            
            db.session.add(table_metadata)
            db.session.commit()
            
            return True, f"Successfully created table '{table_name}' with {len(data_records)} records", len(data_records)
            
        except Exception as e:
            db.session.rollback()
            return False, f"Table creation failed: {str(e)}", 0
