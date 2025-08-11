import pandas as pd
import os
import re
from typing import List, Dict, Tuple, Any
from werkzeug.datastructures import FileStorage
from models import db
from models.base_models import UploadHistory, TableMetadata
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Table
from sqlalchemy.dialects import sqlite, postgresql
from datetime import datetime

class ExcelProcessor:
    """Service class for processing Excel files and creating database tables."""
    
    def __init__(self, upload_folder: str):
        self.upload_folder = upload_folder
        os.makedirs(upload_folder, exist_ok=True)
    
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
            filename = f"{timestamp}_{file.filename}"
            file_path = os.path.join(self.upload_folder, filename)
            file.save(file_path)
            return file_path
        raise ValueError("Invalid file type")
    
    def read_excel_file(self, file_path: str) -> Dict[str, pd.DataFrame]:
        """Read Excel file and return dictionary of sheet names and DataFrames."""
        try:
            # Read all sheets
            excel_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
            return excel_data
        except Exception as e:
            # Try with xlrd engine for older Excel files
            try:
                excel_data = pd.read_excel(file_path, sheet_name=None, engine='xlrd')
                return excel_data
            except Exception:
                raise Exception(f"Could not read Excel file: {str(e)}")
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> Table:
        """Create SQLAlchemy table from pandas DataFrame."""
        columns = []
        
        # Add auto-incrementing ID column
        columns.append(Column('id', Integer, primary_key=True, autoincrement=True))
        
        # Process each column in the DataFrame
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
        return table
    
    def process_excel_file(self, file: FileStorage) -> Tuple[int, List[str]]:
        """
        Process uploaded Excel file and create database tables.
        Returns tuple of (upload_id, list of created table names).
        """
        # Save uploaded file
        file_path = self.save_uploaded_file(file)
        file_size = os.path.getsize(file_path)
        
        # Create upload history record
        upload_record = UploadHistory(
            filename=os.path.basename(file_path),
            original_filename=file.filename,
            file_size=file_size,
            status='processing'
        )
        db.session.add(upload_record)
        db.session.flush()  # Get the ID without committing
        
        try:
            # Read Excel file
            excel_data = self.read_excel_file(file_path)
            
            created_tables = []
            total_records = 0
            
            for sheet_name, df in excel_data.items():
                if df.empty:
                    continue
                
                # Clean the DataFrame
                df = df.dropna(how='all')  # Remove completely empty rows
                df = df.loc[:, ~df.columns.duplicated()]  # Remove duplicate columns
                
                # Clean datetime columns to handle NaT values properly
                for col in df.columns:
                    if pd.api.types.is_datetime64_any_dtype(df[col]):
                        # Convert problematic datetime values to None
                        df[col] = df[col].apply(lambda x: None if pd.isna(x) else x)
                
                if df.empty:
                    continue
                
                # Create table name
                table_name = self.sanitize_table_name(sheet_name)
                
                # Ensure unique table name
                original_table_name = table_name
                counter = 1
                while db.session.query(TableMetadata).filter_by(table_name=table_name).first():
                    table_name = f"{original_table_name}_{counter}"
                    counter += 1
                
                # Create table in database
                table = self.create_table_from_dataframe(df, table_name)
                table.create(db.engine, checkfirst=True)
                
                # Insert data into table
                data_records = []
                for _, row in df.iterrows():
                    record = {}
                    for i, col in enumerate(df.columns):
                        sanitized_col = self.sanitize_column_name(col)
                        # Handle duplicate column names like in table creation
                        original_name = sanitized_col
                        col_counter = 1
                        existing_cols = [c.name for c in table.columns if c.name != 'id']
                        while sanitized_col in existing_cols[:i]:
                            sanitized_col = f"{original_name}_{col_counter}"
                            col_counter += 1
                        
                        value = row[col]
                        # Use safe value conversion to handle NaT and other pandas edge cases
                        record[sanitized_col] = self.safe_value_conversion(value)
                    data_records.append(record)
                
                # Bulk insert data
                if data_records:
                    db.session.execute(table.insert(), data_records)
                
                # Create table metadata record
                table_metadata = TableMetadata(
                    table_name=table_name,
                    original_sheet_name=sheet_name,
                    upload_id=upload_record.id,
                    column_count=len(df.columns),
                    row_count=len(df)
                )
                db.session.add(table_metadata)
                
                created_tables.append(table_name)
                total_records += len(df)
            
            # Update upload record
            upload_record.total_sheets = len(created_tables)
            upload_record.total_records = total_records
            upload_record.status = 'completed'
            
            db.session.commit()
            
            # Clean up uploaded file
            os.remove(file_path)
            
            return upload_record.id, created_tables
            
        except Exception as e:
            db.session.rollback()
            # Update upload record with error
            upload_record.status = 'failed'
            upload_record.error_message = str(e)
            db.session.commit()
            
            # Clean up uploaded file
            if os.path.exists(file_path):
                os.remove(file_path)
            
            raise e
