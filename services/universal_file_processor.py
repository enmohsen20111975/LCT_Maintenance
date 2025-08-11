import pandas as pd
import os
import re
import io
import chardet
import locale
import time
import logging
from typing import List, Dict, Tuple, Any, Optional
from werkzeug.datastructures import FileStorage
from werkzeug.utils import secure_filename
from models import db
from models.base_models import UploadHistory, TableMetadata
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Table, text, inspect
from sqlalchemy.dialects import sqlite, postgresql
from datetime import datetime
import warnings

# Suppress pandas warnings for better user experience
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
warnings.filterwarnings('ignore', category=pd.errors.ParserWarning)
warnings.filterwarnings('ignore', category=UserWarning, module='pandas')
warnings.filterwarnings('ignore', message='Could not infer format')

# PDF processing imports
try:
    import PyPDF2
    import pdfplumber
    import tabula
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False

# Set up logging
logger = logging.getLogger(__name__)


class UniversalFileProcessor:
    """Service class for processing various file types and creating database tables."""
    
    def __init__(self, upload_folder: str):
        self.upload_folder = upload_folder
        os.makedirs(upload_folder, exist_ok=True)
        
        # Set up French locale support
        self.french_locales = ['fr_FR.UTF-8', 'fr_FR', 'French_France.1252', 'fr']
        self.setup_locale()
        
        # French date formats commonly used
        self.french_date_formats = [
            '%d/%m/%Y',     # 25/12/2023
            '%d-%m-%Y',     # 25-12-2023
            '%d.%m.%Y',     # 25.12.2023
            '%d/%m/%y',     # 25/12/23
            '%d-%m-%y',     # 25-12-23
            '%d.%m.%y',     # 25.12.23
            '%d %B %Y',     # 25 décembre 2023
            '%d %b %Y',     # 25 déc 2023
            '%B %d, %Y',    # décembre 25, 2023
            '%d %B %y',     # 25 décembre 23
        ]
        
        # Enhanced encoding priority for French text
        self.encoding_priority = [
            'utf-8',
            'iso-8859-1',   # Latin-1, common for French
            'windows-1252', # Windows Western European
            'cp1252',       # Windows code page 1252
            'iso-8859-15',  # Latin-9, includes Euro symbol
            'utf-16',
            'utf-16le',
            'utf-16be'
        ]
    
    def setup_locale(self):
        """Set up French locale for date parsing."""
        for loc in self.french_locales:
            try:
                locale.setlocale(locale.LC_TIME, loc)
                break
            except:
                continue
    
    def get_file_type(self, filename: str) -> str:
        """Determine the type of file based on its extension."""
        if not filename or '.' not in filename:
            return 'unknown'
        
        ext = filename.rsplit('.', 1)[1].lower()
        
        if ext in {'xlsx', 'xls', 'xlsm', 'xlsb'}:
            return 'excel'
        elif ext in {'csv'}:
            return 'csv'
        elif ext in {'txt', 'tsv'}:
            return 'text'
        elif ext in {'pdf'}:
            return 'pdf'
        else:
            return 'unknown'
    
    def _convert_to_json_serializable(self, obj):
        """Convert numpy/pandas types to JSON serializable Python types."""
        import datetime
        import decimal
        
        try:
            if pd.isna(obj):  # pandas NaN, None, etc.
                return None
            elif hasattr(obj, 'item'):  # numpy scalars
                return obj.item()
            elif hasattr(obj, 'tolist'):  # numpy arrays
                return obj.tolist()
            elif isinstance(obj, (pd.Timestamp, pd.Timedelta)):
                return str(obj)
            elif isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
                return str(obj)
            elif isinstance(obj, decimal.Decimal):
                return float(obj)
            elif isinstance(obj, bytes):
                return obj.decode('utf-8', errors='ignore')
            elif hasattr(obj, '__dict__'):  # Complex objects
                return str(obj)
            else:
                return obj
        except Exception:
            # Final fallback - convert anything problematic to string
            return str(obj) if obj is not None else None
    
    def analyze_excel_file(self, file: FileStorage) -> dict:
        """Analyze Excel file and return sheet information with data type suggestions."""
        if not file or self.get_file_type(file.filename) != 'excel':
            raise ValueError("File must be an Excel file")
        
        # Save file temporarily
        temp_path = os.path.join(self.upload_folder, secure_filename(file.filename))
        file.save(temp_path)
        
        try:
            # Read Excel file to get sheet names
            excel_file = pd.ExcelFile(temp_path, engine='openpyxl')
            sheet_names = excel_file.sheet_names
            
            analysis_result = {
                'filename': file.filename,
                'sheets': []
            }
            
            # Analyze each sheet
            for sheet_name in sheet_names:
                try:
                    # Read first few rows to analyze structure
                    df_preview = pd.read_excel(temp_path, sheet_name=sheet_name, nrows=10, engine='openpyxl')
                    df_full = pd.read_excel(temp_path, sheet_name=sheet_name, engine='openpyxl')
                    
                    # Clean column names
                    df_preview.columns = [self.sanitize_column_name(str(col)) for col in df_preview.columns]
                    df_full.columns = [self.sanitize_column_name(str(col)) for col in df_full.columns]
                    
                    # Analyze columns
                    columns_info = []
                    for col_name in df_full.columns:
                        col_data = df_full[col_name]
                        
                        # Get sample values for preview (convert to JSON serializable)
                        sample_values = [self._convert_to_json_serializable(val) for val in col_data.dropna().head(5).tolist()]
                        
                        # Detect suggested data type
                        suggested_type = self.infer_column_type(col_data)
                        suggested_type_name = str(suggested_type).replace('()', '')
                        
                        # Analyze data quality (convert to native Python types)
                        total_rows = int(len(col_data))
                        null_count = int(col_data.isnull().sum())
                        unique_count = int(col_data.nunique())
                        
                        # Detect potential issues
                        issues = []
                        if null_count > 0:
                            issues.append(f"{null_count} null values")
                        
                        if col_data.dtype == 'object':
                            # Check for mixed types
                            types_in_column = set()
                            for val in col_data.dropna().head(100):
                                types_in_column.add(type(val).__name__)
                            if len(types_in_column) > 1:
                                issues.append(f"Mixed types: {', '.join(types_in_column)}")
                        
                        columns_info.append({
                            'name': col_name,
                            'suggested_type': suggested_type_name,
                            'sample_values': sample_values,
                            'total_rows': total_rows,
                            'null_count': null_count,
                            'unique_count': unique_count,
                            'issues': issues,
                            'current_dtype': str(col_data.dtype)
                        })
                    
                    # Convert preview data to JSON serializable format
                    preview_data = []
                    for record in df_preview.head(5).to_dict('records'):
                        converted_record = {}
                        for key, value in record.items():
                            converted_record[key] = self._convert_to_json_serializable(value)
                        preview_data.append(converted_record)
                    
                    sheet_info = {
                        'name': sheet_name,
                        'rows': int(len(df_full)),
                        'columns': int(len(df_full.columns)),
                        'columns_info': columns_info,
                        'preview_data': preview_data
                    }
                    
                    analysis_result['sheets'].append(sheet_info)
                    
                except Exception as e:
                    logger.warning(f"Could not analyze sheet '{sheet_name}': {e}")
                    analysis_result['sheets'].append({
                        'name': sheet_name,
                        'error': str(e)
                    })
            
            return analysis_result
            
        finally:
            # Clean up temporary file
            try:
                os.remove(temp_path)
            except:
                pass
    
    def process_excel_with_config(self, file: FileStorage, selected_sheets: List[str], column_types: Dict[str, Dict[str, str]]) -> Tuple[str, List[str]]:
        """Process Excel file with user-selected sheets and custom data type configurations."""
        if not file or self.get_file_type(file.filename) != 'excel':
            raise ValueError("File must be an Excel file")
        
        # Save file temporarily
        temp_path = os.path.join(self.upload_folder, secure_filename(file.filename))
        file.save(temp_path)
        
        created_tables = []
        upload_record_id = None
        
        try:
            # Create upload history record and get the ID
            def create_upload_record():
                try:
                    upload_record = UploadHistory(
                        filename=file.filename,
                        original_filename=file.filename,
                        upload_date=datetime.now(),
                        file_type='excel'
                    )
                    db.session.add(upload_record)
                    db.session.commit()
                    record_id = upload_record.id  # Get ID before closing session
                    db.session.close()
                    return record_id
                except Exception as e:
                    db.session.rollback()
                    db.session.close()
                    raise e
            
            upload_record_id = self._retry_database_operation(create_upload_record)
            
            # Process each selected sheet
            for sheet_name in selected_sheets:
                try:
                    # Read the sheet
                    df = pd.read_excel(temp_path, sheet_name=sheet_name, engine='openpyxl')
                    
                    # Clean column names
                    df.columns = [self.sanitize_column_name(str(col)) for col in df.columns]
                    
                    # Apply custom data types if specified
                    sheet_column_types = column_types.get(sheet_name, {})
                    for col_name, custom_type in sheet_column_types.items():
                        if col_name in df.columns and custom_type != 'auto':
                            try:
                                df = self._apply_custom_column_type(df, col_name, custom_type)
                            except Exception as e:
                                logger.warning(f"Could not apply custom type '{custom_type}' to column '{col_name}': {e}")
                    
                    # Clean the dataframe
                    df = self._clean_dataframe_for_sql(df)
                    
                    # Create unique table name
                    base_table_name = self.sanitize_table_name(sheet_name)
                    table_name = base_table_name
                    counter = 1
                    inspector = inspect(db.engine)
                    while table_name in inspector.get_table_names():
                        table_name = f"{base_table_name}_{counter}"
                        counter += 1
                    
                    # Create table and insert data
                    self._create_table_safely(df, table_name)
                    self._insert_data_safely(df, table_name)
                    
                    # Create table metadata record using the upload_record_id
                    def create_metadata():
                        try:
                            table_metadata = TableMetadata(
                                table_name=table_name,
                                original_sheet_name=sheet_name,
                                upload_id=upload_record_id,  # Use the stored ID
                                created_date=datetime.now(),
                                row_count=len(df),
                                column_count=len(df.columns)
                            )
                            db.session.add(table_metadata)
                            db.session.commit()
                            db.session.close()
                        except Exception as e:
                            db.session.rollback()
                            db.session.close()
                            raise e
                    
                    self._retry_database_operation(create_metadata)
                    created_tables.append(table_name)
                    
                    logger.info(f"Successfully processed sheet '{sheet_name}' as table '{table_name}'")
                    
                except Exception as e:
                    logger.error(f"Error processing sheet '{sheet_name}': {e}")
                    raise Exception(f"Error processing sheet '{sheet_name}': {e}")
            
            # Clean up connections after successful processing
            self.cleanup_database_connections()
            return str(upload_record_id), created_tables
            
        except Exception as e:
            # Ensure proper cleanup of database sessions
            try:
                db.session.rollback()
            except:
                pass
            finally:
                self.cleanup_database_connections()
            
            # Clean up any partially created tables
            for table_name in created_tables:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                except:
                    pass
            raise Exception(f"Error processing file: {str(e)}")
        
        finally:
            # Clean up temporary file
            try:
                os.remove(temp_path)
            except:
                pass
    
    def _apply_custom_column_type(self, df: pd.DataFrame, col_name: str, custom_type: str) -> pd.DataFrame:
        """Apply custom data type to a specific column."""
        try:
            if custom_type == 'text':
                df[col_name] = df[col_name].astype(str)
            elif custom_type == 'integer':
                # Convert to numeric, coercing errors to NaN, then fill NaN with 0
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
            elif custom_type == 'float':
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0)
            elif custom_type == 'datetime':
                df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            elif custom_type == 'boolean':
                # Convert to boolean, treating common values
                df[col_name] = df[col_name].map({
                    True: True, False: False, 1: True, 0: False, 
                    '1': True, '0': False, 'true': True, 'false': False,
                    'True': True, 'False': False, 'yes': True, 'no': False,
                    'Yes': True, 'No': False, 'oui': True, 'non': False
                }).fillna(False)
            
            logger.info(f"Applied custom type '{custom_type}' to column '{col_name}'")
            return df
            
        except Exception as e:
            logger.warning(f"Failed to apply custom type '{custom_type}' to column '{col_name}': {e}")
            return df
    
    def allowed_file(self, filename: str) -> bool:
        """Check if file extension is allowed."""
        file_type = self.get_file_type(filename)
        return file_type != 'unknown'
    
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
    
    def detect_encoding(self, file_content: bytes) -> str:
        """Enhanced encoding detection with French text support."""
        # First try chardet
        try:
            result = chardet.detect(file_content)
            detected_encoding = result.get('encoding')
            confidence = result.get('confidence', 0)
            
            # If confidence is high enough, use detected encoding
            if detected_encoding and confidence > 0.7:
                return detected_encoding.lower()
        except:
            pass
        
        # Try encodings in priority order for French text
        for encoding in self.encoding_priority:
            try:
                test_decode = file_content.decode(encoding)
                # Test if it contains French characters properly
                if self._test_french_text(test_decode):
                    return encoding
                return encoding  # If no French-specific test passes, use first working encoding
            except UnicodeDecodeError:
                continue
        
        # Fallback to utf-8
        return 'utf-8'
    
    def _test_french_text(self, text: str) -> bool:
        """Test if text contains French characters and is properly decoded."""
        # Common French characters and words
        french_indicators = [
            'à', 'á', 'â', 'ä', 'ç', 'è', 'é', 'ê', 'ë', 'î', 'ï', 'ô', 'ö', 'ù', 'ú', 'û', 'ü', 'ÿ',
            'À', 'Á', 'Â', 'Ä', 'Ç', 'È', 'É', 'Ê', 'Ë', 'Î', 'Ï', 'Ô', 'Ö', 'Ù', 'Ú', 'Û', 'Ü', 'Ÿ',
            'où', 'être', 'avoir', 'voilà', 'français', 'numéro'
        ]
        
        # Check for French characters or words
        for indicator in french_indicators:
            if indicator in text:
                return True
        return False
    
    def parse_french_dates(self, date_series: pd.Series) -> pd.Series:
        """Parse French date formats with improved error handling."""
        if date_series.dtype == 'object':
            # Try to parse French dates with specific formats first
            for date_format in self.french_date_formats:
                try:
                    parsed = pd.to_datetime(date_series, format=date_format, errors='coerce')
                    # If we got some valid dates, return this result
                    if not parsed.isna().all():
                        return parsed
                except:
                    continue
            
            # Try general date parsing with French locale and suppress warnings
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    return pd.to_datetime(date_series, errors='coerce', dayfirst=True, infer_datetime_format=True)
            except:
                pass
        
        return date_series
    
    def process_excel_file(self, file: FileStorage) -> List[Dict[str, Any]]:
        """Process Excel file and extract data with French format support."""
        # Save file temporarily
        filename = self.secure_filename(file.filename)
        filepath = os.path.join(self.upload_folder, filename)
        file.save(filepath)
        
        try:
            # Read all sheets without deprecated date_parser
            excel_data = pd.read_excel(
                filepath, 
                sheet_name=None, 
                engine='openpyxl'
            )
            
            sheets_data = []
            for sheet_name, df in excel_data.items():
                if not df.empty:
                    # Process French formatting
                    df = self._process_french_data(df)
                    
                    sheets_data.append({
                        'name': sheet_name,
                        'data': df,
                        'original_filename': file.filename
                    })
            
            return sheets_data
            
        finally:
            # Clean up temporary file
            if os.path.exists(filepath):
                os.remove(filepath)
    
    def process_csv_file(self, file: FileStorage) -> List[Dict[str, Any]]:
        """Process CSV file and extract data with French format support."""
        # Read file content to detect encoding
        file_content = file.read()
        file.seek(0)  # Reset file pointer
        
        encoding = self.detect_encoding(file_content)
        
        try:
            # Try different separators (French files often use semicolon)
            separators = [';', ',', '\t', '|']  # Semicolon first for French CSV
            df = None
            used_separator = None
            
            for sep in separators:
                try:
                    file.seek(0)
                    df = pd.read_csv(
                        file, 
                        encoding=encoding, 
                        separator=sep, 
                        low_memory=False,
                        decimal=',',  # French decimal separator
                        thousands=' ',  # French thousands separator
                        dayfirst=True  # Day-first date parsing for French dates
                    )
                    # Check if we got meaningful columns (more than 1 column or good data)
                    if len(df.columns) > 1 or (len(df.columns) == 1 and len(df) > 0):
                        used_separator = sep
                        break
                except:
                    continue
            
            # If semicolon didn't work, try with standard decimal point
            if df is None or len(df.columns) <= 1:
                for sep in separators:
                    try:
                        file.seek(0)
                        df = pd.read_csv(
                            file, 
                            encoding=encoding, 
                            separator=sep, 
                            low_memory=False,
                            dayfirst=True
                        )
                        if len(df.columns) > 1 or (len(df.columns) == 1 and len(df) > 0):
                            used_separator = sep
                            break
                    except:
                        continue
            
            if df is None or df.empty:
                raise ValueError("Could not parse CSV file with any common separator")
            
            # Process French date columns
            df = self._process_french_data(df)
            
            filename_without_ext = os.path.splitext(file.filename)[0]
            return [{
                'name': filename_without_ext,
                'data': df,
                'original_filename': file.filename
            }]
            
        except Exception as e:
            raise ValueError(f"Error processing CSV file: {str(e)}")
    
    def _process_french_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process DataFrame to handle French number and date formats."""
        for col in df.columns:
            series = df[col]
            
            # Skip if already numeric or datetime
            if pd.api.types.is_numeric_dtype(series) or pd.api.types.is_datetime64_any_dtype(series):
                continue
            
            if series.dtype == 'object':
                # Try to parse as French dates first
                date_series = self.parse_french_dates(series)
                if not date_series.isna().all() and not date_series.equals(series):
                    df[col] = date_series
                    continue
                
                # Try to parse as French numbers (comma as decimal)
                try:
                    # Check if the column contains French-style numbers
                    sample = series.dropna().astype(str).str.strip()
                    if len(sample) > 0:
                        # Look for patterns like "1,23" or "1 234,56"
                        french_number_pattern = r'^\d{1,3}(\s\d{3})*,\d+$|^\d+,\d+$'
                        if sample.str.match(french_number_pattern).any():
                            # Convert French numbers to standard format
                            converted = (sample
                                       .str.replace(' ', '', regex=False)  # Remove thousand separators
                                       .str.replace(',', '.', regex=False))  # Replace comma with dot
                            numeric_series = pd.to_numeric(converted, errors='coerce')
                            if not numeric_series.isna().all():
                                df[col] = numeric_series
                                continue
                except:
                    pass
        
        return df
    
    def process_text_file(self, file: FileStorage) -> List[Dict[str, Any]]:
        """Process text file (TXT, TSV) with French format support."""
        # Read file content to detect encoding
        file_content = file.read()
        file.seek(0)  # Reset file pointer
        
        encoding = self.detect_encoding(file_content)
        
        try:
            # Determine separator based on file extension
            ext = file.filename.rsplit('.', 1)[1].lower()
            separator = '\t' if ext == 'tsv' else None
            
            # If no specific separator, try to detect (French preference order)
            if separator is None:
                separators = [';', '\t', ',', '|', ' ']  # Semicolon first for French
                df = None
                
                for sep in separators:
                    try:
                        file.seek(0)
                        df = pd.read_csv(
                            file, 
                            encoding=encoding, 
                            separator=sep, 
                            low_memory=False,
                            decimal=',',
                            thousands=' ',
                            dayfirst=True
                        )
                        if len(df.columns) > 1:
                            break
                    except:
                        continue
                
                # If French format didn't work, try standard format
                if df is None or len(df.columns) <= 1:
                    for sep in separators:
                        try:
                            file.seek(0)
                            df = pd.read_csv(
                                file, 
                                encoding=encoding, 
                                separator=sep, 
                                low_memory=False,
                                dayfirst=True
                            )
                            if len(df.columns) > 1:
                                break
                        except:
                            continue
            else:
                # Use specified separator with French format first
                try:
                    file.seek(0)
                    df = pd.read_csv(
                        file, 
                        encoding=encoding, 
                        separator=separator, 
                        low_memory=False,
                        decimal=',',
                        thousands=' ',
                        dayfirst=True
                    )
                except:
                    # Fallback to standard format
                    file.seek(0)
                    df = pd.read_csv(
                        file, 
                        encoding=encoding, 
                        separator=separator, 
                        low_memory=False,
                        dayfirst=True
                    )
            
            if df is None or df.empty:
                raise ValueError("Could not parse text file")
            
            # Process French data
            df = self._process_french_data(df)
            
            filename_without_ext = os.path.splitext(file.filename)[0]
            return [{
                'name': filename_without_ext,
                'data': df,
                'original_filename': file.filename
            }]
            
        except Exception as e:
            raise ValueError(f"Error processing text file: {str(e)}")
    
    def process_pdf_file(self, file: FileStorage) -> List[Dict[str, Any]]:
        """Process PDF file and extract tabular data."""
        if not PDF_AVAILABLE:
            raise ValueError("PDF processing libraries not available. Please install PyPDF2, pdfplumber, and tabula-py.")
        
        # Save file temporarily
        filename = self.secure_filename(file.filename)
        filepath = os.path.join(self.upload_folder, filename)
        file.save(filepath)
        
        try:
            tables_data = []
            
            # Method 1: Try tabula-py for table extraction
            try:
                dfs = tabula.read_pdf(filepath, pages='all', multiple_tables=True, pandas_options={'header': 0})
                for i, df in enumerate(dfs):
                    if not df.empty and len(df.columns) > 1:
                        table_name = f"{os.path.splitext(file.filename)[0]}_table_{i+1}"
                        tables_data.append({
                            'name': table_name,
                            'data': df,
                            'original_filename': file.filename
                        })
            except Exception as e:
                print(f"Tabula extraction failed: {e}")
            
            # Method 2: Try pdfplumber for table extraction
            if not tables_data:
                try:
                    with pdfplumber.open(filepath) as pdf:
                        for page_num, page in enumerate(pdf.pages):
                            tables = page.extract_tables()
                            for table_num, table in enumerate(tables):
                                if table and len(table) > 1:  # Must have header + at least one data row
                                    # Convert to DataFrame
                                    df = pd.DataFrame(table[1:], columns=table[0])
                                    # Clean empty columns/rows
                                    df = df.dropna(how='all').dropna(axis=1, how='all')
                                    
                                    if not df.empty:
                                        table_name = f"{os.path.splitext(file.filename)[0]}_page_{page_num+1}_table_{table_num+1}"
                                        tables_data.append({
                                            'name': table_name,
                                            'data': df,
                                            'original_filename': file.filename
                                        })
                except Exception as e:
                    print(f"PDFplumber extraction failed: {e}")
            
            # Method 3: Extract text and try to parse as structured data
            if not tables_data:
                try:
                    with pdfplumber.open(filepath) as pdf:
                        all_text = ""
                        for page in pdf.pages:
                            all_text += page.extract_text() + "\n"
                        
                        # Try to find tabular patterns in text
                        lines = all_text.strip().split('\n')
                        # Filter out empty lines
                        lines = [line.strip() for line in lines if line.strip()]
                        
                        if len(lines) > 1:
                            # Create a simple text table
                            text_data = []
                            for line in lines[:100]:  # Limit to first 100 lines
                                # Split by multiple spaces or tabs
                                parts = re.split(r'\s{2,}|\t', line)
                                if len(parts) > 1:
                                    text_data.append(parts)
                            
                            if text_data:
                                # Use first row as headers
                                if len(text_data) > 1:
                                    df = pd.DataFrame(text_data[1:], columns=text_data[0])
                                    table_name = f"{os.path.splitext(file.filename)[0]}_text_data"
                                    tables_data.append({
                                        'name': table_name,
                                        'data': df,
                                        'original_filename': file.filename
                                    })
                except Exception as e:
                    print(f"Text extraction failed: {e}")
            
            if not tables_data:
                raise ValueError("No tabular data found in PDF file")
            
            return tables_data
            
        finally:
            # Clean up temporary file
            if os.path.exists(filepath):
                os.remove(filepath)
    
    def secure_filename_helper(self, filename: str) -> str:
        """Generate a secure filename."""
        return secure_filename(filename)
    
    def infer_column_type(self, series: pd.Series) -> str:
        """Infer the appropriate SQL column type for a pandas Series with French format support."""
        # Drop null values for type inference
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return String(255)
        
        # Check if it's already datetime - be very strict about this
        if pd.api.types.is_datetime64_any_dtype(non_null_series):
            # Verify that all values are actually valid timestamps
            try:
                # Test conversion to Python datetime objects
                has_valid_datetime = False
                for value in non_null_series.head(10):
                    if isinstance(value, pd.Timestamp):
                        value.to_pydatetime()
                        has_valid_datetime = True
                    elif isinstance(value, datetime):
                        has_valid_datetime = True
                
                if has_valid_datetime:
                    return DateTime
                else:
                    logger.warning(f"Column appears to be datetime64 but contains no valid datetime values, treating as string")
                    return String(255)
            except:
                # If conversion fails, treat as string
                logger.warning(f"Column appears to be datetime but has invalid values, treating as string")
                return String(255)
        
        # Check if it's numeric (be more careful with mixed types)
        if pd.api.types.is_numeric_dtype(non_null_series):
            # Check for mixed numeric/string data that might cause issues
            try:
                # Test if all non-null values are actually numeric
                numeric_test = pd.to_numeric(non_null_series, errors='coerce')
                if numeric_test.notna().sum() / len(non_null_series) > 0.95:  # 95% must be numeric
                    # Check if it's integer (and doesn't have NaN that would break int conversion)
                    if pd.api.types.is_integer_dtype(non_null_series):
                        # Ensure no infinity values
                        if not (non_null_series == float('inf')).any() and not (non_null_series == float('-inf')).any():
                            return Integer
                    return Float
                else:
                    # Mixed data, treat as string
                    return String(255)
            except:
                return String(255)
        
        # For object type, be extremely conservative about datetime detection
        if non_null_series.dtype == 'object':
            # Only consider datetime if we see explicit datetime patterns
            datetime_candidates = 0
            total_tested = 0
            
            for value in non_null_series.head(20):  # Test more samples
                total_tested += 1
                try:
                    if isinstance(value, (pd.Timestamp, datetime)):
                        datetime_candidates += 1
                    elif isinstance(value, str):
                        # Only consider strings that look like dates
                        value_str = value.strip()
                        # Look for common date patterns: YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY, etc.
                        import re
                        date_patterns = [
                            r'^\d{4}-\d{1,2}-\d{1,2}',  # YYYY-MM-DD
                            r'^\d{1,2}[/-]\d{1,2}[/-]\d{4}',  # DD/MM/YYYY or MM/DD/YYYY
                            r'^\d{4}[/-]\d{1,2}[/-]\d{1,2}',  # YYYY/MM/DD
                            r'^\d{1,2} \w+ \d{4}',  # DD Month YYYY
                        ]
                        
                        has_date_pattern = any(re.match(pattern, value_str) for pattern in date_patterns)
                        
                        if has_date_pattern:
                            # Try parsing as datetime
                            parsed_date = pd.to_datetime(value_str, errors='coerce')
                            if not pd.isna(parsed_date):
                                datetime_candidates += 1
                except:
                    pass
            
            # Only treat as datetime if a high percentage matches and we have explicit patterns
            if total_tested >= 5 and datetime_candidates >= max(5, total_tested * 0.9):
                logger.info(f"Column detected as DateTime based on {datetime_candidates}/{total_tested} valid datetime patterns")
                return DateTime
            
            # Try to parse as numbers (French format with comma)
            try:
                sample_str = non_null_series.astype(str).str.strip()
                
                # Check for French number patterns
                french_number_pattern = r'^\d{1,3}(\s\d{3})*,\d+$|^\d+,\d+$|^\d+$'
                if len(sample_str) > 0 and sample_str.str.match(french_number_pattern).any():
                    # Try to convert and see if it's mostly numeric
                    converted = (sample_str
                               .str.replace(' ', '', regex=False)
                               .str.replace(',', '.', regex=False))
                    numeric_test = pd.to_numeric(converted, errors='coerce')
                    if numeric_test.notna().sum() / len(numeric_test) > 0.7:  # 70% success rate
                        # Check if integers (and safe to convert)
                        try:
                            int_test = numeric_test.astype('Int64', errors='ignore')
                            if (numeric_test == int_test).all():
                                return Integer
                            else:
                                return Float
                        except:
                            return Float
            except:
                pass
        
        # Check string length for optimal storage
        try:
            max_length = non_null_series.astype(str).str.len().max()
            if pd.isna(max_length) or max_length > 500:
                return Text
            else:
                return String(min(int(max_length * 2), 255))  # Give some buffer, cap at 255
        except:
            return String(255)
    
    def create_table_from_dataframe(self, df: pd.DataFrame, table_name: str) -> Table:
        """Create SQLAlchemy table from DataFrame."""
        # Sanitize column names
        df.columns = [self.sanitize_column_name(col) for col in df.columns]
        
        # Handle duplicate column names
        seen_columns = set()
        new_columns = []
        for col in df.columns:
            original_col = col
            counter = 1
            while col in seen_columns:
                col = f"{original_col}_{counter}"
                counter += 1
            seen_columns.add(col)
            new_columns.append(col)
        df.columns = new_columns
        
        # Create table metadata
        columns = [Column('id', Integer, primary_key=True)]
        
        for col_name in df.columns:
            col_type = self.infer_column_type(df[col_name])
            
            # Double-check datetime columns to ensure data compatibility
            if col_type == DateTime:
                # Verify that data can actually be converted to datetime
                try:
                    test_series = df[col_name].dropna()
                    if len(test_series) > 0:
                        # Try to convert a sample to datetime
                        for value in test_series.head(5):
                            if value is not None:
                                if isinstance(value, pd.Timestamp):
                                    value.to_pydatetime()
                                elif isinstance(value, datetime):
                                    pass  # Already datetime
                                else:
                                    # Try parsing string
                                    parsed = pd.to_datetime(value, errors='coerce')
                                    if pd.isna(parsed):
                                        raise ValueError(f"Cannot parse as datetime: {value}")
                        logger.info(f"Confirmed column '{col_name}' as DateTime")
                except Exception as e:
                    logger.warning(f"Column '{col_name}' detected as DateTime but data validation failed: {e}")
                    logger.warning(f"Converting column '{col_name}' to String instead")
                    col_type = String(255)
            
            columns.append(Column(col_name, col_type))
            logger.debug(f"Column '{col_name}' created with type: {col_type}")
        
        table = Table(table_name, db.metadata, *columns, extend_existing=True)
        return table
    
    def process_file(self, file: FileStorage) -> Tuple[str, List[str]]:
        """Process any supported file type and create database tables."""
        if not file or not self.allowed_file(file.filename):
            raise ValueError("Unsupported file type")
        
        file_type = self.get_file_type(file.filename)
        
        # Extract data based on file type
        if file_type == 'excel':
            data_sources = self.process_excel_file(file)
        elif file_type == 'csv':
            data_sources = self.process_csv_file(file)
        elif file_type == 'text':
            data_sources = self.process_text_file(file)
        elif file_type == 'pdf':
            data_sources = self.process_pdf_file(file)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
        
        if not data_sources:
            raise ValueError("No data found in file")
        
        # Create database tables with proper transaction management
        created_tables = []
        upload_record = None
        
        try:
            # Pre-process data sources to identify and fix potential issues
            logger.info(f"Pre-processing {len(data_sources)} data sources")
            processed_data_sources = []
            
            for idx, data_source in enumerate(data_sources):
                df = data_source['data']
                source_name = data_source['name']
                
                logger.info(f"Processing data source {idx + 1}: '{source_name}' with {len(df)} rows")
                
                # Detect and warn about potential data issues
                for column in df.columns:
                    # Check for mixed data types in object columns
                    if df[column].dtype == 'object':
                        sample_values = df[column].dropna().head(10).tolist()
                        logger.debug(f"Column '{column}' sample values: {sample_values}")
                    
                    # Check for problematic datetime values
                    if df[column].dtype == 'datetime64[ns]':
                        invalid_dates = df[column].apply(lambda x: isinstance(x, pd.Timestamp) and (x.year < 1900 or x.year > 2100))
                        if invalid_dates.any():
                            logger.warning(f"Column '{column}' contains {invalid_dates.sum()} invalid timestamp(s)")
                
                processed_data_sources.append(data_source)
            
            data_sources = processed_data_sources
            
            # Create upload history record with retry logic and proper session management
            def create_upload_record():
                try:
                    upload_record = UploadHistory(
                        filename=file.filename,
                        original_filename=file.filename,
                        upload_date=datetime.now(),
                        file_type=file_type
                    )
                    db.session.add(upload_record)
                    db.session.commit()
                    record_id = upload_record.id  # Get ID before closing session
                    db.session.close()
                    return record_id
                except Exception as e:
                    db.session.rollback()
                    db.session.close()
                    raise e
            
            upload_record_id = self._retry_database_operation(create_upload_record)
            
            for data_source in data_sources:
                df = data_source['data']
                base_table_name = self.sanitize_table_name(data_source['name'])
                
                # Ensure unique table name
                table_name = base_table_name
                counter = 1
                # Use inspector to check if table exists
                inspector = inspect(db.engine)
                while table_name in inspector.get_table_names():
                    table_name = f"{base_table_name}_{counter}"
                    counter += 1
                
                # Create table using a separate connection to avoid locks
                self._create_table_safely(df, table_name)
                
                # Insert data using the main session
                self._insert_data_safely(df, table_name)
                
                # Create table metadata record with retry logic and proper session management
                def create_metadata():
                    try:
                        table_metadata = TableMetadata(
                            table_name=table_name,
                            original_sheet_name=data_source['name'],
                            upload_id=upload_record_id,  # Use the stored ID
                            created_date=datetime.now(),
                            row_count=len(df),
                            column_count=len(df.columns)
                        )
                        db.session.add(table_metadata)
                        db.session.commit()
                        db.session.close()
                    except Exception as e:
                        db.session.rollback()
                        db.session.close()
                        raise e
                
                self._retry_database_operation(create_metadata)
                created_tables.append(table_name)
            
            # Clean up connections after successful processing
            self.cleanup_database_connections()
            return str(upload_record_id), created_tables
            
        except Exception as e:
            # Ensure proper cleanup of database sessions
            try:
                db.session.rollback()
            except:
                pass
            finally:
                self.cleanup_database_connections()
            
            # Clean up any partially created tables
            for table_name in created_tables:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
                except:
                    pass
            raise Exception(f"Error processing file: {str(e)}")
    
    def cleanup_database_connections(self):
        """Clean up database connections to prevent pool exhaustion."""
        try:
            db.session.close()
            if hasattr(db.session, 'remove'):
                db.session.remove()
            db.engine.dispose()
            logger.debug("Database connections cleaned up successfully")
        except Exception as e:
            logger.warning(f"Error during connection cleanup: {e}")
    
    def _retry_database_operation(self, operation_func, max_retries=5, base_delay=0.1):
        """Retry database operations with exponential backoff for lock handling."""
        last_exception = None
        
        for attempt in range(max_retries):
            try:
                return operation_func()
            except Exception as e:
                last_exception = e
                error_msg = str(e).lower()
                
                # Check if it's a database lock or connection timeout error
                if ('database is locked' in error_msg or 
                    'database locked' in error_msg or
                    'sqlite3.operationalerror' in error_msg or
                    'connection timed out' in error_msg or
                    'queuepool limit' in error_msg or
                    'timeout' in error_msg):
                    
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"Database connection issue detected (attempt {attempt + 1}/{max_retries}). "
                            f"Error: {error_msg[:100]}... Retrying in {delay:.2f} seconds..."
                        )
                        time.sleep(delay)
                        
                        # Aggressive connection cleanup
                        try:
                            db.session.close()
                            db.session.remove()
                            db.engine.dispose()
                        except Exception:
                            pass
                        
                        continue
                    else:
                        logger.error(f"Max retries ({max_retries}) exceeded for database connection issues")
                        break
                else:
                    # Not a connection error, re-raise immediately
                    raise
        
        # If we get here, all retries failed
        raise last_exception
    
    def _create_table_safely(self, df: pd.DataFrame, table_name: str) -> None:
        """Create table with better error handling and connection management."""
        def create_operation():
            # Create table using raw SQL to avoid session conflicts
            table = self.create_table_from_dataframe(df, table_name)
            
            # Use raw connection for table creation with retry logic
            with db.engine.begin() as conn:
                table.create(conn, checkfirst=True)
        
        self._retry_database_operation(create_operation)
    
    def _clean_dataframe_for_sql(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame data to be compatible with SQL insertion."""
        df_clean = df.copy()
        
        logger.debug(f"Cleaning DataFrame with {len(df_clean)} rows and {len(df_clean.columns)} columns")
        
        # Handle different data types and clean problematic values
        for column in df_clean.columns:
            col_dtype = df_clean[column].dtype
            logger.debug(f"Processing column '{column}' with dtype: {col_dtype}")
            
            # Convert pandas NaT (Not a Time) to None or valid datetime
            if col_dtype == 'datetime64[ns]':
                # Replace NaT and invalid timestamps with None
                def clean_datetime(x):
                    if pd.isna(x):
                        return None
                    if isinstance(x, pd.Timestamp):
                        try:
                            # Check for valid date range
                            if x.year < 1900 or x.year > 2100:
                                logger.warning(f"Invalid timestamp in column '{column}': {x}")
                                return None
                            # Convert to Python datetime object for SQLite compatibility
                            return x.to_pydatetime()
                        except (ValueError, OverflowError):
                            logger.warning(f"Cannot convert timestamp in column '{column}': {x}")
                            return None
                    elif isinstance(x, datetime):
                        return x
                    else:
                        # Try to parse string dates
                        try:
                            parsed_date = pd.to_datetime(x, errors='coerce')
                            if pd.isna(parsed_date):
                                return None
                            return parsed_date.to_pydatetime()
                        except:
                            return None
                
                df_clean[column] = df_clean[column].apply(clean_datetime)
            
            # Handle integer columns with NaN - ensure they are Python int, not numpy int
            elif col_dtype in ['int64', 'int32', 'int16', 'int8']:
                # For integer columns, replace NaN with None and convert to Python int
                def clean_integer(x):
                    if pd.isna(x):
                        return None
                    try:
                        # Convert numpy integers to Python integers
                        if hasattr(x, 'item'):
                            return int(x.item())
                        return int(x)
                    except (ValueError, OverflowError, TypeError):
                        logger.warning(f"Cannot convert to int in column '{column}': {x}")
                        return None
                
                df_clean[column] = df_clean[column].apply(clean_integer)
            
            # Handle float columns with inf values - ensure they are Python float, not numpy float
            elif col_dtype in ['float64', 'float32']:
                # Replace NaN and infinity with None and convert to Python float
                def clean_float(x):
                    if pd.isna(x) or x == float('inf') or x == float('-inf'):
                        return None
                    try:
                        # Convert numpy floats to Python floats
                        if hasattr(x, 'item'):
                            return float(x.item())
                        return float(x)
                    except (ValueError, OverflowError, TypeError):
                        logger.warning(f"Cannot convert to float in column '{column}': {x}")
                        return None
                
                df_clean[column] = df_clean[column].apply(clean_float)
            
            # Handle boolean columns
            elif col_dtype == 'bool':
                def clean_boolean(x):
                    if pd.isna(x):
                        return None
                    try:
                        # Convert numpy bool to Python bool
                        if hasattr(x, 'item'):
                            return bool(x.item())
                        return bool(x)
                    except (ValueError, TypeError):
                        logger.warning(f"Cannot convert to bool in column '{column}': {x}")
                        return None
                
                df_clean[column] = df_clean[column].apply(clean_boolean)
            
            # Handle object columns (strings, mixed types)
            elif col_dtype == 'object':
                # Replace NaN and convert to string, handling special cases
                def clean_object(x):
                    if pd.isna(x) or x is None:
                        return None
                    try:
                        # Handle datetime objects in object columns
                        if isinstance(x, (pd.Timestamp, datetime)):
                            return x.to_pydatetime() if isinstance(x, pd.Timestamp) else x
                        # Handle string values of 'nan', 'NaN', etc.
                        str_x = str(x).strip().lower()
                        if str_x in ['nan', 'nat', 'none', 'null', '']:
                            return None
                        return str(x)
                    except Exception as e:
                        logger.warning(f"Cannot convert to string in column '{column}': {x}, error: {e}")
                        return None
                
                df_clean[column] = df_clean[column].apply(clean_object)
        
        # Final pass to replace any remaining NaN values with None
        df_clean = df_clean.where(pd.notnull(df_clean), None)
        
        logger.debug("DataFrame cleaning completed")
        return df_clean
    
    def _insert_data_safely(self, df: pd.DataFrame, table_name: str) -> None:
        """Insert data with proper error handling and retry logic."""
        def insert_operation():
            try:
                # Clean the data thoroughly
                df_clean = self._clean_dataframe_for_sql(df)
                logger.info(f"Inserting {len(df_clean)} rows into table '{table_name}'")
                
                # Convert DataFrame to list of dictionaries
                records = df_clean.to_dict('records')
                
                # Insert records using raw connection to avoid session conflicts
                with db.engine.begin() as conn:
                    # Get table metadata
                    metadata = db.MetaData()
                    table = Table(table_name, metadata, autoload_with=conn)
                    
                    # Insert data in chunks to avoid memory issues
                    chunk_size = 1000
                    for i in range(0, len(records), chunk_size):
                        chunk = records[i:i + chunk_size]
                        if chunk:  # Only insert if chunk is not empty
                            try:
                                conn.execute(table.insert(), chunk)
                                logger.debug(f"Inserted chunk {i//chunk_size + 1} ({len(chunk)} records)")
                            except Exception as chunk_error:
                                logger.error(f"Error inserting chunk {i//chunk_size + 1}: {chunk_error}")
                                # Try inserting records one by one to identify problematic rows
                                for j, record in enumerate(chunk):
                                    try:
                                        # Additional cleaning for problematic records
                                        cleaned_record = {}
                                        for key, value in record.items():
                                            if value is not None:
                                                # Handle datetime conversion issues
                                                if isinstance(value, (pd.Timestamp, datetime)):
                                                    try:
                                                        cleaned_record[key] = value.to_pydatetime() if isinstance(value, pd.Timestamp) else value
                                                    except:
                                                        cleaned_record[key] = None
                                                # Handle numpy types
                                                elif hasattr(value, 'item'):
                                                    try:
                                                        cleaned_record[key] = value.item()
                                                    except:
                                                        cleaned_record[key] = str(value) if value is not None else None
                                                # Handle string 'nan' values
                                                elif isinstance(value, str) and value.lower().strip() in ['nan', 'nat', 'none', 'null']:
                                                    cleaned_record[key] = None
                                                else:
                                                    cleaned_record[key] = value
                                            else:
                                                cleaned_record[key] = None
                                        
                                        conn.execute(table.insert(), [cleaned_record])
                                    except Exception as record_error:
                                        logger.error(f"Error inserting record {i+j}: {record_error}")
                                        logger.error(f"Problematic record: {record}")
                                        # Skip this record and continue
                                        continue
                
                logger.info(f"Successfully inserted data into table '{table_name}'")
                
            except Exception as e:
                logger.error(f"Critical error during data insertion: {e}")
                raise
        
        self._retry_database_operation(insert_operation)
