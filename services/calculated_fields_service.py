"""
Calculated Fields Service

This service provides functionality to add calculated fields to database tables
using mathematical operations, logical conditions, date/time calculations, and statistics.
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import re
import os
import logging

logger = logging.getLogger(__name__)

class CalculatedFieldsService:
    def __init__(self, instance_folder: str = 'instance'):
        self.instance_folder = instance_folder
        
    def get_table_columns(self, table_name: str, database: str = 'excel_data') -> List[Dict[str, Any]]:
        """Get columns information for a table including data types."""
        try:
            db_path = os.path.join(self.instance_folder, f"{database}.db")
            if not os.path.exists(db_path):
                return []
                
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get column info
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns_info = cursor.fetchall()
            
            columns = []
            for col in columns_info:
                col_name = col[1]
                col_type = col[2]
                
                # Analyze sample data to determine actual data type
                cursor.execute(f'SELECT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL LIMIT 100')
                sample_data = cursor.fetchall()
                
                data_type = self._analyze_data_type([row[0] for row in sample_data])
                
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'inferred_type': data_type,
                    'sample_values': [str(row[0]) for row in sample_data[:5]]
                })
            
            conn.close()
            return columns
            
        except Exception as e:
            logger.error(f"Error getting table columns: {str(e)}")
            return []
    
    def _analyze_data_type(self, sample_data: List) -> str:
        """Analyze sample data to determine the most appropriate data type."""
        if not sample_data:
            return 'text'
            
        numeric_count = 0
        date_count = 0
        
        for value in sample_data:
            if value is None:
                continue
                
            # Check if numeric
            try:
                float(str(value))
                numeric_count += 1
                continue
            except (ValueError, TypeError):
                pass
                
            # Check if date
            if self._is_date_like(str(value)):
                date_count += 1
                
        total_count = len([v for v in sample_data if v is not None])
        if total_count == 0:
            return 'text'
            
        # Determine type based on majority
        if numeric_count / total_count > 0.8:
            return 'numeric'
        elif date_count / total_count > 0.8:
            return 'date'
        else:
            return 'text'
    
    def _is_date_like(self, value: str) -> bool:
        """Check if a string value looks like a date."""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, str(value)):
                return True
        return False
    
    def validate_formula(self, formula: str, table_columns: List[str]) -> Dict[str, Any]:
        """Validate a calculation formula."""
        try:
            # Replace column references with dummy values for validation
            test_formula = formula
            
            for col in table_columns:
                # Replace column references [column_name] with test values
                test_formula = test_formula.replace(f'[{col}]', '1')
            
            # Test mathematical functions
            math_functions = {
                'ABS': 'abs',
                'ROUND': 'round',
                'CEIL': 'math.ceil',
                'FLOOR': 'math.floor',
                'SQRT': 'math.sqrt',
                'LOG': 'math.log',
                'SIN': 'math.sin',
                'COS': 'math.cos',
                'TAN': 'math.tan',
                'MAX': 'max',
                'MIN': 'min',
                'AVG': 'statistics.mean',
                'SUM': 'sum'
            }
            
            for func_name, python_func in math_functions.items():
                test_formula = test_formula.replace(func_name, python_func)
            
            # Basic validation - try to compile the expression
            compile(test_formula, '<string>', 'eval')
            
            return {'valid': True, 'message': 'Formula is valid'}
            
        except Exception as e:
            return {'valid': False, 'message': f'Invalid formula: {str(e)}'}
    
    def create_calculated_field(self, table_name: str, field_name: str, formula: str, 
                              field_type: str = 'REAL', database: str = 'excel_data') -> Dict[str, Any]:
        """Create a new calculated field in the table."""
        try:
            db_path = os.path.join(self.instance_folder, f"{database}.db")
            if not os.path.exists(db_path):
                return {'success': False, 'error': 'Database not found'}
            
            # Get table columns for validation
            columns = self.get_table_columns(table_name, database)
            column_names = [col['name'] for col in columns]
            
            # Validate formula
            validation = self.validate_formula(formula, column_names)
            if not validation['valid']:
                return {'success': False, 'error': validation['message']}
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check if field already exists
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            existing_columns = [col[1] for col in cursor.fetchall()]
            
            if field_name in existing_columns:
                return {'success': False, 'error': f'Field "{field_name}" already exists'}
            
            # Add the new column
            cursor.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{field_name}" {field_type}')
            
            # Calculate values for all rows
            cursor.execute(f'SELECT rowid, * FROM "{table_name}"')
            rows = cursor.fetchall()
            
            # Get column names (excluding rowid)
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            all_columns = [col[1] for col in cursor.fetchall()]
            
            updated_count = 0
            
            for row in rows:
                rowid = row[0]
                row_data = row[1:]  # Exclude rowid
                
                try:
                    # Calculate the value for this row
                    calculated_value = self._calculate_value(formula, all_columns[:-1], row_data)  # Exclude new column
                    
                    # Update the row with calculated value
                    cursor.execute(f'UPDATE "{table_name}" SET "{field_name}" = ? WHERE rowid = ?', 
                                 (calculated_value, rowid))
                    updated_count += 1
                    
                except Exception as calc_error:
                    logger.warning(f"Error calculating value for row {rowid}: {calc_error}")
                    # Leave as NULL for this row
                    continue
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'message': f'Calculated field "{field_name}" created successfully',
                'rows_updated': updated_count,
                'formula': formula
            }
            
        except Exception as e:
            logger.error(f"Error creating calculated field: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def _calculate_value(self, formula: str, column_names: List[str], row_data: tuple) -> Any:
        """Calculate the value for a single row."""
        import math
        import statistics
        from datetime import datetime, timedelta
        
        # Create a safe execution environment
        safe_dict = {
            '__builtins__': {},
            'math': math,
            'statistics': statistics,
            'datetime': datetime,
            'timedelta': timedelta,
            'abs': abs,
            'round': round,
            'max': max,
            'min': min,
            'sum': sum,
            'len': len,
            'int': int,
            'float': float,
            'str': str
        }
        
        # Add column values to the environment
        for i, col_name in enumerate(column_names):
            if i < len(row_data):
                value = row_data[i]
                # Convert to appropriate type if possible
                if value is not None:
                    try:
                        # Try to convert to numeric
                        if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                            value = float(value)
                    except:
                        pass
                safe_dict[col_name.replace(' ', '_')] = value
        
        # Replace column references in formula
        calc_formula = formula
        for col_name in column_names:
            # Replace [column_name] with column_name_value
            calc_formula = calc_formula.replace(f'[{col_name}]', col_name.replace(' ', '_'))
        
        # Handle special functions
        calc_formula = self._replace_special_functions(calc_formula)
        
        try:
            result = eval(calc_formula, safe_dict)
            return result
        except Exception as e:
            raise ValueError(f"Error evaluating formula: {e}")
    
    def _replace_special_functions(self, formula: str) -> str:
        """Replace special functions with their Python equivalents."""
        replacements = {
            'ABS(': 'abs(',
            'ROUND(': 'round(',
            'CEIL(': 'math.ceil(',
            'FLOOR(': 'math.floor(',
            'SQRT(': 'math.sqrt(',
            'LOG(': 'math.log(',
            'SIN(': 'math.sin(',
            'COS(': 'math.cos(',
            'TAN(': 'math.tan(',
            'IF(': 'self._if_function(',
            'AND(': 'all([',
            'OR(': 'any([',
            'TODAY()': 'datetime.now().date()',
            'NOW()': 'datetime.now()',
        }
        
        result = formula
        for old, new in replacements.items():
            result = result.replace(old, new)
        
        # Handle closing brackets for AND/OR
        result = result.replace('AND(', 'all([').replace(', ', ', ') if 'all([' in result else result
        result = result.replace('OR(', 'any([').replace(', ', ', ') if 'any([' in result else result
        
        return result
    
    def get_formula_examples(self) -> Dict[str, List[Dict[str, str]]]:
        """Get examples of formulas for different categories."""
        return {
            'mathematical': [
                {'name': 'Addition', 'formula': '[column1] + [column2]', 'description': 'Add two columns'},
                {'name': 'Percentage', 'formula': '([part] / [total]) * 100', 'description': 'Calculate percentage'},
                {'name': 'Square Root', 'formula': 'SQRT([number])', 'description': 'Square root of a number'},
                {'name': 'Absolute Value', 'formula': 'ABS([value])', 'description': 'Absolute value'},
                {'name': 'Round', 'formula': 'ROUND([value], 2)', 'description': 'Round to 2 decimal places'},
            ],
            'logical': [
                {'name': 'Conditional', 'formula': '[score] > 80 and "Pass" or "Fail"', 'description': 'Pass/Fail based on score'},
                {'name': 'Category', 'formula': '[age] < 18 and "Minor" or ([age] < 65 and "Adult" or "Senior")', 'description': 'Age categories'},
                {'name': 'Boolean', 'formula': '[quantity] > 0', 'description': 'True if quantity is positive'},
            ],
            'text': [
                {'name': 'Concatenation', 'formula': 'str([first_name]) + " " + str([last_name])', 'description': 'Combine first and last name'},
                {'name': 'Upper Case', 'formula': 'str([text]).upper()', 'description': 'Convert to uppercase'},
                {'name': 'Length', 'formula': 'len(str([text]))', 'description': 'Length of text'},
            ],
            'date_time': [
                {'name': 'Days Difference', 'formula': '(datetime.now() - datetime.strptime(str([date_column]), "%Y-%m-%d")).days', 'description': 'Days since date'},
                {'name': 'Age Calculation', 'formula': '(datetime.now().date() - datetime.strptime(str([birth_date]), "%Y-%m-%d").date()).days // 365', 'description': 'Calculate age in years'},
                {'name': 'Current Date', 'formula': 'datetime.now().strftime("%Y-%m-%d")', 'description': 'Current date'},
            ],
            'statistical': [
                {'name': 'Running Total', 'formula': '[current_value] + ([previous_total] or 0)', 'description': 'Running sum'},
                {'name': 'Difference from Average', 'formula': '[value] - [average_value]', 'description': 'Deviation from average'},
                {'name': 'Rank Score', 'formula': '([value] - [min_value]) / ([max_value] - [min_value]) * 100', 'description': 'Normalize to 0-100 scale'},
            ]
        }
    
    def get_available_functions(self) -> Dict[str, List[Dict[str, str]]]:
        """Get list of available functions categorized."""
        return {
            'mathematical': [
                {'name': 'ABS', 'syntax': 'ABS(number)', 'description': 'Absolute value'},
                {'name': 'ROUND', 'syntax': 'ROUND(number, decimals)', 'description': 'Round to specified decimal places'},
                {'name': 'CEIL', 'syntax': 'CEIL(number)', 'description': 'Round up to nearest integer'},
                {'name': 'FLOOR', 'syntax': 'FLOOR(number)', 'description': 'Round down to nearest integer'},
                {'name': 'SQRT', 'syntax': 'SQRT(number)', 'description': 'Square root'},
                {'name': 'LOG', 'syntax': 'LOG(number)', 'description': 'Natural logarithm'},
                {'name': 'MAX', 'syntax': 'max(a, b, c)', 'description': 'Maximum value'},
                {'name': 'MIN', 'syntax': 'min(a, b, c)', 'description': 'Minimum value'},
            ],
            'logical': [
                {'name': 'IF', 'syntax': 'condition and value_if_true or value_if_false', 'description': 'Conditional logic'},
                {'name': 'AND', 'syntax': 'condition1 and condition2', 'description': 'Logical AND'},
                {'name': 'OR', 'syntax': 'condition1 or condition2', 'description': 'Logical OR'},
                {'name': 'NOT', 'syntax': 'not condition', 'description': 'Logical NOT'},
            ],
            'text': [
                {'name': 'str', 'syntax': 'str(value)', 'description': 'Convert to string'},
                {'name': 'len', 'syntax': 'len(text)', 'description': 'Length of text'},
                {'name': 'upper', 'syntax': 'str(text).upper()', 'description': 'Convert to uppercase'},
                {'name': 'lower', 'syntax': 'str(text).lower()', 'description': 'Convert to lowercase'},
            ],
            'date_time': [
                {'name': 'datetime.now', 'syntax': 'datetime.now()', 'description': 'Current date and time'},
                {'name': 'strptime', 'syntax': 'datetime.strptime(date_string, format)', 'description': 'Parse date string'},
                {'name': 'strftime', 'syntax': 'date.strftime(format)', 'description': 'Format date to string'},
            ]
        }

# Global instance
calculated_fields_service = CalculatedFieldsService()
