from typing import List, Dict, Any, Optional, Tuple
from sqlalchemy import Table, text, inspect, desc, asc
from sqlalchemy.exc import SQLAlchemyError
from models import db
from models.base_models import TableMetadata
import math

class DatabaseService:
    """Service class for database operations on dynamically created tables."""
    
    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get all user-created tables with metadata."""
        try:
            # Get tables from metadata
            metadata_tables = TableMetadata.query.order_by(desc(TableMetadata.created_date)).all()
            result = [table.to_dict() for table in metadata_tables]
            
            # Enhance with column names for each table
            for table_data in result:
                table_name = table_data['table_name']
                try:
                    inspector = inspect(db.engine)
                    if table_name in inspector.get_table_names():
                        columns = inspector.get_columns(table_name)
                        table_data['columns'] = [col['name'] for col in columns]
                    else:
                        table_data['columns'] = []
                except:
                    table_data['columns'] = []
            
            # Use direct SQLite connection as fallback to find orphaned tables
            import sqlite3
            import os
            
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'instance', 'Stock.db')
            
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                all_db_tables = [row[0] for row in cursor.fetchall()]
                
                # Filter out system tables
                system_tables = {'table_metadata', 'upload_history', 'sqlite_sequence'}
                user_tables = [t for t in all_db_tables if t not in system_tables]
                
                # Find tables in DB but not in metadata
                metadata_table_names = {table.table_name for table in metadata_tables}
                orphaned_tables = [t for t in user_tables if t not in metadata_table_names]
                
                # Add orphaned tables with basic info
                for table_name in orphaned_tables:
                    try:
                        # Get row count
                        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                        row_count = cursor.fetchone()[0]
                        
                        # Get column info
                        cursor.execute(f"PRAGMA table_info({table_name})")
                        columns_info = cursor.fetchall()
                        column_names = [col[1] for col in columns_info]  # col[1] is the column name
                        
                        result.append({
                            'id': None,
                            'table_name': table_name,
                            'name': table_name,  # alias for table_name
                            'original_sheet_name': 'Unknown',
                            'original_filename': 'Unknown',
                            'column_count': len(columns_info),
                            'row_count': row_count,
                            'columns': column_names,
                            'created_date': None,
                            'updated_date': None,
                            'is_orphaned': True  # Flag to indicate missing metadata
                        })
                    except Exception as e:
                        pass  # Skip tables with errors
                
                conn.close()
            
            return result
            
        except Exception as e:
            # Return empty list instead of raising exception
            return []
    
    def repair_missing_metadata(self, table_name: str) -> bool:
        """Create metadata record for table that exists but has no metadata."""
        try:
            # Check if table exists
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return False
            
            # Check if metadata already exists
            existing_metadata = TableMetadata.query.filter_by(table_name=table_name).first()
            if existing_metadata:
                return True  # Already has metadata
            
            # Get table info
            columns = inspector.get_columns(table_name)
            count_result = db.session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = count_result.scalar()
            
            # Create metadata record
            from datetime import datetime
            metadata_record = TableMetadata(
                table_name=table_name,
                original_sheet_name='Unknown (Repaired)',
                original_filename='Unknown (Repaired)',
                column_count=len(columns),
                row_count=row_count,
                created_date=datetime.now()
            )
            
            db.session.add(metadata_record)
            db.session.commit()
            
            return True
            
        except Exception as e:
            db.session.rollback()
            print(f"Error repairing metadata for {table_name}: {e}")
            return False
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a specific table."""
        table_metadata = TableMetadata.query.filter_by(table_name=table_name).first()
        if not table_metadata:
            return None
        
        # Get actual table structure from database
        inspector = inspect(db.engine)
        if table_name not in inspector.get_table_names():
            return None
        
        columns = inspector.get_columns(table_name)
        
        # Get actual row count
        result = db.session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        actual_row_count = result.scalar()
        
        return {
            **table_metadata.to_dict(),
            'columns': columns,
            'actual_row_count': actual_row_count
        }
    
    def get_table_data(self, table_name: str, page: int = 1, per_page: int = 50, 
                      search: str = None, sort_by: str = None, 
                      sort_order: str = 'asc') -> Dict[str, Any]:
        """
        Get paginated data from a table with optional search and sorting.
        """
        try:
            # Validate table exists
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                raise ValueError(f"Table '{table_name}' does not exist")
            
            # Get table columns
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            
            # Build base query
            query = f"SELECT * FROM {table_name}"
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            
            # Add search filter if provided
            where_conditions = []
            if search and search.strip():
                search_conditions = []
                for col in column_names:
                    search_conditions.append(f"CAST({col} AS TEXT) LIKE :search")
                where_conditions.append(f"({' OR '.join(search_conditions)})")
                search_param = f"%{search}%"
            
            # Add WHERE clause if needed
            if where_conditions:
                where_clause = " WHERE " + " AND ".join(where_conditions)
                query += where_clause
                count_query += where_clause
            
            # Add sorting
            if sort_by and sort_by in column_names:
                order_direction = "DESC" if sort_order.lower() == 'desc' else "ASC"
                query += f" ORDER BY {sort_by} {order_direction}"
            else:
                query += " ORDER BY id ASC"
            
            # Add pagination
            offset = (page - 1) * per_page
            query += f" LIMIT {per_page} OFFSET {offset}"
            
            # Execute queries
            if search and search.strip():
                total_result = db.session.execute(text(count_query), {'search': search_param})
                data_result = db.session.execute(text(query), {'search': search_param})
            else:
                total_result = db.session.execute(text(count_query))
                data_result = db.session.execute(text(query))
            
            total_records = total_result.scalar()
            total_pages = math.ceil(total_records / per_page)
            
            # Convert results to list of dictionaries
            rows = []
            for row in data_result:
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[column_names[i]] = value
                rows.append(row_dict)
            
            return {
                'data': rows,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_records': total_records,
                    'total_pages': total_pages,
                    'has_prev': page > 1,
                    'has_next': page < total_pages
                },
                'columns': column_names,
                'search': search,
                'sort_by': sort_by,
                'sort_order': sort_order
            }
            
        except Exception as e:
            raise SQLAlchemyError(f"Error fetching table data: {str(e)}")
    
    def get_record(self, table_name: str, record_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific record by ID."""
        try:
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return None
            
            query = f"SELECT * FROM {table_name} WHERE id = :id"
            result = db.session.execute(text(query), {'id': record_id})
            row = result.fetchone()
            
            if not row:
                return None
            
            columns = inspector.get_columns(table_name)
            column_names = [col['name'] for col in columns]
            
            record = {}
            for i, value in enumerate(row):
                record[column_names[i]] = value
            
            return record
            
        except Exception as e:
            raise SQLAlchemyError(f"Error fetching record: {str(e)}")
    
    def update_record(self, table_name: str, record_id: int, data: Dict[str, Any]) -> bool:
        """Update a record in the table."""
        try:
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return False
            
            # Build update query
            set_clauses = []
            params = {'id': record_id}
            
            for key, value in data.items():
                if key != 'id':  # Don't update ID
                    set_clauses.append(f"{key} = :{key}")
                    params[key] = value
            
            if not set_clauses:
                return False
            
            query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = :id"
            result = db.session.execute(text(query), params)
            db.session.commit()
            
            return result.rowcount > 0
            
        except Exception as e:
            db.session.rollback()
            raise SQLAlchemyError(f"Error updating record: {str(e)}")
    
    def delete_record(self, table_name: str, record_id: int) -> bool:
        """Delete a record from the table."""
        try:
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return False
            
            query = f"DELETE FROM {table_name} WHERE id = :id"
            result = db.session.execute(text(query), {'id': record_id})
            db.session.commit()
            
            return result.rowcount > 0
            
        except Exception as e:
            db.session.rollback()
            raise SQLAlchemyError(f"Error deleting record: {str(e)}")
    
    def delete_table(self, table_name: str) -> bool:
        """Delete a table and its metadata."""
        try:
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return False
            
            # Delete table metadata
            table_metadata = TableMetadata.query.filter_by(table_name=table_name).first()
            if table_metadata:
                db.session.delete(table_metadata)
            
            # Drop the actual table
            db.session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            db.session.commit()
            
            return True
            
        except Exception as e:
            db.session.rollback()
            raise SQLAlchemyError(f"Error deleting table: {str(e)}")
    
    def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """Get statistics for a specific column."""
        try:
            inspector = inspect(db.engine)
            if table_name not in inspector.get_table_names():
                return {}
            
            # Get basic stats
            stats_query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT({column_name}) as non_null_count,
                COUNT(DISTINCT {column_name}) as unique_count
            FROM {table_name}
            """
            
            result = db.session.execute(text(stats_query))
            row = result.fetchone()
            
            stats = {
                'total_count': row[0],
                'non_null_count': row[1],
                'unique_count': row[2],
                'null_count': row[0] - row[1],
                'null_percentage': ((row[0] - row[1]) / row[0] * 100) if row[0] > 0 else 0
            }
            
            return stats
            
        except Exception as e:
            return {'error': str(e)}


# Standalone utility functions for API endpoints
def get_table_data(table_name: str, limit: int = 100, offset: int = 0) -> List[Dict]:
    """Get data from a table with limit and offset."""
    try:
        from sqlalchemy import text, inspect
        from app import db
        
        # Validate table exists
        inspector = inspect(db.engine)
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Build and execute query
        query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
        result = db.session.execute(text(query))
        
        # Convert to list of dictionaries
        columns = [col['name'] for col in inspector.get_columns(table_name)]
        data = []
        for row in result:
            data.append(dict(zip(columns, row)))
        
        return data
        
    except Exception as e:
        raise Exception(f"Error getting table data: {str(e)}")


def get_table_columns(table_name: str) -> List[Dict]:
    """Get column information for a table."""
    try:
        from sqlalchemy import inspect
        from app import db
        
        inspector = inspect(db.engine)
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table '{table_name}' does not exist")
        
        return inspector.get_columns(table_name)
        
    except Exception as e:
        raise Exception(f"Error getting table columns: {str(e)}")


def get_table_row_count(table_name: str) -> int:
    """Get total row count for a table."""
    try:
        from sqlalchemy import text, inspect
        from app import db
        
        # Validate table exists
        inspector = inspect(db.engine)
        if table_name not in inspector.get_table_names():
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Get row count
        result = db.session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        return result.scalar()
        
    except Exception as e:
        raise Exception(f"Error getting table row count: {str(e)}")
