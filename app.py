from flask import Flask, request, render_template, redirect, url_for, flash, jsonify, send_from_directory
import os
import json
import sqlite3
import gc  # For garbage collection with large files
from datetime import datetime
from config import config
from models import db
from models.base_models import UploadHistory, TableMetadata
from services.excel_processor import ExcelProcessor
from services.enhanced_excel_processor import EnhancedExcelProcessor
from services.universal_file_processor import UniversalFileProcessor
from services.currency_service import currency_service
from services.database_service import DatabaseService
from services.relationship_service import RelationshipService
from services.calculated_fields_service import calculated_fields_service

# Try to import maintenance services, but continue if they fail
try:
    from services.maintenance_service import MaintenanceService
    from services.work_order_service import WorkOrderService
    from services.spare_parts_service import SparePartsService
    from services.stock_analysis_service import StockAnalysisService
    MAINTENANCE_SERVICES_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Maintenance services not available: {e}")
    MAINTENANCE_SERVICES_AVAILABLE = False

from werkzeug.utils import secure_filename
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text
import logging
import tempfile
from threading import Thread
import uuid
import time

# Global progress tracking dictionary
progress_store = {}

def track_progress(operation_id, progress_data):
    """Store progress updates for a specific operation"""
    progress_store[operation_id] = {
        'timestamp': time.time(),
        'data': progress_data
    }
    # Clean up old progress data (older than 1 hour)
    current_time = time.time()
    to_remove = [key for key, value in progress_store.items() 
                 if current_time - value['timestamp'] > 3600]
    for key in to_remove:
        del progress_store[key]

def create_app(config_name='default'):
    """Create and configure the Flask application."""
    app = Flask(__name__)
    
    # Load configuration
    app.config.from_object(config[config_name])
    
    # Explicitly set max upload size (for large files up to 512MB)
    app.config['MAX_CONTENT_LENGTH'] = 512 * 1024 * 1024  # 512MB
    
    # Initialize database
    db.init_app(app)
    
    # Create upload directory
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    
    # Initialize services
    excel_processor = ExcelProcessor(app.config['UPLOAD_FOLDER'])
    enhanced_excel_processor = EnhancedExcelProcessor(app.config['UPLOAD_FOLDER'])
    universal_file_processor = UniversalFileProcessor(app.config['UPLOAD_FOLDER'])
    db_service = DatabaseService()
    relationship_service = RelationshipService()
    
    # Initialize maintenance-specific services if available
    if MAINTENANCE_SERVICES_AVAILABLE:
        try:
            maintenance_service = MaintenanceService()
            work_order_service = WorkOrderService()
            spare_parts_service = SparePartsService()
            stock_analysis_service = StockAnalysisService()
        except Exception as e:
            print(f"Warning: Could not initialize maintenance services: {e}")
            maintenance_service = None
            work_order_service = None
            spare_parts_service = None
            stock_analysis_service = None
    else:
        maintenance_service = None
        work_order_service = None
        spare_parts_service = None
        stock_analysis_service = None
    
    # Import and initialize database management service
    from services.db_management_service import DatabaseManagementService
    db_management_service = DatabaseManagementService()
    
    def get_current_database_name():
        """Get the current database name from the configuration."""
        try:
            current_db_uri = app.config.get('SQLALCHEMY_DATABASE_URI', '')
            if 'sqlite:///' in current_db_uri:
                db_path = current_db_uri.replace('sqlite:///', '')
                return os.path.basename(db_path).replace('.db', '')
            return 'Unknown'
        except Exception:
            return 'Unknown'
    
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Create database tables during app initialization with retry logic
    with app.app_context():
        # Configure SQLite pragmas for better concurrency within app context
        if 'sqlite' in app.config['SQLALCHEMY_DATABASE_URI']:
            from sqlalchemy import event
            import sqlite3
            
            @event.listens_for(db.engine, "connect")
            def set_sqlite_pragma(dbapi_connection, connection_record):
                """Set SQLite pragmas for better concurrency and performance."""
                if isinstance(dbapi_connection, sqlite3.Connection):
                    cursor = dbapi_connection.cursor()
                    try:
                        # Enable WAL mode for better concurrency
                        cursor.execute("PRAGMA journal_mode=WAL")
                        
                        # Set busy timeout (60 seconds)
                        cursor.execute("PRAGMA busy_timeout=60000")
                        
                        # Set synchronous mode to NORMAL for better performance
                        cursor.execute("PRAGMA synchronous=NORMAL")
                        
                        # Use memory for temporary storage
                        cursor.execute("PRAGMA temp_store=MEMORY")
                        
                        # Increase cache size (10MB)
                        cursor.execute("PRAGMA cache_size=-10000")
                        
                        # Enable foreign keys
                        cursor.execute("PRAGMA foreign_keys=ON")
                        
                    except Exception as e:
                        print(f"Warning: Could not set SQLite pragmas: {e}")
                    finally:
                        cursor.close()
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                db.create_all()
                break  # Success, exit retry loop
            except Exception as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    import time
                    time.sleep(1 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(f"Failed to create database tables: {e}")
                    # Try to continue anyway, tables might already exist
                    break
    
    # Routes
    @app.route('/api/table-info/<table_name>')
    def api_get_table_info(table_name):
        """Get detailed table information."""
        try:
            # Get basic table info
            table_info = db_service.get_table_info(table_name)
            if not table_info:
                return jsonify({'success': False, 'error': f'Table "{table_name}" not found'})
            
            # Get sample values for each column
            from sqlalchemy import inspect, text
            inspector = inspect(db.engine)
            columns = inspector.get_columns(table_name)
            
            enhanced_columns = []
            for col in columns:
                if col['name'] != 'id':  # Skip ID column
                    try:
                        # Get sample values for this column
                        result = db.session.execute(
                            text(f"SELECT DISTINCT {col['name']} FROM {table_name} WHERE {col['name']} IS NOT NULL LIMIT 5")
                        )
                        sample_values = [str(row[0]) for row in result.fetchall()]
                        
                        enhanced_columns.append({
                            'name': col['name'],
                            'type': str(col['type']),
                            'sample_values': sample_values
                        })
                    except:
                        enhanced_columns.append({
                            'name': col['name'],
                            'type': str(col['type']),
                            'sample_values': []
                        })
            
            table_info['columns'] = enhanced_columns
            
            return jsonify({
                'success': True,
                'table_info': table_info
            })
        except Exception as e:
            logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table-preview/<table_name>')
    def api_get_table_preview(table_name):
        """Get preview data from a table."""
        try:
            # Get table info
            table_info = db_service.get_table_info(table_name)
            if not table_info:
                return jsonify({'success': False, 'error': f'Table "{table_name}" not found'})
            
            # Get preview data
            data = db_service.get_table_data(table_name, page=1, per_page=5)
            
            return jsonify({
                'success': True,
                'columns': data['columns'],
                'rows': data['data'],
                'total_rows': data['total']
            })
        except Exception as e:
            logger.error(f"Error getting table preview for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/repair-metadata/<table_name>', methods=['POST'])
    def api_repair_metadata(table_name):
        """Repair missing metadata for a specific table."""
        try:
            success = db_service.repair_missing_metadata(table_name)
            if success:
                return jsonify({'success': True, 'message': f'Metadata repaired for table "{table_name}"'})
            else:
                return jsonify({'success': False, 'error': f'Could not repair metadata for table "{table_name}"'})
        except Exception as e:
            logger.error(f"Error repairing metadata for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/repair-all-metadata', methods=['POST'])
    def api_repair_all_metadata():
        """Repair missing metadata for all orphaned tables."""
        try:
            all_tables = db_service.get_all_tables()
            orphaned_tables = [t for t in all_tables if t.get('is_orphaned')]
            
            repaired_count = 0
            for table in orphaned_tables:
                try:
                    if db_service.repair_missing_metadata(table['table_name']):
                        repaired_count += 1
                except:
                    continue
            
            return jsonify({
                'success': True, 
                'message': f'Repaired metadata for {repaired_count} table(s)',
                'repaired_count': repaired_count
            })
        except Exception as e:
            logger.error(f"Error repairing all metadata: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/')
    def index():
        """LCT STS Data Monitoring App - Home page with monitoring overview and database content."""
        try:
            # Get recent tables from metadata
            recent_tables = TableMetadata.query.order_by(
                TableMetadata.created_date.desc()
            ).limit(5).all()
            
            # DEBUG: Log the retrieved data
            logger.info(f"DEBUG: Retrieved {len(recent_tables)} recent tables:")
            for table in recent_tables:
                logger.info(f"  - ID: {table.id}, table_name: {repr(table.table_name)}, type: {type(table.table_name)}")
            
            # Get all existing tables using the enhanced database service
            all_existing_tables = db_service.get_all_tables()
            
            # Calculate dashboard statistics with monitoring focus
            stats = {
                'total_tables': len(all_existing_tables),
                'total_records': sum(table.get('row_count', 0) for table in all_existing_tables),
                'total_columns': sum(table.get('column_count', 0) for table in all_existing_tables),
                'orphaned_tables': len([t for t in all_existing_tables if t.get('is_orphaned')]),
                'recent_uploads': len(recent_tables),
                'active_work_orders': 0,  # Will be calculated below
                'total_stock_items': 0    # Will be calculated below
            }
            
            # Add data monitoring specific stats
            try:
                # Work Orders - Check multiple possible tables and databases
                stats['total_work_orders'] = 0
                stats['active_work_orders'] = 0
                
                # Try work_orders table in current database first
                try:
                    wo_result = db.session.execute(text("SELECT COUNT(*) FROM work_orders"))
                    stats['total_work_orders'] = wo_result.scalar() or 0
                except:
                    # If work_orders table doesn't exist or is empty, try alternate work order tables
                    try:
                        # Try wo_active table (active work orders)
                        wo_active_result = db.session.execute(text("SELECT COUNT(*) FROM wo_active"))
                        wo_active_count = wo_active_result.scalar() or 0
                        stats['active_work_orders'] = wo_active_count
                        
                        # Try wo_history table (historical work orders)
                        wo_history_result = db.session.execute(text("SELECT COUNT(*) FROM wo_history"))
                        wo_history_count = wo_history_result.scalar() or 0
                        
                        stats['total_work_orders'] = wo_active_count + wo_history_count
                    except:
                        # Try connecting to Workorder.db directly for work order data
                        try:
                            import sqlite3
                            import os
                            wo_db_path = os.path.join(app.instance_path, 'Workorder.db')
                            if os.path.exists(wo_db_path):
                                wo_conn = sqlite3.connect(wo_db_path)
                                wo_cursor = wo_conn.cursor()
                                
                                # Get counts from work order database
                                wo_active_count = 0
                                wo_history_count = 0
                                
                                try:
                                    wo_cursor.execute("SELECT COUNT(*) FROM wo_active")
                                    wo_active_count = wo_cursor.fetchone()[0]
                                    stats['active_work_orders'] = wo_active_count
                                except:
                                    pass
                                    
                                try:
                                    wo_cursor.execute("SELECT COUNT(*) FROM wo_history")
                                    wo_history_count = wo_cursor.fetchone()[0]
                                except:
                                    pass
                                
                                stats['total_work_orders'] = wo_active_count + wo_history_count
                                wo_conn.close()
                        except:
                            stats['total_work_orders'] = 0
                            stats['active_work_orders'] = 0
                
                # Stock and Critical Spare Parts
                try:
                    # Total stock items
                    stock_result = db.session.execute(text("SELECT COUNT(*) FROM Stock"))
                    stats['total_stock_items'] = stock_result.scalar() or 0
                    
                    # Critical Spare Parts (for monitoring stock levels)
                    critical_parts_result = db.session.execute(text("""
                        SELECT COUNT(*) FROM Stock 
                        WHERE quantite_en_stock <= seuil_de_reappro_min 
                        AND seuil_de_reappro_min > 0
                        AND seuil_de_reappro_min IS NOT NULL
                        AND quantite_en_stock IS NOT NULL
                    """))
                    stats['critical_spare_parts'] = critical_parts_result.scalar() or 0
                except Exception as e:
                    logger.warning(f"Could not calculate stock statistics: {e}")
                    stats['critical_spare_parts'] = 0
                    stats['total_stock_items'] = 0
                    
            except Exception as e:
                logger.error(f"Error calculating statistics: {e}")
                stats['total_work_orders'] = 0
                stats['active_work_orders'] = 0
                stats['critical_spare_parts'] = 0
                stats['total_stock_items'] = 0
            
            # Get recent upload history
            recent_uploads = UploadHistory.query.order_by(
                UploadHistory.upload_date.desc()
            ).limit(5).all()
            
            return render_template('index.html', 
                                 recent_tables=recent_tables,
                                 existing_tables=all_existing_tables,
                                 stats=stats,
                                 recent_uploads=recent_uploads,
                                 app_title="LCT STS Data Monitoring App")
        except Exception as e:
            logger.error(f"Error loading home page: {str(e)}")
            flash('Error loading dashboard', 'error')
            return render_template('index.html', 
                                 recent_tables=[], 
                                 existing_tables=[],
                                 stats={'total_tables': 0, 'total_records': 0, 'total_columns': 0, 'orphaned_tables': 0, 'recent_uploads': 0, 'total_work_orders': 0, 'active_work_orders': 0, 'critical_spare_parts': 0, 'total_stock_items': 0},
                                 recent_uploads=[],
                                 app_title="LCT STS Data Monitoring App")

    @app.route('/monitoring-dashboard')
    def monitoring_dashboard():
        """Dedicated data monitoring dashboard."""
        try:
            # Get all existing tables
            all_existing_tables = db_service.get_all_tables()
            
            # Calculate total records
            total_records = sum(table.get('row_count', 0) for table in all_existing_tables)
            
            # Get last update time
            last_update = None
            recent_uploads = UploadHistory.query.order_by(
                UploadHistory.upload_date.desc()
            ).limit(1).all()
            
            if recent_uploads:
                last_update = recent_uploads[0].upload_date
            
            # Calculate data health (non-orphaned tables percentage)
            orphaned_count = len([t for t in all_existing_tables if t.get('is_orphaned')])
            data_health = 100
            if len(all_existing_tables) > 0:
                data_health = round(((len(all_existing_tables) - orphaned_count) / len(all_existing_tables)) * 100, 1)
            
            # Get recent uploads for activity
            all_recent_uploads = UploadHistory.query.order_by(
                UploadHistory.upload_date.desc()
            ).limit(10).all()
            
            return render_template('monitoring_dashboard.html',
                                 tables=all_existing_tables,
                                 total_records=total_records,
                                 last_update=last_update,
                                 data_health=data_health,
                                 recent_uploads=all_recent_uploads)
                                 
        except Exception as e:
            logger.error(f"Error loading monitoring dashboard: {str(e)}")
            return render_template('monitoring_dashboard.html',
                                 tables=[],
                                 total_records=0,
                                 last_update=None,
                                 data_health=100,
                                 recent_uploads=[],
                                 error=str(e))
    
    @app.route('/upload', methods=['GET', 'POST'])
    def upload():
        """Upload various file types (Excel, CSV, TXT, PDF) page."""
        if request.method == 'GET':
            # Get recent uploads for display with proper session management
            try:
                recent_uploads = UploadHistory.query.order_by(
                    UploadHistory.upload_date.desc()
                ).limit(10).all()
            except Exception as e:
                logger.warning(f"Could not fetch recent uploads: {e}")
                recent_uploads = []
            finally:
                # Ensure session is properly closed
                db.session.close()
            
            return render_template('upload.html', recent_uploads=recent_uploads)
        
        # Handle POST request
        if 'file' not in request.files:
            if request.is_json:
                return jsonify({'success': False, 'error': 'No file selected'})
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        
        if file.filename == '':
            if request.is_json:
                return jsonify({'success': False, 'error': 'No file selected'})
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if not universal_file_processor.allowed_file(file.filename):
            supported_types = "Excel (.xlsx, .xls, .xlsm, .xlsb), CSV (.csv), Text (.txt, .tsv), PDF (.pdf)"
            error_message = f'Invalid file type. Supported formats: {supported_types}'
            if request.is_json:
                return jsonify({'success': False, 'error': error_message})
            flash(error_message, 'error')
            return redirect(request.url)
        
        # Check file size before processing (helpful error message)
        file.seek(0, 2)  # Seek to end of file
        file_size = file.tell()  # Get current position (file size)
        file.seek(0)  # Reset to beginning
        
        max_size = app.config.get('MAX_CONTENT_LENGTH', 512 * 1024 * 1024)
        if file_size > max_size:
            size_mb = file_size / (1024 * 1024)
            max_mb = max_size / (1024 * 1024)
            error_message = f'File too large ({size_mb:.1f}MB). Maximum allowed size is {max_mb:.0f}MB.'
            if request.is_json:
                return jsonify({'success': False, 'error': error_message})
            flash(error_message, 'error')
            return redirect(request.url)
        
        try:
            # Force garbage collection before processing large files
            gc.collect()
            
            # Process the file using universal processor
            upload_id, created_tables = universal_file_processor.process_file(file)
            
            file_type = universal_file_processor.get_file_type(file.filename)
            message = f"Successfully processed {file_type.upper()} file. Created {len(created_tables)} table(s): {', '.join(created_tables)}"
            
            if request.is_json:
                return jsonify({
                    'success': True, 
                    'message': message,
                    'upload_id': upload_id,
                    'tables': created_tables,
                    'file_type': file_type,
                    'redirect': url_for('tables')
                })
            
            flash(message, 'success')
            return redirect(url_for('tables'))
            
        except Exception as e:
            logger.error(f"Error processing file: {str(e)}")
            
            if request.is_json:
                return jsonify({'success': False, 'error': str(e)})
            
            flash(f'Error processing file: {str(e)}', 'error')
            return redirect(request.url)
    
    @app.route('/tables')
    def tables():
        """List all tables page with enhanced filtering and search."""
        try:
            # Get query parameters
            search = request.args.get('search', '').strip()
            filter_type = request.args.get('filter', 'all')  # all, orphaned, recent
            sort_by = request.args.get('sort_by', 'created_date')
            sort_order = request.args.get('sort_order', 'desc')
            
            # Get all tables from database service
            all_tables = db_service.get_all_tables()
            
            # Apply filters
            filtered_tables = all_tables
            
            # Search filter
            if search:
                filtered_tables = [
                    table for table in filtered_tables
                    if search.lower() in table.get('table_name', '').lower() or
                       search.lower() in table.get('original_sheet_name', '').lower() or
                       search.lower() in table.get('original_filename', '').lower()
                ]
            
            # Type filter
            if filter_type == 'orphaned':
                filtered_tables = [table for table in filtered_tables if table.get('is_orphaned')]
            elif filter_type == 'recent':
                # Tables created in last 7 days
                from datetime import datetime, timedelta
                week_ago = datetime.utcnow() - timedelta(days=7)
                filtered_tables = [
                    table for table in filtered_tables 
                    if table.get('created_date') and table['created_date'] > week_ago
                ]
            
            # Sort tables
            def get_sort_key(table):
                value = table.get(sort_by)
                if value is None:
                    return 0 if sort_by in ['row_count', 'column_count'] else ''
                # Ensure consistent types for sorting
                if sort_by in ['row_count', 'column_count']:
                    return int(value) if isinstance(value, (int, str)) and str(value).isdigit() else 0
                elif sort_by == 'created_date':
                    return value if value else datetime.min
                else:
                    return str(value) if value else ''
            
            filtered_tables.sort(
                key=get_sort_key,
                reverse=(sort_order == 'desc')
            )
            
            # Calculate statistics for filtered results
            stats = {
                'total_shown': len(filtered_tables),
                'total_records': sum(table.get('row_count', 0) for table in filtered_tables),
                'orphaned_count': len([t for t in filtered_tables if t.get('is_orphaned')]),
                'search_term': search,
                'filter_type': filter_type
            }
            
            return render_template('tables.html', 
                                 tables=filtered_tables,
                                 stats=stats,
                                 search=search,
                                 filter_type=filter_type,
                                 sort_by=sort_by,
                                 sort_order=sort_order,
                                 current_db_name=get_current_database_name())
        except Exception as e:
            logger.error(f"Error loading tables: {str(e)}")
            flash('Error loading tables', 'error')
            return render_template('tables.html', 
                                 tables=[], 
                                 stats={'total_shown': 0, 'total_records': 0, 'orphaned_count': 0},
                                 search='',
                                 filter_type='all',
                                 sort_by='created_date',
                                 sort_order='desc',
                                 current_db_name=get_current_database_name())
    
    @app.route('/table/<table_name>')
    def view_table(table_name):
        """View table data page."""
        try:
            # Get table info
            table_info = db_service.get_table_info(table_name)
            if not table_info:
                flash(f'Table "{table_name}" not found', 'error')
                return redirect(url_for('tables'))
            
            # Get request parameters
            page = request.args.get('page', 1, type=int)
            search = request.args.get('search', '')
            sort_by = request.args.get('sort_by', '')
            sort_order = request.args.get('sort_order', 'asc')
            per_page = request.args.get('per_page', 50, type=int)
            
            # Get table data
            data = db_service.get_table_data(
                table_name=table_name,
                page=page,
                per_page=per_page,
                search=search,
                sort_by=sort_by,
                sort_order=sort_order
            )
            
            return render_template('view_table.html', 
                                 table_name=table_name,
                                 table_info=table_info,
                                 data=data,
                                 current_db_name=get_current_database_name())
            
        except Exception as e:
            logger.error(f"Error viewing table {table_name}: {str(e)}")
            flash(f'Error loading table data: {str(e)}', 'error')
            return redirect(url_for('tables'))
    
    # ===== STOCK ANALYSIS ROUTES =====
    
    @app.route('/stock-analysis')
    def stock_analysis():
        """Comprehensive stock analysis page combining data from all tables."""
        if not stock_analysis_service:
            flash('Stock analysis service not available', 'error')
            return redirect(url_for('index'))
            
        try:
            # Get filter parameters
            limit = request.args.get('limit', 100, type=int)
            article_filter = request.args.get('article_filter', '')
            analysis_type = request.args.get('analysis_type', 'comprehensive')
            
            # Get basic data for the page load (detailed data will be loaded via AJAX)
            initial_data = stock_analysis_service.get_comprehensive_stock_analysis(limit=50)
            alerts = stock_analysis_service.get_stock_alerts()
            
            return render_template('stock_analysis.html',
                                 initial_data=initial_data,
                                 alerts=alerts,
                                 limit=limit,
                                 article_filter=article_filter,
                                 analysis_type=analysis_type,
                                 current_db_name=get_current_database_name())
                                 
        except Exception as e:
            logger.error(f"Error in stock analysis page: {str(e)}")
            flash(f'Error loading stock analysis: {str(e)}', 'error')
            return redirect(url_for('index'))
    
    # ===== STOCK MANAGEMENT ROUTES =====
    
    @app.route('/spare-parts-inventory')
    def spare_parts_inventory():
        """Comprehensive spare parts inventory management page."""
        if not spare_parts_service:
            flash('Spare parts service not available', 'error')
            return redirect(url_for('index'))
            
        try:
            # Get filter parameters
            filter_type = request.args.get('filter', 'all')
            search_term = request.args.get('search', '')
            page = request.args.get('page', 1, type=int)
            per_page = 50
            
            # Get inventory data based on filters
            if filter_type == 'critical':
                spare_parts = spare_parts_service.get_critical_spare_parts(limit=500)
            elif filter_type == 'out_of_stock':
                spare_parts = spare_parts_service.get_out_of_stock_parts()
            elif search_term:
                spare_parts = spare_parts_service.search_spare_parts(search_term)
            else:
                spare_parts = spare_parts_service.get_spare_parts_inventory(limit=500)
            
            # Get statistics
            stats = spare_parts_service.get_spare_parts_statistics()
            
            # Get reorder suggestions
            reorder_suggestions = spare_parts_service.get_reorder_suggestions()
            
            # Current filters for template
            current_filters = {
                'filter': filter_type,
                'search': search_term
            }
            
            return render_template('spare_parts_inventory.html',
                                 spare_parts=spare_parts,
                                 stats=stats,
                                 reorder_suggestions=reorder_suggestions,
                                 current_filters=current_filters)
                                 
        except Exception as e:
            logger.error(f"Error loading spare parts inventory: {str(e)}")
            flash(f'Error loading inventory: {str(e)}', 'error')
            return render_template('spare_parts_inventory.html',
                                 spare_parts=[],
                                 stats={},
                                 reorder_suggestions=[],
                                 current_filters={'filter': 'all', 'search': ''})

    @app.route('/stock-inventory')
    def stock_inventory():
        """Stock Inventory page with search and filtering."""
        try:
            # Get filter parameters
            search_term = request.args.get('search', '')
            category = request.args.get('category', '')
            status = request.args.get('status', '')
            sort_by = request.args.get('sort', 'designation_1')
            page = request.args.get('page', 1, type=int)
            per_page = 50
            
            # Get stock data using database service
            query = """
                SELECT 
                    id,
                    reference_article,
                    designation_1,
                    designation_2,
                    categorie_article,
                    quantite_en_stock,
                    seuil_de_reappro_min,
                    pmp,
                    emplacement_de_l_article,
                    CASE 
                        WHEN quantite_en_stock = 0 THEN 'Out of Stock'
                        WHEN quantite_en_stock <= seuil_de_reappro_min THEN 'Low Stock'
                        ELSE 'In Stock'
                    END as stock_status
                FROM Stock 
                WHERE 1=1
            """
            params = {}
            
            # Apply filters
            if search_term:
                query += " AND (designation_1 LIKE :search1 OR reference_article LIKE :search2 OR designation_2 LIKE :search3)"
                search_pattern = f"%{search_term}%"
                params.update({"search1": search_pattern, "search2": search_pattern, "search3": search_pattern})
            
            if category:
                query += " AND categorie_article = :category"
                params["category"] = category
                
            if status == 'critical':
                query += " AND quantite_en_stock <= seuil_de_reappro_min"
            elif status == 'out_of_stock':
                query += " AND quantite_en_stock = 0"
            
            query += f" ORDER BY {sort_by} LIMIT :per_page OFFSET :offset"
            params.update({"per_page": per_page, "offset": (page - 1) * per_page})
            
            result = db.session.execute(text(query), params)
            stock_items = []
            for row in result.fetchall():
                stock_items.append({
                    'id': row[0],
                    'reference_article': row[1] or '',
                    'designation_1': row[2] or '',
                    'designation_2': row[3] or '',
                    'categorie_article': row[4] or '',
                    'quantite_en_stock': row[5] or 0,
                    'seuil_de_reappro_min': row[6] or 0,
                    'pmp': row[7] or 0.0,
                    'emplacement_de_l_article': row[8] or '',
                    'stock_status': row[9]
                })
            
            # Get categories for filter dropdown
            categories_result = db.session.execute(text("SELECT DISTINCT categorie_article FROM Stock WHERE categorie_article IS NOT NULL ORDER BY categorie_article"))
            categories = [row[0] for row in categories_result.fetchall()]
            
            # Get statistics
            stats_result = db.session.execute(text("""
                SELECT 
                    COUNT(*) as total_items,
                    SUM(CASE WHEN quantite_en_stock = 0 THEN 1 ELSE 0 END) as out_of_stock,
                    SUM(CASE WHEN quantite_en_stock <= seuil_de_reappro_min THEN 1 ELSE 0 END) as critical_stock,
                    SUM(quantite_en_stock * pmp) as total_value
                FROM Stock
            """))
            stats_row = stats_result.fetchone()
            stats = {
                'total_items': stats_row[0] or 0,
                'out_of_stock': stats_row[1] or 0,
                'critical_stock': stats_row[2] or 0,
                'total_value': stats_row[3] or 0.0
            }
            
            return render_template('stock_inventory.html',
                                 stock_items=stock_items,
                                 stats=stats,
                                 categories=categories,
                                 current_filters={
                                     'search': search_term,
                                     'category': category,
                                     'status': status,
                                     'sort': sort_by
                                 },
                                 page=page,
                                 per_page=per_page)
                                 
        except Exception as e:
            logger.error(f"Error loading stock inventory: {str(e)}")
            flash(f'Error loading stock inventory: {str(e)}', 'error')
            return render_template('stock_inventory.html',
                                 stock_items=[],
                                 stats={},
                                 categories=[],
                                 current_filters={},
                                 page=1,
                                 per_page=50)

    @app.route('/purchase-orders')
    def purchase_orders():
        """Purchase Orders page with search and filtering."""
        try:
            # Get filter parameters
            search_term = request.args.get('search', '')
            status = request.args.get('status', '')
            sort_by = request.args.get('sort', 'n_commande')
            page = request.args.get('page', 1, type=int)
            per_page = 50
            
            # Get PO data
            query = """
                SELECT 
                    id,
                    n_commande,
                    code_article,
                    designation,
                    qté_commandée,
                    prix_net,
                    montant_ht_ligne,
                    nom_founisseur,
                    commande_soldée,
                    date_commande,
                    date_livraison
                FROM PO 
                WHERE 1=1
            """
            params = {}
            
            # Apply filters
            if search_term:
                query += " AND (n_commande LIKE :search1 OR code_article LIKE :search2 OR designation LIKE :search3 OR nom_founisseur LIKE :search4)"
                search_pattern = f"%{search_term}%"
                params.update({"search1": search_pattern, "search2": search_pattern, "search3": search_pattern, "search4": search_pattern})
            
            if status:
                query += " AND commande_soldée = :status"
                params["status"] = status
            
            query += f" ORDER BY {sort_by} DESC LIMIT :per_page OFFSET :offset"
            params.update({"per_page": per_page, "offset": (page - 1) * per_page})
            
            result = db.session.execute(text(query), params)
            purchase_orders = []
            for row in result.fetchall():
                purchase_orders.append({
                    'id': row[0],
                    'n_commande': row[1] or '',
                    'code_article': row[2] or '',
                    'designation': row[3] or '',
                    'qte_commandee': row[4] or 0,
                    'prix_unitaire': row[5] or 0.0,
                    'montant_ligne': row[6] or 0.0,
                    'fournisseur': row[7] or '',
                    'statut': row[8] or '',
                    'date_commande': row[9] or '',
                    'date_prevue_livraison': row[10] or ''
                })
            
            # Get status options for filter
            status_result = db.session.execute(text("SELECT DISTINCT commande_soldée FROM PO WHERE commande_soldée IS NOT NULL ORDER BY commande_soldée"))
            statuses = [row[0] for row in status_result.fetchall()]
            
            # Get statistics
            stats_result = db.session.execute(text("""
                SELECT 
                    COUNT(*) as total_pos,
                    COUNT(DISTINCT n_commande) as unique_pos,
                    SUM(montant_ht_ligne) as total_value,
                    COUNT(DISTINCT nom_founisseur) as suppliers_count
                FROM PO
            """))
            stats_row = stats_result.fetchone()
            stats = {
                'total_pos': stats_row[0] or 0,
                'unique_pos': stats_row[1] or 0,
                'total_value': stats_row[2] or 0.0,
                'suppliers_count': stats_row[3] or 0
            }
            
            return render_template('purchase_orders.html',
                                 purchase_orders=purchase_orders,
                                 stats=stats,
                                 statuses=statuses,
                                 current_filters={
                                     'search': search_term,
                                     'status': status,
                                     'sort': sort_by
                                 },
                                 page=page,
                                 per_page=per_page)
                                 
        except Exception as e:
            logger.error(f"Error loading purchase orders: {str(e)}")
            flash(f'Error loading purchase orders: {str(e)}', 'error')
            return render_template('purchase_orders.html',
                                 purchase_orders=[],
                                 stats={},
                                 statuses=[],
                                 current_filters={},
                                 page=1,
                                 per_page=50)

    @app.route('/purchase-requests')
    def purchase_requests():
        """Purchase Requests page with search and filtering."""
        try:
            # Get filter parameters
            search_term = request.args.get('search', '')
            status = request.args.get('status', '')
            sort_by = request.args.get('sort', 'no_demande')
            page = request.args.get('page', 1, type=int)
            per_page = 50
            
            # Get PR data
            query = """
                SELECT 
                    id,
                    no_demande,
                    article,
                    designation,
                    quantita_ua,
                    prix_net,
                    montant_ligne_ht,
                    a_solder,
                    date_crã_ation,
                    demandeur
                FROM PR 
                WHERE 1=1
            """
            params = {}
            
            # Apply filters
            if search_term:
                query += " AND (no_demande LIKE :search1 OR article LIKE :search2 OR designation LIKE :search3 OR demandeur LIKE :search4)"
                search_pattern = f"%{search_term}%"
                params.update({"search1": search_pattern, "search2": search_pattern, "search3": search_pattern, "search4": search_pattern})
            
            if status:
                query += " AND a_solder = :status"
                params["status"] = status
            
            query += f" ORDER BY {sort_by} DESC LIMIT :per_page OFFSET :offset"
            params.update({"per_page": per_page, "offset": (page - 1) * per_page})
            
            result = db.session.execute(text(query), params)
            purchase_requests = []
            for row in result.fetchall():
                purchase_requests.append({
                    'id': row[0],
                    'no_demande': row[1] or '',
                    'article': row[2] or '',
                    'designation': row[3] or '',
                    'quantite': row[4] or 0,
                    'prix_unitaire': row[5] or 0.0,
                    'prix_total': row[6] or 0.0,
                    'statut': row[7] or '',
                    'date_demande': row[8] or '',
                    'demandeur': row[9] or '',
                    'justification': ''  # Empty since column doesn't exist
                })
            
            # Get status options for filter
            status_result = db.session.execute(text("SELECT DISTINCT a_solder FROM PR WHERE a_solder IS NOT NULL ORDER BY a_solder"))
            statuses = [row[0] for row in status_result.fetchall()]
            
            # Get statistics
            stats_result = db.session.execute(text("""
                SELECT 
                    COUNT(*) as total_prs,
                    COUNT(DISTINCT no_demande) as unique_prs,
                    SUM(montant_ligne_ht) as total_value,
                    COUNT(DISTINCT demandeur) as requesters_count
                FROM PR
            """))
            stats_row = stats_result.fetchone()
            stats = {
                'total_prs': stats_row[0] or 0,
                'unique_prs': stats_row[1] or 0,
                'total_value': stats_row[2] or 0.0,
                'requesters_count': stats_row[3] or 0
            }
            
            return render_template('purchase_requests.html',
                                 purchase_requests=purchase_requests,
                                 stats=stats,
                                 statuses=statuses,
                                 current_filters={
                                     'search': search_term,
                                     'status': status,
                                     'sort': sort_by
                                 },
                                 page=page,
                                 per_page=per_page)
                                 
        except Exception as e:
            logger.error(f"Error loading purchase requests: {str(e)}")
            flash(f'Error loading purchase requests: {str(e)}', 'error')
            return render_template('purchase_requests.html',
                                 purchase_requests=[],
                                 stats={},
                                 statuses=[],
                                 current_filters={},
                                 page=1,
                                 per_page=50)
    
    @app.route('/stock-movements')
    def stock_movements():
        """Stock movements and transaction history page."""
        if not spare_parts_service:
            flash('Spare parts service not available', 'error')
            return redirect(url_for('index'))
            
        try:
            # Get movements data
            movements = spare_parts_service.get_stock_movements()
            
            return render_template('stock_movements.html', movements=movements)
                                 
        except Exception as e:
            logger.error(f"Error loading stock movements: {str(e)}")
            flash(f'Error loading movements: {str(e)}', 'error')
            return render_template('stock_movements.html', movements=[])
    
    @app.route('/stock-analytics')
    def stock_analytics():
        """Advanced stock analytics and reporting page."""
        if not spare_parts_service:
            flash('Spare parts service not available', 'error')
            return redirect(url_for('index'))
            
        try:
            # Get analytics data
            analytics = spare_parts_service.get_stock_analytics()
            
            return render_template('stock_analytics.html', analytics=analytics)
                                 
        except Exception as e:
            logger.error(f"Error loading stock analytics: {str(e)}")
            flash(f'Error loading analytics: {str(e)}', 'error')
            return render_template('stock_analytics.html', analytics={})
    
    # ===== STOCK MANAGEMENT API ROUTES =====
    
    @app.route('/api/stock-inventory/<int:item_id>')
    def api_get_stock_item(item_id):
        """Get detailed information for a stock item."""
        try:
            result = db.session.execute(text("""
                SELECT 
                    id, reference_article, designation_1, designation_2, categorie_article,
                    quantite_en_stock, seuil_de_reappro_min, quantite_maximum_max,
                    pmp, unite_de_stock, emplacement_de_l_article,
                    date_derniere_entree, date_derniere_sortie, stock_securite
                FROM Stock WHERE id = ?
            """), [item_id])
            
            row = result.fetchone()
            if not row:
                return jsonify({'success': False, 'error': 'Stock item not found'})
            
            stock_item = {
                'id': row[0],
                'reference_article': row[1] or '',
                'designation_1': row[2] or '',
                'designation_2': row[3] or '',
                'categorie_article': row[4] or '',
                'quantite_en_stock': row[5] or 0,
                'seuil_de_reappro_min': row[6] or 0,
                'quantite_maximum_max': row[7] or 0,
                'pmp': row[8] or 0.0,
                'unite_de_stock': row[9] or '',
                'emplacement_de_l_article': row[10] or '',
                'date_derniere_entree': row[11] or '',
                'date_derniere_sortie': row[12] or '',
                'stock_securite': row[13] or 0
            }
            
            return jsonify({'success': True, 'item': stock_item})
            
        except Exception as e:
            logger.error(f"Error getting stock item {item_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/purchase-orders/<int:po_id>')
    def api_get_purchase_order(po_id):
        """Get detailed information for a purchase order."""
        try:
            result = db.session.execute(text("""
                SELECT 
                    id, n_commande, code_article, designation, qte_commandee,
                    prix_unitaire, montant_ligne, fournisseur, statut,
                    date_commande, date_prevue_livraison, devise, taux_de_change
                FROM PO WHERE id = :po_id
            """), {"po_id": po_id})
            
            row = result.fetchone()
            if not row:
                return jsonify({'success': False, 'error': 'Purchase order not found'})
            
            po_item = {
                'id': row[0],
                'n_commande': row[1] or '',
                'code_article': row[2] or '',
                'designation': row[3] or '',
                'qte_commandee': row[4] or 0,
                'prix_unitaire': row[5] or 0.0,
                'montant_ligne': row[6] or 0.0,
                'fournisseur': row[7] or '',
                'statut': row[8] or '',
                'date_commande': row[9] or '',
                'date_prevue_livraison': row[10] or '',
                'devise': row[11] or '',
                'taux_de_change': row[12] or 0.0
            }
            
            return jsonify({'success': True, 'item': po_item})
            
        except Exception as e:
            logger.error(f"Error getting purchase order {po_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/purchase-requests/<int:pr_id>')
    def api_get_purchase_request(pr_id):
        """Get detailed information for a purchase request."""
        try:
            result = db.session.execute(text("""
                SELECT 
                    id, no_demande, article, designation, quantita_ua,
                    prix_unitaire, prix_total, statut, date_demande,
                    demandeur, justification, centre_de_cout
                FROM PR WHERE id = :pr_id
            """), {"pr_id": pr_id})
            
            row = result.fetchone()
            if not row:
                return jsonify({'success': False, 'error': 'Purchase request not found'})
            
            pr_item = {
                'id': row[0],
                'no_demande': row[1] or '',
                'article': row[2] or '',
                'designation': row[3] or '',
                'quantite': row[4] or 0,
                'prix_unitaire': row[5] or 0.0,
                'prix_total': row[6] or 0.0,
                'statut': row[7] or '',
                'date_demande': row[8] or '',
                'demandeur': row[9] or '',
                'justification': row[10] or '',
                'centre_de_cout': row[11] or ''
            }
            
            return jsonify({'success': True, 'item': pr_item})
            
        except Exception as e:
            logger.error(f"Error getting purchase request {pr_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/spare-parts/<int:part_id>/update-stock', methods=['POST'])
    def api_update_stock(part_id):
        """Update stock quantity for a spare part."""
        if not spare_parts_service:
            return jsonify({'success': False, 'error': 'Spare parts service not available'})
            
        try:
            data = request.get_json()
            quantity = data.get('quantity')
            transaction_type = data.get('transaction_type', 'manual')
            notes = data.get('notes', '')
            
            if quantity is None:
                return jsonify({'success': False, 'error': 'Quantity is required'})
            
            result = spare_parts_service.update_stock_quantity(
                part_id, quantity, transaction_type, notes
            )
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error updating stock for part {part_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/spare-parts/<int:part_id>/stock-movement', methods=['POST'])
    def api_stock_movement(part_id):
        """Record stock movement (receipt, issue, transfer, adjustment)."""
        if not spare_parts_service:
            return jsonify({'success': False, 'error': 'Spare parts service not available'})
            
        try:
            data = request.get_json()
            movement_type = data.get('movement_type')  # 'receipt', 'issue', 'transfer', 'adjustment'
            quantity = data.get('quantity')
            reference_doc = data.get('reference_doc', '')
            notes = data.get('notes', '')
            cost_per_unit = data.get('cost_per_unit')
            
            if not movement_type or quantity is None:
                return jsonify({'success': False, 'error': 'Movement type and quantity are required'})
            
            result = spare_parts_service.record_stock_movement(
                part_id, movement_type, quantity, reference_doc, notes, cost_per_unit
            )
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error recording stock movement for part {part_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/spare-parts/reorder-suggestions')
    def api_reorder_suggestions():
        """Get intelligent reorder suggestions."""
        if not spare_parts_service:
            return jsonify({'success': False, 'error': 'Spare parts service not available'})
            
        try:
            suggestions = spare_parts_service.get_reorder_suggestions()
            return jsonify({'success': True, 'suggestions': suggestions})
            
        except Exception as e:
            logger.error(f"Error getting reorder suggestions: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/spare-parts/generate-po', methods=['POST'])
    def api_generate_purchase_order():
        """Generate purchase order for selected parts."""
        if not spare_parts_service:
            return jsonify({'success': False, 'error': 'Spare parts service not available'})
            
        try:
            data = request.get_json()
            part_ids = data.get('part_ids', [])
            supplier_id = data.get('supplier_id')
            
            if not part_ids:
                return jsonify({'success': False, 'error': 'No parts selected'})
            
            result = spare_parts_service.generate_purchase_order(part_ids, supplier_id)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error generating purchase order: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/spare-parts/stats')
    def api_spare_parts_stats():
        """Get comprehensive spare parts statistics."""
        if not spare_parts_service:
            return jsonify({'success': False, 'error': 'Spare parts service not available'})
            
        try:
            stats = spare_parts_service.get_spare_parts_statistics()
            return jsonify({'success': True, 'stats': stats})
            
        except Exception as e:
            logger.error(f"Error getting spare parts stats: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/spare-parts/search')
    def api_search_spare_parts():
        """Search spare parts with filters."""
        if not spare_parts_service:
            return jsonify({'success': False, 'error': 'Spare parts service not available'})
            
        try:
            search_term = request.args.get('q', '')
            category = request.args.get('category', '')
            location = request.args.get('location', '')
            status = request.args.get('status', '')  # 'critical', 'out_of_stock', 'normal'
            
            results = spare_parts_service.advanced_search(
                search_term, category, location, status
            )
            
            return jsonify({'success': True, 'results': results})
            
        except Exception as e:
            logger.error(f"Error searching spare parts: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    # ============================================================
    # CURRENCY API ROUTES
    # ============================================================
    
    @app.route('/api/currency/info')
    def api_currency_info():
        """Get currency conversion information."""
        try:
            info = currency_service.get_currency_info()
            return jsonify({'success': True, 'currency_info': info})
        except Exception as e:
            logger.error(f"Error getting currency info: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/currency/convert', methods=['POST'])
    def api_currency_convert():
        """Convert currency amounts."""
        try:
            data = request.get_json()
            amount = data.get('amount', 0)
            from_currency = data.get('from', 'XOF')
            to_currency = data.get('to', 'EUR')
            force_update = data.get('force_update', False)
            
            if from_currency == 'XOF' and to_currency == 'EUR':
                converted_amount = currency_service.convert_to_eur(amount, force_update)
            elif from_currency == 'EUR' and to_currency == 'XOF':
                converted_amount = currency_service.convert_from_eur(amount, force_update)
            else:
                return jsonify({'success': False, 'error': f'Conversion from {from_currency} to {to_currency} not supported'})
            
            return jsonify({
                'success': True,
                'original_amount': amount,
                'converted_amount': converted_amount,
                'from_currency': from_currency,
                'to_currency': to_currency,
                'exchange_rate': currency_service.get_exchange_rate(force_update),
                'formatted': currency_service.format_currency(amount, from_currency)
            })
            
        except Exception as e:
            logger.error(f"Error converting currency: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    # ============================================================
    # STOCK ANALYSIS API ROUTES
    # ============================================================
    
    @app.route('/api/stock-analysis/comprehensive')
    def api_comprehensive_stock_analysis():
        """Get comprehensive stock analysis combining all tables."""
        if not stock_analysis_service:
            return jsonify({'success': False, 'error': 'Stock analysis service not available'})
            
        try:
            limit = request.args.get('limit', type=int)
            article_filter = request.args.get('article_filter', '')
            
            if article_filter:
                results = stock_analysis_service.get_comprehensive_stock_analysis(
                    limit=limit, article_filter=article_filter
                )
            else:
                results = stock_analysis_service.get_comprehensive_stock_analysis(limit=limit)
            
            return jsonify(results)
            
        except Exception as e:
            logger.error(f"Error in comprehensive stock analysis: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/stock-analysis/article/<article_reference>')
    def api_article_details(article_reference):
        """Get detailed information for a specific article."""
        if not stock_analysis_service:
            return jsonify({'success': False, 'error': 'Stock analysis service not available'})
            
        try:
            results = stock_analysis_service.get_article_details(article_reference)
            return jsonify(results)
            
        except Exception as e:
            logger.error(f"Error getting article details: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/stock-analysis/alerts')
    def api_stock_alerts():
        """Get stock alerts for items requiring attention."""
        if not stock_analysis_service:
            return jsonify({'success': False, 'error': 'Stock analysis service not available'})
            
        try:
            results = stock_analysis_service.get_stock_alerts()
            return jsonify(results)
            
        except Exception as e:
            logger.error(f"Error getting stock alerts: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/stock-analysis/search')
    def api_search_articles():
        """Search for articles based on various criteria."""
        if not stock_analysis_service:
            return jsonify({'success': False, 'error': 'Stock analysis service not available'})
            
        try:
            search_term = request.args.get('q', '')
            search_field = request.args.get('field', 'reference_article')
            
            results = stock_analysis_service.search_articles(search_term, search_field)
            return jsonify(results)
            
        except Exception as e:
            logger.error(f"Error searching articles: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/stock-analysis/export')
    def api_export_stock_analysis():
        """Export stock analysis data to Excel or CSV."""
        if not stock_analysis_service:
            return jsonify({'success': False, 'error': 'Stock analysis service not available'})
            
        try:
            format_type = request.args.get('format', 'excel')
            limit = request.args.get('limit', type=int)
            
            results = stock_analysis_service.export_stock_analysis(format_type, limit)
            return jsonify(results)
            
        except Exception as e:
            logger.error(f"Error exporting stock analysis: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    # ============================================================
    # GENERAL API ROUTES
    # ============================================================

    # API Routes
    @app.route('/api/table/<table_name>/info')
    def api_table_info(table_name):
        """API endpoint for table information."""
        try:
            table_info = db_service.get_table_info(table_name)
            if not table_info:
                return jsonify({'success': False, 'error': 'Table not found'})
            
            return jsonify({'success': True, 'table_info': table_info})
            
        except Exception as e:
            logger.error(f"API error getting table info for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/<table_name>', methods=['DELETE'])
    def api_delete_table(table_name):
        """API endpoint for deleting a table."""
        try:
            success = db_service.delete_table(table_name)
            if success:
                return jsonify({'success': True, 'message': f'Table "{table_name}" deleted successfully'})
            else:
                return jsonify({'success': False, 'error': 'Table not found or could not be deleted'})
                
        except Exception as e:
            logger.error(f"API error deleting table {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/<table_name>/record/<int:record_id>')
    def api_get_record(table_name, record_id):
        """API endpoint for getting a specific record."""
        try:
            record = db_service.get_record(table_name, record_id)
            if not record:
                return jsonify({'success': False, 'error': 'Record not found'})
            
            return jsonify({'success': True, 'record': record})
            
        except Exception as e:
            logger.error(f"API error getting record {record_id} from {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/<table_name>/record/<int:record_id>', methods=['PUT'])
    def api_update_record(table_name, record_id):
        """API endpoint for updating a record."""
        try:
            data = request.get_json()
            if not data:
                return jsonify({'success': False, 'error': 'No data provided'})
            
            success = db_service.update_record(table_name, record_id, data)
            if success:
                return jsonify({'success': True, 'message': 'Record updated successfully'})
            else:
                return jsonify({'success': False, 'error': 'Record not found or could not be updated'})
                
        except Exception as e:
            logger.error(f"API error updating record {record_id} in {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/<table_name>/record/<int:record_id>', methods=['DELETE'])
    def api_delete_record(table_name, record_id):
        """API endpoint for deleting a record."""
        try:
            success = db_service.delete_record(table_name, record_id)
            if success:
                return jsonify({'success': True, 'message': 'Record deleted successfully'})
            else:
                return jsonify({'success': False, 'error': 'Record not found or could not be deleted'})
                
        except Exception as e:
            logger.error(f"API error deleting record {record_id} from {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/<table_name>/stats')
    def api_table_stats(table_name):
        """API endpoint for table statistics."""
        try:
            # Get basic table info
            table_info = db_service.get_table_info(table_name)
            if not table_info:
                return jsonify({'success': False, 'error': 'Table not found'})
            
            # Get column statistics
            stats = {}
            for column in table_info['columns']:
                column_name = column['name']
                if column_name != 'id':  # Skip ID column
                    column_stats = db_service.get_column_stats(table_name, column_name)
                    stats[column_name] = column_stats
            
            return jsonify({
                'success': True, 
                'table_info': table_info,
                'column_stats': stats
            })
            
        except Exception as e:
            logger.error(f"API error getting stats for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    # Enhanced Upload Routes
    @app.route('/enhanced-upload')
    def enhanced_upload():
        """Enhanced upload page with worksheet selection."""
        return render_template('enhanced_upload.html')

    @app.route('/memory-upload')
    def memory_upload():
        """Memory-based upload test page."""
        return render_template('memory_upload.html')
    
    @app.route('/interactive-upload')
    def interactive_upload():
        """Interactive upload page with sheet selection and data type customization."""
        return render_template('interactive_upload.html')
    
    @app.route('/api/excel/analyze', methods=['POST'])
    def analyze_excel_file():
        """Analyze Excel file and return sheet information with data type suggestions."""
        try:
            if 'file' not in request.files:
                return jsonify({'success': False, 'error': 'No file selected'})
            
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({'success': False, 'error': 'No file selected'})
            
            if not universal_file_processor.allowed_file(file.filename):
                return jsonify({'success': False, 'error': 'File type not supported'})
            
            if universal_file_processor.get_file_type(file.filename) != 'excel':
                return jsonify({'success': False, 'error': 'Only Excel files are supported for analysis'})
            
            # Analyze the Excel file
            analysis_result = universal_file_processor.analyze_excel_file(file)
            
            # Additional safety check for JSON serialization
            import json
            try:
                json.dumps(analysis_result)  # Test if it's JSON serializable
            except TypeError as json_error:
                logger.error(f"JSON serialization test failed: {json_error}")
                # Return a simplified error response
                return jsonify({
                    'success': False,
                    'error': f'Data contains non-serializable types: {str(json_error)}'
                })
            
            return jsonify({
                'success': True,
                'analysis': analysis_result
            })
            
        except Exception as e:
            logger.error(f"Error analyzing Excel file: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/excel/process-with-config', methods=['POST'])
    def process_excel_with_config():
        """Process Excel file with user-selected sheets and data type configurations."""
        try:
            if 'file' not in request.files:
                return jsonify({'success': False, 'error': 'No file selected'})
            
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({'success': False, 'error': 'No file selected'})
            
            # Get configuration from form data
            config_json = request.form.get('config')
            if not config_json:
                return jsonify({'success': False, 'error': 'No configuration provided'})
            
            try:
                import json
                config_data = json.loads(config_json)
            except:
                return jsonify({'success': False, 'error': 'Invalid configuration format'})
            
            # Parse the configuration
            selected_sheets = config_data.get('selected_sheets', [])
            column_types = config_data.get('column_types', {})
            
            if not selected_sheets:
                return jsonify({'success': False, 'error': 'No sheets selected for processing'})
            
            # Process file with custom configuration
            upload_id, created_tables = universal_file_processor.process_excel_with_config(
                file, selected_sheets, column_types
            )
            
            return jsonify({
                'success': True,
                'upload_id': upload_id,
                'created_tables': created_tables,
                'message': f'Successfully processed {len(created_tables)} table(s)'
            })
            
        except Exception as e:
            logger.error(f"Error processing Excel file with config: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/excel/analyze-memory', methods=['POST'])
    def analyze_excel_memory():
        """Analyze uploaded Excel file in memory without saving to disk."""
        try:
            if 'file' not in request.files:
                return jsonify({'success': False, 'error': 'No file selected'})
            
            file = request.files['file']
            
            if file.filename == '':
                return jsonify({'success': False, 'error': 'No file selected'})
            
            if not enhanced_excel_processor.allowed_file(file.filename):
                return jsonify({'success': False, 'error': 'Invalid file type. Please upload .xlsx, .xls, or .xlsm files only.'})
            
            # Check file size before processing
            file.seek(0, 2)  # Seek to end of file
            file_size = file.tell()  # Get current position (file size)
            file.seek(0)  # Reset to beginning
            
            max_size = app.config.get('MAX_CONTENT_LENGTH', 512 * 1024 * 1024)
            if file_size > max_size:
                size_mb = file_size / (1024 * 1024)
                max_mb = max_size / (1024 * 1024)
                return jsonify({'success': False, 'error': f'File too large ({size_mb:.1f}MB). Maximum allowed size is {max_mb:.0f}MB.'})
            
            # Analyze worksheets directly from memory
            worksheets_info = enhanced_excel_processor.get_excel_worksheets_from_memory(file)
            
            return jsonify({
                'success': True,
                'filename': file.filename,
                'worksheets': worksheets_info,
                'total_worksheets': len(worksheets_info)
            })
            
        except Exception as e:
            logger.error(f"Error analyzing Excel file: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
            worksheets_info = enhanced_excel_processor.get_excel_worksheets(file_path)
            
            return jsonify({
                'success': True,
                'file_path': file_path,
                'worksheets': worksheets_info,
                'total_worksheets': len(worksheets_info)
            })
            
        except Exception as e:
            logger.error(f"Error analyzing Excel file: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/databases/available')
    def get_available_databases():
        """Get list of available databases."""
        try:
            databases = []
            instance_dir = os.path.join(os.path.dirname(__file__), 'instance')
            
            if os.path.exists(instance_dir):
                for file in os.listdir(instance_dir):
                    if file.endswith('.db'):
                        db_name = file[:-3]  # Remove .db extension
                        db_path = os.path.join(instance_dir, file)
                        db_size = os.path.getsize(db_path)
                        
                        # Count tables in this database
                        table_count = 0
                        try:
                            import sqlite3
                            conn = sqlite3.connect(db_path)
                            cursor = conn.cursor()
                            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('table_metadata', 'upload_history')")
                            table_count = cursor.fetchone()[0]
                            conn.close()
                        except Exception as e:
                            logger.warning(f"Could not count tables in {db_name}: {e}")
                        
                        databases.append({
                            'name': db_name,
                            'filename': file,
                            'path': db_path,
                            'size': db_size,
                            'size_mb': round(db_size / (1024 * 1024), 2),
                            'table_count': table_count
                        })
            
            return jsonify({
                'success': True,
                'databases': databases,
                'total_databases': len(databases)
            })
        except Exception as e:
            logger.error(f"Error getting available databases: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/tables/existing')
    def get_existing_tables():
        """Get list of existing database tables."""
        try:
            # Get database parameter
            database = request.args.get('database', 'excel_data')
            
            if database and database != 'excel_data':
                # Use specific database
                db_path = os.path.join(os.path.dirname(__file__), 'instance', f'{database}.db')
                if not os.path.exists(db_path):
                    return jsonify({'success': False, 'error': f'Database {database} not found'})
                
                import sqlite3
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                tables = [row[0] for row in cursor.fetchall()]
                conn.close()
            else:
                # Use default database
                existing_tables = enhanced_excel_processor.get_existing_tables()
                tables = existing_tables
            
            return jsonify({
                'success': True,
                'tables': tables,
                'total_tables': len(tables),
                'database': database
            })
        except Exception as e:
            logger.error(f"Error getting existing tables: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/tables/<table_name>/data')
    def get_table_data(table_name):
        """Get data from a specific table."""
        try:
            # Get limit parameter (default 100)
            limit = request.args.get('limit', 100, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            # Validate table name exists
            existing_tables = enhanced_excel_processor.get_existing_tables()
            table_exists = any(table['name'] == table_name for table in existing_tables)
            
            if not table_exists:
                return jsonify({'success': False, 'error': f'Table "{table_name}" not found'})
            
            # Get table data using database service
            from services.database_service import get_table_data, get_table_columns, get_table_row_count
            
            # Get columns info
            columns = get_table_columns(table_name)
            column_names = [col['name'] for col in columns]
            
            # Get total row count
            total_rows = get_table_row_count(table_name)
            
            # Get data with limit
            data = get_table_data(table_name, limit=limit, offset=offset)
            
            return jsonify({
                'success': True,
                'table_name': table_name,
                'data': data,
                'columns': column_names,
                'total_rows': total_rows,
                'returned_rows': len(data),
                'offset': offset,
                'limit': limit
            })
            
        except Exception as e:
            logger.error(f"Error getting table data for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/excel/compare', methods=['POST'])
    def compare_structures():
        """Compare worksheet structure with existing table."""
        try:
            data = request.get_json()
            worksheet_columns = data.get('worksheet_columns', [])
            table_name = data.get('table_name', '')
            
            if not worksheet_columns or not table_name:
                return jsonify({'success': False, 'error': 'Missing required parameters'})
            
            comparison = enhanced_excel_processor.compare_table_structure(worksheet_columns, table_name)
            
            return jsonify({
                'success': True,
                'comparison': comparison
            })
            
        except Exception as e:
            logger.error(f"Error comparing structures: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/excel/import', methods=['POST'])
    def import_excel_data():
        """Import Excel data based on user selections."""
        try:
            file_path = request.form.get('file_path')
            worksheet = request.form.get('worksheet')
            import_mode = request.form.get('import_mode')
            target_table = request.form.get('target_table')
            new_table_name = request.form.get('new_table_name')
            
            if not file_path or not worksheet or not import_mode:
                return jsonify({'success': False, 'error': 'Missing required parameters'})
            
            if import_mode == 'replace':
                if not target_table:
                    return jsonify({'success': False, 'error': 'Target table is required for replace mode'})
                
                success, message, records_imported = enhanced_excel_processor.replace_table_data(
                    file_path, worksheet, target_table
                )
                
                # Don't delete file here - let batch processing handle it
                
                return jsonify({
                    'success': success,
                    'message': message,
                    'records_imported': records_imported,
                    'table_name': target_table if success else None
                })
                
            elif import_mode == 'new':
                # Use provided name or generate from worksheet name
                table_name = new_table_name.strip() if new_table_name and new_table_name.strip() else \
                           enhanced_excel_processor.sanitize_table_name(worksheet)
                
                # Ensure unique table name
                existing_tables = enhanced_excel_processor.get_existing_tables()
                existing_names = [t['name'] for t in existing_tables]
                
                original_name = table_name
                counter = 1
                while table_name in existing_names:
                    table_name = f"{original_name}_{counter}"
                    counter += 1
                
                success, message, records_imported = enhanced_excel_processor.create_new_table_from_worksheet(
                    file_path, worksheet, table_name
                )
                
                # Don't delete file here - let batch processing handle it
                
                return jsonify({
                    'success': success,
                    'message': message,
                    'records_imported': records_imported,
                    'table_name': table_name if success else None
                })
            else:
                return jsonify({'success': False, 'error': 'Invalid import mode'})
            
        except Exception as e:
            logger.error(f"Error importing Excel data: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/excel/import-memory', methods=['POST'])
    def import_excel_memory():
        """Import Excel worksheet data directly from memory without saving file."""
        try:
            # Get the original file from session or multipart data
            file_data = None
            
            # Check if file is in the request
            if 'file' in request.files:
                file_data = request.files['file']
            else:
                return jsonify({'success': False, 'error': 'No file data provided'})
            
            # Get form data
            worksheet_name = request.form.get('worksheet')
            import_mode = request.form.get('import_mode')
            target_table = request.form.get('target_table', '')
            new_table_name = request.form.get('new_table_name', '')
            
            if not worksheet_name or not import_mode:
                return jsonify({'success': False, 'error': 'Missing required parameters'})
            
            # Validate import mode and parameters
            if import_mode == 'new' and not new_table_name:
                return jsonify({'success': False, 'error': 'New table name is required for new table mode'})
            
            if import_mode in ['replace', 'append'] and not target_table:
                return jsonify({'success': False, 'error': 'Target table is required for replace/append mode'})
            
            # Import worksheet using memory processing
            result = enhanced_excel_processor.import_worksheet_from_memory(
                file=file_data,
                worksheet_name=worksheet_name,
                import_mode=import_mode,
                target_table=target_table,
                new_table_name=new_table_name
            )
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error in memory Excel import: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/excel/cleanup', methods=['POST'])
    def cleanup_excel_file():
        """Clean up uploaded Excel file after batch processing."""
        try:
            file_path = request.form.get('file_path')
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                return jsonify({'success': True, 'message': 'File cleaned up successfully'})
            else:
                return jsonify({'success': False, 'error': 'File not found or already deleted'})
        except Exception as e:
            logger.error(f"Error cleaning up file: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/test-db')
    def test_database():
        """Simple test to check database connection and tables."""
        try:
            import sqlite3
            db_path = os.path.join(app.instance_path, 'Stock.db')
            
            if not os.path.exists(db_path):
                return f"Database file not found at: {db_path}"
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = cursor.fetchall()
            
            result = f"Database path: {db_path}<br><br>"
            result += f"Tables found: {len(tables)}<br><br>"
            
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                result += f"Table: {table_name} - Rows: {count}<br>"
            
            conn.close()
            return result
            
        except Exception as e:
            return f"Error: {str(e)}"

    @app.route('/debug/database')
    def debug_database():
        """Debug endpoint to see what's in the database."""
        try:
            import sqlite3
            db_path = os.path.join(app.instance_path, 'Stock.db')
            
            result = {'tables': [], 'metadata': [], 'uploads': []}
            
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = cursor.fetchall()
                
                for table in tables:
                    table_name = table[0]
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    count = cursor.fetchone()[0]
                    result['tables'].append({'name': table_name, 'count': count})
                
                # Get metadata records
                try:
                    cursor.execute("SELECT * FROM table_metadata")
                    metadata = cursor.fetchall()
                    result['metadata'] = metadata
                except:
                    result['metadata'] = "No metadata table"
                
                # Get upload history
                try:
                    cursor.execute("SELECT * FROM upload_history")
                    uploads = cursor.fetchall()
                    result['uploads'] = uploads
                except:
                    result['uploads'] = "No upload history table"
                
                conn.close()
            else:
                result['error'] = f"Database file not found: {db_path}"
            
            return jsonify(result)
            
        except Exception as e:
            return jsonify({'error': str(e)})

    # Relationships Routes
    @app.route('/relationships')
    def relationships():
        """Table relationships configuration page - redirect to new version."""
        return redirect(url_for('relationships_new'))
    
    @app.route('/relationships-new')
    def relationships_new():
        """Visual relationship designer with query execution capabilities."""
        try:
            # Get available databases from database management service
            db_info = db_management_service.get_database_selection_info()
            available_databases = [db['display_name'] for db in db_info['databases']]
            
            return render_template('relationships_new.html', 
                                 databases=available_databases,
                                 current_db_name=get_current_database_name())
        except Exception as e:
            return render_template('relationships_new.html', 
                                 databases=['excel_data'],
                                 current_db_name=get_current_database_name())
    
    @app.route('/executive-dashboard')
    def executive_dashboard():
        """LCT STS Maintenance Executive Dashboard - Maintenance performance overview with real data"""
        from services.database_service import DatabaseService
        
        try:
            db_service = DatabaseService()
            
            # Get real maintenance KPI data from your tables
            kpis = {}
            
            # Total Work Orders
            try:
                wo_result = db.session.execute(text("SELECT COUNT(*) FROM work_orders"))
                kpis['total_work_orders'] = wo_result.scalar() or 0
            except:
                kpis['total_work_orders'] = 0
            
            # Preventive Maintenance Count
            try:
                pm_result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE maintenance_type = 'PM'"))
                kpis['total_pm'] = pm_result.scalar() or 0
            except:
                kpis['total_pm'] = 0
                
            # Corrective Maintenance Count
            try:
                cm_result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE maintenance_type = 'CM'"))
                kpis['total_cm'] = cm_result.scalar() or 0
            except:
                kpis['total_cm'] = 0
                
            # Breakdown Maintenance Count
            try:
                breakdown_result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE maintenance_type = 'Breakdown'"))
                kpis['total_breakdowns'] = breakdown_result.scalar() or 0
            except:
                kpis['total_breakdowns'] = 0
            
            # Total Purchase Orders
            try:
                po_result = db.session.execute(text("SELECT COUNT(*) FROM po"))
                kpis['total_pos'] = po_result.scalar() or 0
            except:
                kpis['total_pos'] = 0
            
            # Total Purchase Requests  
            try:
                pr_result = db.session.execute(text("SELECT COUNT(*) FROM PR"))
                kpis['total_prs'] = pr_result.scalar() or 0
            except:
                kpis['total_prs'] = 0
                
            # Total Spare Parts
            try:
                spare_parts_result = db.session.execute(text("SELECT COUNT(*) FROM spare_parts"))
                kpis['total_spare_parts'] = spare_parts_result.scalar() or 0
            except:
                try:
                    # Fallback to Stock table if spare_parts doesn't exist
                    stock_result = db.session.execute(text("SELECT COUNT(*) FROM Stock"))
                    kpis['total_spare_parts'] = stock_result.scalar() or 0
                except:
                    kpis['total_spare_parts'] = 0
                    
            # Critical Spare Parts (Low Stock)
            try:
                critical_parts_result = db.session.execute(text("SELECT COUNT(*) FROM spare_parts WHERE quantity_on_hand <= reorder_level"))
                kpis['critical_spare_parts'] = critical_parts_result.scalar() or 0
            except:
                kpis['critical_spare_parts'] = 0
            
            # Maintenance Efficiency (Completed vs Total Work Orders)
            try:
                completed_wo = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE status = 'Completed'"))
                total_wo = kpis['total_work_orders']
                if total_wo > 0:
                    kpis['maintenance_efficiency'] = round((completed_wo.scalar() / total_wo) * 100, 1)
                else:
                    kpis['maintenance_efficiency'] = 0
            except:
                kpis['maintenance_efficiency'] = 0
                
            # Available tables
            tables = db_service.get_all_tables()
            kpis['total_tables'] = len(tables)
            
            # Calculate total records across all maintenance-related tables
            total_records = (kpis['total_work_orders'] + kpis['total_pos'] + 
                           kpis['total_prs'] + kpis['total_spare_parts'])
            kpis['total_records'] = total_records
            
            return render_template('executive_dashboard.html', kpis=kpis, tables=tables)
            
        except Exception as e:
            # Fallback to mock data if there's an error
            kpis = {
                'total_work_orders': 0,
                'total_pm': 0,
                'total_cm': 0, 
                'total_breakdowns': 0,
                'total_pos': 0,
                'total_prs': 0,
                'total_spare_parts': 0,
                'critical_spare_parts': 0,
                'maintenance_efficiency': 0,
                'total_tables': 0,
                'total_records': 0
            }
            return render_template('executive_dashboard.html', kpis=kpis, tables=[], error=str(e))
    
    @app.route('/data-analysis')
    def data_analysis():
        """Comprehensive data analysis dashboard."""
        return render_template('data_analysis.html')
    
    @app.route('/graphs-analysis')
    def graphs_analysis():
        """Charts and graphs data analysis dashboard."""
        return render_template('graphs_analysis.html')
    
    @app.route('/advanced-charts')
    def advanced_charts():
        """Advanced Charts - Power BI style visualizations with real data"""
        from services.database_service import DatabaseService
        
        try:
            db_service = DatabaseService()
            tables = db_service.get_all_tables()
            
            # Get available databases from database management service
            db_info = db_management_service.get_database_selection_info()
            available_databases = [db['display_name'] for db in db_info['databases']]
            
            return render_template('advanced_charts.html', 
                                 tables=tables, 
                                 databases=available_databases,
                                 current_db_name=get_current_database_name())
        except Exception as e:
            return render_template('advanced_charts.html', 
                                 tables=[], 
                                 databases=['excel_data'],
                                 current_db_name=get_current_database_name(),
                                 error=str(e))
    
    @app.route('/report-builder')
    def report_builder():
        """Interactive drag-and-drop report builder."""
        try:
            # Get available databases from database management service
            db_info = db_management_service.get_database_selection_info()
            available_databases = [db['display_name'] for db in db_info['databases']]
            
            return render_template('report_builder.html', 
                                 databases=available_databases,
                                 current_db_name=get_current_database_name())
        except Exception as e:
            return render_template('report_builder.html', 
                                 databases=['excel_data'],
                                 current_db_name=get_current_database_name())
    
    @app.route('/data-grid')
    def data_grid():
        """Excel-style spreadsheet data grid interface with real data"""
        from services.database_service import DatabaseService
        
        try:
            db_service = DatabaseService()
            tables = db_service.get_all_tables()
            
            # Get available databases from database management service
            db_info = db_management_service.get_database_selection_info()
            available_databases = [db['display_name'] for db in db_info['databases']]
            
            return render_template('data_grid.html', 
                                 tables=tables, 
                                 databases=available_databases,
                                 current_db_name=get_current_database_name())
        except Exception as e:
            return render_template('data_grid.html', 
                                 tables=[], 
                                 databases=['excel_data'],
                                 current_db_name=get_current_database_name(),
                                 error=str(e))
    
    @app.route('/calculated-fields')
    def calculated_fields():
        """Calculated fields creation interface."""
        return render_template('calculated_fields.html')
    
    @app.route('/api/relationships/columns/<table_name>')
    def api_get_table_columns(table_name):
        """Get columns for a specific table."""
        try:
            # Get database parameter
            database = request.args.get('database', 'excel_data')
            columns = relationship_service.get_table_columns(table_name, database)
            return jsonify({'success': True, 'columns': columns, 'database': database})
        except Exception as e:
            logger.error(f"Error getting columns for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/suggestions', methods=['POST'])
    def api_relationship_suggestions():
        """Get suggested relationships between tables."""
        try:
            data = request.get_json()
            tables = data.get('tables', [])
            
            if len(tables) < 2:
                return jsonify({'success': False, 'error': 'At least 2 tables required'})
            
            relationships = relationship_service.find_potential_relationships(tables)
            return jsonify({'success': True, 'relationships': relationships})
            
        except Exception as e:
            logger.error(f"Error finding relationship suggestions: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/preview', methods=['POST'])
    def api_relationship_preview():
        """Generate preview of joined data."""
        try:
            config = request.get_json()
            if not config:
                return jsonify({'success': False, 'error': 'No configuration provided'})
            
            # Validate configuration
            if not config.get('tables'):
                return jsonify({'success': False, 'error': 'No tables selected'})
            
            # Only require joins if we have multiple tables
            if len(config.get('tables', [])) > 1 and not config.get('joins'):
                return jsonify({'success': False, 'error': 'No joins defined for multiple tables'})
            
            # Validate filter rules
            filters = config.get('filters', [])
            for filter_rule in filters:
                if not all(key in filter_rule for key in ['table', 'column', 'operator']):
                    return jsonify({'success': False, 'error': 'Invalid filter rule format'})
            
            result = relationship_service.preview_joined_data(config, limit=100)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error generating relationship preview: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/validate', methods=['POST'])
    def api_validate_relationships():
        """Validate relationship configuration."""
        try:
            config = request.get_json()
            validation = relationship_service.validate_join_configuration(config)
            return jsonify({'success': True, 'validation': validation})
            
        except Exception as e:
            logger.error(f"Error validating relationships: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/test-sql', methods=['POST'])
    def api_test_sql():
        """Test custom SQL query."""
        try:
            data = request.get_json()
            custom_query = data.get('query', '').strip()
            database = data.get('database', 'excel_data')
            
            if not custom_query:
                return jsonify({'success': False, 'error': 'No query provided'})
            
            # Execute the custom query safely
            result_data = relationship_service.execute_custom_query(custom_query, database)
            
            return jsonify({
                'success': True,
                'data': result_data['data'],
                'columns': result_data['columns'],
                'row_count': result_data['row_count'],
                'query': custom_query
            })
            
        except Exception as e:
            logger.error(f"Error testing custom SQL: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/execute-as-table', methods=['POST'])
    def api_execute_as_table():
        """Execute SQL query and save results as a new table."""
        try:
            data = request.get_json()
            custom_query = data.get('query', '').strip()
            table_name = data.get('table_name', '').strip()
            database = data.get('database', 'excel_data')
            
            if not custom_query:
                return jsonify({'success': False, 'error': 'No query provided'})
            
            if not table_name:
                return jsonify({'success': False, 'error': 'No table name provided'})
            
            # Validate table name (alphanumeric and underscores only)
            import re
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', table_name):
                return jsonify({'success': False, 'error': 'Invalid table name. Use only letters, numbers, and underscores.'})
            
            # Execute the query and create table
            result = relationship_service.execute_query_as_table(custom_query, table_name, database)
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error executing query as table: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/analyze-table/<table_name>')
    def api_analyze_table(table_name):
        """Comprehensive analysis of a table's data."""
        try:
            # Get database parameter
            database = request.args.get('database', 'excel_data')
            
            # Perform comprehensive table analysis
            analysis_result = relationship_service.analyze_table_data(table_name, database)
            return jsonify(analysis_result)
            
        except Exception as e:
            logger.error(f"Error analyzing table {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/export', methods=['POST'])
    def api_export_relationships():
        """Export joined data to Excel."""
        try:
            config = request.get_json()
            result = relationship_service.export_joined_data_to_excel(config)
            
            if result['success']:
                # Return file info for download
                return jsonify(result)
            else:
                return jsonify(result)
                
        except Exception as e:
            logger.error(f"Error exporting relationships: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/sample/<table_name>')
    def api_get_table_sample(table_name):
        """Get sample data from a table."""
        try:
            result = relationship_service.get_table_sample_data(table_name, limit=5)
            return jsonify(result)
        except Exception as e:
            logger.error(f"Error getting sample data for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/save-config', methods=['POST'])
    def api_save_relationship_config():
        """Save a relationship configuration."""
        try:
            data = request.get_json()
            config = data.get('config')
            name = data.get('name')
            
            if not config or not name:
                return jsonify({'success': False, 'error': 'Configuration and name are required'})
            
            result = relationship_service.save_relationship_configuration(config, name)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error saving relationship configuration: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/load-config/<config_name>')
    def api_load_relationship_config(config_name):
        """Load a saved relationship configuration."""
        try:
            result = relationship_service.load_relationship_configuration(config_name)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error loading relationship configuration: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/list-configs')
    def api_list_relationship_configs():
        """List all saved relationship configurations."""
        try:
            result = relationship_service.list_saved_configurations()
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error listing relationship configurations: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/delete-config/<config_name>', methods=['DELETE'])
    def api_delete_relationship_config(config_name):
        """Delete a saved relationship configuration."""
        try:
            result = relationship_service.delete_relationship_configuration(config_name)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error deleting relationship configuration: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/relationships/test-performance', methods=['POST'])
    def api_test_relationship_performance():
        """Test the performance of a relationship configuration."""
        try:
            config = request.get_json()
            if not config:
                return jsonify({'success': False, 'error': 'No configuration provided'})
            
            result = relationship_service.test_relationship_performance(config)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error testing relationship performance: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    # Serve uploaded files
    @app.route('/uploads/<filename>')
    def uploaded_file(filename):
        """Serve uploaded files for download."""
        try:
            uploads_dir = app.config['UPLOAD_FOLDER']
            return send_from_directory(uploads_dir, filename)
        except Exception as e:
            logger.error(f"Error serving file {filename}: {str(e)}")
            return "File not found", 404
    
    # Error handlers
    @app.errorhandler(404)
    def not_found_error(error):
        return render_template('base.html'), 404
    
    @app.errorhandler(500)
    def internal_error(error):
        db.session.rollback()
        return render_template('base.html'), 500
    
    @app.errorhandler(413)
    def too_large(error):
        error_msg = 'File too large. Maximum size is 512MB. Please try splitting your file into smaller chunks or contact support for assistance with larger files.'
        if request.is_json:
            return jsonify({'success': False, 'error': error_msg}), 413
        flash(error_msg, 'error')
        return redirect(url_for('upload'))
    
    # Database Management Routes
    @app.route('/database-management')
    def database_management():
        """Database management page."""
        try:
            databases = db_management_service.list_databases()
            tables = db_service.get_all_tables()
            return render_template('database_management.html', 
                                 databases=databases, 
                                 tables=tables)
        except Exception as e:
            logger.error(f"Error in database management: {e}")
            flash('Error loading database management', 'error')
            return redirect(url_for('index'))

    @app.route('/progress-demo')
    def progress_demo():
        """Show progress bar demo page"""
        return render_template('progress_demo.html')

    @app.route('/test-table-copy')
    def test_table_copy():
        """Test page for table copy functionality"""
        return render_template('test_table_copy.html')

    @app.route('/api/debug/tables')
    def debug_tables():
        """Debug endpoint to check table loading"""
        try:
            from sqlalchemy import inspect
            inspector = inspect(db.engine)
            raw_tables = inspector.get_table_names()
            
            # Also get from enhanced processor
            existing_tables = enhanced_excel_processor.get_existing_tables()
            
            # Get from db_service
            all_tables = db_service.get_all_tables()
            
            return jsonify({
                'success': True,
                'raw_sqlalchemy_tables': raw_tables,
                'enhanced_processor_tables': existing_tables,
                'db_service_tables': all_tables,
                'database_uri': app.config.get('SQLALCHEMY_DATABASE_URI', 'Not found')
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e), 'traceback': str(e.__traceback__)})

    @app.route('/api/simple-tables')
    def simple_tables():
        """Simple endpoint to get current database tables"""
        try:
            from sqlalchemy import inspect, text
            inspector = inspect(db.engine)
            raw_tables = inspector.get_table_names()
            
            # Filter out system tables and return user tables with basic info
            user_tables = []
            for table_name in raw_tables:
                if not table_name.startswith('sqlite_') and table_name not in ['upload_history', 'table_metadata']:
                    try:
                        # Get row count
                        result = db.session.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                        row_count = result.scalar()
                        user_tables.append({
                            'name': table_name,
                            'row_count': row_count
                        })
                    except Exception as e:
                        user_tables.append({
                            'name': table_name,
                            'row_count': f'Error: {str(e)}'
                        })
            
            return jsonify({
                'success': True,
                'tables': user_tables,
                'total_tables': len(user_tables)
            })
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/create', methods=['POST'])
    def api_create_database():
        """Create a new database."""
        try:
            data = request.get_json()
            database_name = data.get('name', '').strip()
            
            if not database_name:
                return jsonify({'success': False, 'error': 'Database name is required'})
            
            result = db_management_service.create_database(database_name)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/switch', methods=['POST'])
    def api_switch_database():
        """Switch to a different database."""
        try:
            data = request.get_json()
            database_name = data.get('name', '').strip()
            
            if not database_name:
                return jsonify({'success': False, 'error': 'Database name is required'})
            
            result = db_management_service.switch_database(database_name)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error switching database: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/delete', methods=['POST'])
    def api_delete_database():
        """Delete a database."""
        try:
            data = request.get_json()
            database_name = data.get('name', '').strip()
            confirm = data.get('confirm', False)
            
            if not database_name:
                return jsonify({'success': False, 'error': 'Database name is required'})
            
            result = db_management_service.delete_database(database_name, confirm)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error deleting database: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/list')
    def api_list_databases():
        """List all databases."""
        try:
            databases = db_management_service.list_databases()
            return jsonify({'success': True, 'databases': databases})
            
        except Exception as e:
            logger.error(f"Error listing databases: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/tables/<db_name>')
    def api_list_database_tables(db_name):
        """List all tables in a specific database."""
        try:
            tables = db_management_service.list_tables_in_database(db_name)
            return jsonify({
                'success': True, 
                'tables': tables,
                'database': db_name,
                'total_tables': len(tables)
            })
            
        except Exception as e:
            logger.error(f"Error listing tables in database {db_name}: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/selection-info')
    def api_database_selection_info():
        """Get database selection information for upload interface."""
        try:
            info = db_management_service.get_database_selection_info()
            return jsonify({'success': True, **info})
            
        except Exception as e:
            logger.error(f"Error getting database selection info: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/health-check')
    def api_database_health_check():
        """Run database schema validation and health check."""
        try:
            from services.database_validation_service import run_database_health_check
            health_report = run_database_health_check()
            return jsonify({'success': True, 'health_report': health_report})
            
        except Exception as e:
            logger.error(f"Error running database health check: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/database/fix-schema', methods=['POST'])
    def api_fix_database_schema():
        """Automatically fix missing database columns."""
        try:
            from services.database_validation_service import fix_missing_columns
            fix_results = fix_missing_columns()
            return jsonify({'success': True, 'fix_results': fix_results})
            
        except Exception as e:
            logger.error(f"Error fixing database schema: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/move', methods=['POST'])
    def api_move_table():
        """Move or copy a table to another database with progress tracking."""
        try:
            data = request.get_json()
            table_name = data.get('table_name', '').strip()
            target_database = data.get('target_database', '').strip()
            action = data.get('action', 'move').strip()  # 'move' or 'copy'
            
            if not table_name or not target_database:
                return jsonify({'success': False, 'error': 'Table name and target database are required'})
            
            if action not in ['move', 'copy']:
                return jsonify({'success': False, 'error': 'Action must be "move" or "copy"'})
            
            # Generate operation ID for progress tracking
            operation_id = str(uuid.uuid4())
            
            # Create progress callback
            def progress_callback(progress_data):
                track_progress(operation_id, progress_data)
            
            # Get current app config to pass to the background thread
            app_config = {
                'SQLALCHEMY_DATABASE_URI': app.config.get('SQLALCHEMY_DATABASE_URI'),
                'UPLOAD_FOLDER': app.config.get('UPLOAD_FOLDER')
            }
            
            # Start the move operation in a background thread
            def run_move_operation():
                try:
                    # Use the current app context in the background thread
                    with app.app_context():
                        result = db_management_service.move_table_to_database(
                            table_name, target_database, action, progress_callback, app_config
                        )
                        # Store final result
                        track_progress(operation_id, {
                            'stage': 'complete' if result['success'] else 'error',
                            'percent': 100 if result['success'] else 0,
                            'message': result.get('message', result.get('error', '')),
                            'result': result,
                            'table_name': table_name,
                            'target_database': target_database
                        })
                except Exception as e:
                    track_progress(operation_id, {
                        'stage': 'error',
                        'percent': 0,
                        'message': f'Unexpected error: {str(e)}',
                        'result': {'success': False, 'error': str(e)},
                        'table_name': table_name,
                        'target_database': target_database
                    })
            
            # Start background thread
            thread = Thread(target=run_move_operation)
            thread.daemon = True
            thread.start()
            
            return jsonify({
                'success': True,
                'operation_id': operation_id,
                'message': f'Table {action} operation started',
                'action': action,
                'table_name': table_name,
                'target_database': target_database
            })
            
        except Exception as e:
            logger.error(f"Error starting table move operation: {e}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/progress/<operation_id>', methods=['GET'])
    def get_progress(operation_id):
        """Get progress information for a specific operation"""
        try:
            if operation_id in progress_store:
                progress_data = progress_store[operation_id]['data']
                return jsonify({
                    'success': True,
                    'progress': progress_data
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Operation not found or completed'
                }), 404
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    @app.route('/api/table/rename', methods=['POST'])
    def api_rename_table():
        """Rename a table."""
        try:
            data = request.get_json()
            old_name = data.get('old_name', '').strip()
            new_name = data.get('new_name', '').strip()
            
            if not old_name or not new_name:
                return jsonify({'success': False, 'error': 'Both old and new table names are required'})
            
            result = db_management_service.rename_table(old_name, new_name)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error renaming table: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/duplicate', methods=['POST'])
    def api_duplicate_table():
        """Duplicate a table."""
        try:
            data = request.get_json()
            source_name = data.get('source_name', '').strip()
            target_name = data.get('target_name', '').strip()
            copy_data = data.get('copy_data', True)
            
            if not source_name or not target_name:
                return jsonify({'success': False, 'error': 'Both source and target table names are required'})
            
            result = db_management_service.duplicate_table(source_name, target_name, copy_data)
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error duplicating table: {e}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/table/delete', methods=['POST'])
    def api_delete_table_with_confirmation():
        """Delete a table (with confirmation)."""
        try:
            data = request.get_json()
            logger.info(f"Delete table request data: {data}")
            
            table_name = data.get('table_name', '').strip()
            confirm = data.get('confirm', False)
            
            logger.info(f"Attempting to delete table: {table_name}, confirm: {confirm}")
            
            if not table_name:
                return jsonify({'success': False, 'error': 'Table name is required'})
            
            if not confirm:
                return jsonify({'success': False, 'error': 'Confirmation required for table deletion'})
            
            result = db_management_service.delete_table(table_name, confirm)
            logger.info(f"Delete table result: {result}")
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error deleting table: {e}", exc_info=True)
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/chart-data', methods=['POST'])
    def api_chart_data():
        """Generate chart data for visualization."""
        try:
            data = request.get_json()
            database = data.get('database')
            table = data.get('table')
            x_column = data.get('x_column')
            y_column = data.get('y_column')
            chart_type = data.get('chart_type', 'bar')
            
            if not all([database, table, x_column, y_column]):
                return jsonify({'success': False, 'error': 'Missing required parameters'})
            
            # Get database path
            if database == 'stock':
                db_path = 'instance/Stock.db'
            else:
                db_path = f'instance/{database}.db'
            
            import sqlite3
            import pandas as pd
            
            conn = sqlite3.connect(db_path)
            
            # Get chart data
            query = f'SELECT "{x_column}", "{y_column}" FROM "{table}" WHERE "{x_column}" IS NOT NULL AND "{y_column}" IS NOT NULL LIMIT 100'
            df = pd.read_sql_query(query, conn)
            
            if df.empty:
                conn.close()
                return jsonify({'success': False, 'error': 'No data found for the selected columns'})
            
            # Prepare chart data
            if chart_type in ['pie', 'doughnut']:
                # For pie charts, group by x_column and sum y_column
                grouped = df.groupby(x_column)[y_column].sum().head(10)
                chart_data = {
                    'labels': grouped.index.tolist(),
                    'data': grouped.values.tolist(),
                    'label': f'{y_column} by {x_column}'
                }
            else:
                # For other charts, use data as-is or aggregate if needed
                if len(df) > 20:
                    # If too many data points, group by x_column
                    grouped = df.groupby(x_column)[y_column].mean().head(20)
                    chart_data = {
                        'labels': grouped.index.tolist(),
                        'data': grouped.values.tolist(),
                        'label': f'Average {y_column} by {x_column}'
                    }
                else:
                    chart_data = {
                        'labels': df[x_column].astype(str).tolist(),
                        'data': df[y_column].tolist(),
                        'label': f'{y_column} vs {x_column}'
                    }
            
            # Generate distribution data (top 10 values of x_column)
            distribution_data = None
            try:
                dist_query = f'SELECT "{x_column}", COUNT(*) as count FROM "{table}" GROUP BY "{x_column}" ORDER BY count DESC LIMIT 10'
                dist_df = pd.read_sql_query(dist_query, conn)
                if not dist_df.empty:
                    distribution_data = {
                        'labels': dist_df[x_column].astype(str).tolist(),
                        'data': dist_df['count'].tolist()
                    }
            except:
                pass
            
            # Generate trend data (if x_column looks like a date/time)
            trend_data = None
            try:
                # Simple trend based on row order
                trend_query = f'SELECT ROW_NUMBER() OVER() as row_num, AVG(CAST("{y_column}" AS REAL)) as avg_val FROM "{table}" GROUP BY (ROW_NUMBER() OVER() - 1) / 10 ORDER BY row_num LIMIT 20'
                trend_df = pd.read_sql_query(trend_query, conn)
                if not trend_df.empty:
                    trend_data = {
                        'labels': [f'Group {i+1}' for i in range(len(trend_df))],
                        'data': trend_df['avg_val'].tolist()
                    }
            except:
                pass
            
            conn.close()
            
            return jsonify({
                'success': True,
                'chart_data': chart_data,
                'distribution_data': distribution_data,
                'trend_data': trend_data
            })
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/analyze-table/<table_name>')
    def api_analyze_table_simple(table_name):
        """Simple table analysis for charts."""
        try:
            database = request.args.get('database', 'stock')
            
            # Get database path
            if database == 'stock':
                db_path = 'instance/Stock.db'
            else:
                db_path = f'instance/{database}.db'
            
            import sqlite3
            import os
            
            if not os.path.exists(db_path):
                return jsonify({'success': False, 'error': f'Database {database} not found'})
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get basic table stats
            cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
            total_rows = cursor.fetchone()[0]
            
            # Get column info
            cursor.execute(f'PRAGMA table_info("{table_name}")')
            columns = cursor.fetchall()
            
            numeric_columns = 0
            for col in columns:
                col_type = col[2].upper()
                if any(t in col_type for t in ['INT', 'REAL', 'FLOAT', 'DECIMAL', 'NUMERIC']):
                    numeric_columns += 1
            
            # Get null count estimate
            null_percentage = 0
            if columns:
                first_col = columns[0][1]
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}" WHERE "{first_col}" IS NULL')
                null_count = cursor.fetchone()[0]
                null_percentage = round((null_count / total_rows) * 100, 1) if total_rows > 0 else 0
            
            # Get unique values estimate
            unique_values = 0
            if columns:
                first_col = columns[0][1]
                cursor.execute(f'SELECT COUNT(DISTINCT "{first_col}") FROM "{table_name}"')
                unique_values = cursor.fetchone()[0]
            
            conn.close()
            
            analysis = {
                'total_rows': total_rows,
                'numeric_columns': numeric_columns,
                'null_percentage': null_percentage,
                'unique_values': unique_values
            }
            
            return jsonify({'success': True, 'analysis': analysis})
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/chart-data/<table_name>')
    def api_get_chart_data(table_name):
        """Get data for charts and visualizations"""
        try:
            from services.database_service import DatabaseService
            db_service = DatabaseService()
            
            # Get table info
            table_info = db_service.get_table_info(table_name)
            if not table_info:
                return jsonify({'success': False, 'error': 'Table not found'})
            
            # Get sample data for charts (limit to 100 rows for performance)
            data = db_service.get_table_data(table_name, page=1, per_page=100)
            
            if data and 'data' in data:
                return jsonify({
                    'success': True,
                    'table_name': table_name,
                    'columns': [col['name'] for col in table_info['columns']],
                    'data': data['data'],
                    'total_rows': table_info.get('actual_row_count', 0),
                    'sample_size': len(data['data'])
                })
            else:
                return jsonify({'success': False, 'error': 'No data available'})
                
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)})

    # Calculated Fields API Routes
    @app.route('/api/calculated-fields/columns/<table_name>')
    def api_get_calculated_fields_columns(table_name):
        """Get table columns with data type analysis for calculated fields."""
        try:
            database = request.args.get('database', 'excel_data')
            columns = calculated_fields_service.get_table_columns(table_name, database)
            
            # Get row count
            import sqlite3
            import os
            db_path = os.path.join(calculated_fields_service.instance_folder, f"{database}.db")
            if os.path.exists(db_path):
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                row_count = cursor.fetchone()[0]
                conn.close()
            else:
                row_count = 0
            
            return jsonify({
                'success': True,
                'columns': columns,
                'row_count': row_count,
                'table': table_name,
                'database': database
            })
            
        except Exception as e:
            logger.error(f"Error getting calculated fields columns for {table_name}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/calculated-fields/validate', methods=['POST'])
    def api_validate_formula():
        """Validate a calculated field formula."""
        try:
            data = request.get_json()
            formula = data.get('formula', '')
            table_name = data.get('table', '')
            database = data.get('database', 'excel_data')
            
            if not formula or not table_name:
                return jsonify({'valid': False, 'message': 'Formula and table name are required'})
            
            # Get table columns
            columns = calculated_fields_service.get_table_columns(table_name, database)
            column_names = [col['name'] for col in columns]
            
            # Validate formula
            validation = calculated_fields_service.validate_formula(formula, column_names)
            
            return jsonify(validation)
            
        except Exception as e:
            logger.error(f"Error validating formula: {str(e)}")
            return jsonify({'valid': False, 'message': str(e)})
    
    @app.route('/api/calculated-fields/create', methods=['POST'])
    def api_create_calculated_field():
        """Create a new calculated field."""
        try:
            data = request.get_json()
            table_name = data.get('table_name', '')
            field_name = data.get('field_name', '')
            formula = data.get('formula', '')
            field_type = data.get('field_type', 'REAL')
            database = data.get('database', 'excel_data')
            
            if not all([table_name, field_name, formula]):
                return jsonify({'success': False, 'error': 'Missing required parameters'})
            
            # Create the calculated field
            result = calculated_fields_service.create_calculated_field(
                table_name=table_name,
                field_name=field_name,
                formula=formula,
                field_type=field_type,
                database=database
            )
            
            # Add field_name to the result for the frontend
            if result['success']:
                result['field_name'] = field_name
            
            return jsonify(result)
            
        except Exception as e:
            logger.error(f"Error creating calculated field: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/calculated-fields/functions')
    def api_get_available_functions():
        """Get available functions for calculated fields."""
        try:
            functions = calculated_fields_service.get_available_functions()
            return jsonify({'success': True, 'functions': functions})
            
        except Exception as e:
            logger.error(f"Error getting available functions: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/calculated-fields/examples')
    def api_get_formula_examples():
        """Get formula examples for calculated fields."""
        try:
            examples = calculated_fields_service.get_formula_examples()
            return jsonify({'success': True, 'examples': examples})
            
        except Exception as e:
            logger.error(f"Error getting formula examples: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    # ==========================================
    # LCT STS MAINTENANCE MANAGEMENT ROUTES
    # ==========================================
    
    @app.route('/maintenance-dashboard')
    def maintenance_dashboard():
        """LCT STS Maintenance Management Dashboard."""
        if not MAINTENANCE_SERVICES_AVAILABLE or not maintenance_service:
            flash('Maintenance services are not available. Please ensure all dependencies are installed.', 'warning')
            return redirect(url_for('index'))
            
        try:
            # Get maintenance KPIs
            maintenance_kpis = maintenance_service.get_maintenance_kpis()
            
            # Get recent work orders
            recent_work_orders = work_order_service.get_work_orders(limit=10)
            
            # Get overdue work orders
            overdue_work_orders = work_order_service.get_overdue_work_orders()
            
            # Get critical spare parts
            critical_parts = spare_parts_service.get_critical_spare_parts()
            
            return render_template('maintenance_dashboard.html',
                                 kpis=maintenance_kpis,
                                 recent_work_orders=recent_work_orders,
                                 overdue_work_orders=overdue_work_orders,
                                 critical_parts=critical_parts[:10])  # Top 10 critical parts
            
        except Exception as e:
            logger.error(f"Error loading maintenance dashboard: {str(e)}")
            return render_template('maintenance_dashboard.html',
                                 kpis={}, recent_work_orders=[], 
                                 overdue_work_orders=[], critical_parts=[],
                                 error=str(e))
    
    @app.route('/work-orders')
    @app.route('/work-orders-list')
    def work_orders_list():
        """Work Orders Management Page."""
        try:
            # Check if user wants simple view
            simple_view = request.args.get('simple', 'false').lower() == 'true'
            
            # Get filter parameters
            status = request.args.get('status')
            maintenance_type = request.args.get('type')
            priority = request.args.get('priority')
            page = request.args.get('page', 1, type=int)
            per_page = 25
            
            # Get work orders directly from the database
            workorder_db_path = os.path.join(app.instance_path, 'Workorder.db')
            
            work_orders = []
            stats = {}
            
            if os.path.exists(workorder_db_path):
                import sqlite3
                from datetime import datetime
                conn = sqlite3.connect(workorder_db_path)
                cursor = conn.cursor()
                
                # Use all_cm table which has the most complete data
                query = "SELECT * FROM all_cm WHERE 1=1"
                params = []
                
                # Default to current year for performance unless specific date filters are provided
                date_filter = request.args.get('date_filter')
                if not date_filter:
                    current_year = str(datetime.now().year)
                    query += " AND strftime('%Y', order_date) = ?"
                    params.append(current_year)
                
                # Apply status filter
                if status == 'Completed':
                    query += " AND etatjob IN ('TER', 'Completed', 'Done')"
                elif status == 'Active':
                    query += " AND (etatjob NOT IN ('TER', 'Completed', 'Done') OR etatjob IS NULL)"
                
                # Apply additional filters
                if priority:
                    query += " AND priority_key LIKE ?"
                    params.append(f"%{priority}%")
                
                if maintenance_type:
                    query += " AND (job_type LIKE ? OR cost_purpose_key LIKE ?)"
                    type_param = f"%{maintenance_type}%"
                    params.extend([type_param, type_param])
                
                # Add pagination
                offset = (page - 1) * per_page
                query += f" ORDER BY order_date DESC LIMIT {per_page} OFFSET {offset}"
                
                cursor.execute(query, params)
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                for row in rows:
                    work_order = dict(zip(columns, row))
                    work_orders.append(work_order)
                
                # Get basic statistics
                cursor.execute("SELECT COUNT(*) FROM all_cm WHERE etatjob NOT IN ('TER', 'Completed', 'Done') OR etatjob IS NULL")
                active_count = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM all_cm WHERE etatjob IN ('TER', 'Completed', 'Done')")
                history_count = cursor.fetchone()[0]
                
                stats = {
                    'total_active': active_count,
                    'total_history': history_count,
                    'total_work_orders': active_count + history_count
                }
                
                conn.close()
            
            # Determine which template to use
            template_name = 'work_orders_simple.html' if simple_view else 'work_orders.html'
            
            return render_template(template_name,
                                 work_orders=work_orders,
                                 stats=stats,
                                 current_filters={
                                     'status': status,
                                     'type': maintenance_type,
                                     'priority': priority
                                 },
                                 page=page,
                                 database_source='all_cm')
            
        except Exception as e:
            logger.error(f"Error loading work orders: {str(e)}")
            # Always fall back to simple template on error
            return render_template('work_orders_simple.html',
                                 work_orders=[], stats={},
                                 current_filters={}, page=1,
                                 error=str(e))
    
    # ==========================================
    # MAINTENANCE API ROUTES
    # ==========================================
    
    @app.route('/api/maintenance/kpis')
    def api_maintenance_kpis():
        """Get maintenance KPIs."""
        try:
            kpis = maintenance_service.get_maintenance_kpis()
            return jsonify({'success': True, 'kpis': kpis})
        except Exception as e:
            logger.error(f"Error getting maintenance KPIs: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/analytics', methods=['GET'])
    def api_get_work_order_analytics():
        """Get work order analytics from Workorder database."""
        try:
            # Force connection to Workorder database
            workorder_db_path = os.path.join(app.instance_path, 'Workorder.db')
            if not os.path.exists(workorder_db_path):
                return jsonify({'success': False, 'error': 'Workorder database not found'})
            
            # Connect directly to get analytics
            import sqlite3
            from datetime import datetime, timedelta
            conn = sqlite3.connect(workorder_db_path)
            cursor = conn.cursor()
            
            analytics = {}
            
            # Basic counts
            cursor.execute("SELECT COUNT(*) FROM wo_active")
            analytics['active_count'] = cursor.fetchone()[0]
            analytics['total_active'] = analytics['active_count']  # For template compatibility
            
            cursor.execute("SELECT COUNT(*) FROM wo_history")
            analytics['history_count'] = cursor.fetchone()[0]
            
            # Completed this month (from history table)
            current_date = datetime.now()
            first_day_of_month = current_date.replace(day=1).strftime('%Y-%m-%d')
            try:
                cursor.execute("""
                    SELECT COUNT(*) FROM wo_history 
                    WHERE completion_date >= ? AND completion_date IS NOT NULL
                """, [first_day_of_month])
                analytics['completed_this_month'] = cursor.fetchone()[0]
            except:
                analytics['completed_this_month'] = 0
            
            # Overdue work orders (estimating based on very old active work orders)
            thirty_days_ago = (current_date - timedelta(days=30)).strftime('%Y-%m-%d')
            try:
                cursor.execute("""
                    SELECT COUNT(*) FROM wo_active 
                    WHERE order_date < ? AND order_date IS NOT NULL
                """, [thirty_days_ago])
                analytics['overdue'] = cursor.fetchone()[0]
            except:
                analytics['overdue'] = 0
            
            # Average completion time (mock calculation)
            analytics['avg_completion_time'] = '5.2 days'
            
            # Status distribution from active work orders
            cursor.execute("""
                SELECT status, COUNT(*) 
                FROM wo_active 
                WHERE status IS NOT NULL 
                GROUP BY status 
                ORDER BY COUNT(*) DESC
            """)
            analytics['status_distribution'] = dict(cursor.fetchall())
            
            # Job type distribution
            cursor.execute("""
                SELECT job_type, COUNT(*) 
                FROM wo_active 
                WHERE job_type IS NOT NULL 
                GROUP BY job_type 
                ORDER BY COUNT(*) DESC
            """)
            analytics['job_type_distribution'] = dict(cursor.fetchall())
            
            # Priority distribution
            cursor.execute("""
                SELECT priority_key, COUNT(*) 
                FROM wo_active 
                WHERE priority_key IS NOT NULL 
                GROUP BY priority_key 
                ORDER BY COUNT(*) DESC
            """)
            analytics['priority_distribution'] = dict(cursor.fetchall())
            
            conn.close()
            
            return jsonify({
                'success': True,
                'analytics': analytics
            })
            
        except Exception as e:
            logger.error(f"Error getting work order analytics: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    # ==========================================
    # WORK ORDERS ANALYSIS ROUTES
    # ==========================================
    
    @app.route('/work-orders-analysis')
    def work_orders_analysis():
        """Work Orders Comprehensive Analysis Dashboard - Enhanced Version."""
        return render_template('comprehensive_work_orders_analysis.html',
                             current_db_name=get_current_database_name())
    
    @app.route('/work-orders-analysis-standalone')
    def work_orders_analysis_standalone():
        """Work Orders Analysis - Standalone Simple Version."""
        return render_template('work_orders_standalone.html',
                             current_db_name=get_current_database_name())

    @app.route('/work-orders-analysis-simple')
    def work_orders_analysis_simple():
        """Work Orders Analysis Simple Version."""
        return render_template('work_orders_analysis_simple.html',
                             current_db_name=get_current_database_name())

    @app.route('/work-orders-analysis-full')
    def work_orders_analysis_full():
        """Work Orders Analysis with Charts."""
        return render_template('work_orders_analysis.html',
                             current_db_name=get_current_database_name())

    @app.route('/work-orders-analysis-debug')
    def work_orders_analysis_debug():
        """Work Orders Analysis Debug Page."""
        return render_template('work_orders_analysis_debug.html',
                             current_db_name=get_current_database_name())

    @app.route('/work-orders-test')
    def work_orders_test():
        """Basic work orders test page."""
        return """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Work Orders Test</title>
            <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
            <style>
                body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }
                .card { border: 1px solid #ccc; padding: 20px; margin: 10px 0; border-radius: 5px; background: white; }
                .success { background: #d4edda; border-color: #c3e6cb; color: #155724; }
                .error { background: #f8d7da; border-color: #f5c6cb; color: #721c24; }
                .loading { background: #d1ecf1; border-color: #bee5eb; color: #0c5460; }
                .big-number { font-size: 48px; font-weight: bold; color: #007bff; }
            </style>
        </head>
        <body>
            <h1>🔧 Work Orders Analysis - Live Test</h1>
            <p>Testing direct API connection and data display</p>
            
            <div id="status-card" class="card loading">
                <h3>🔄 Status</h3>
                <p id="status-text">Initializing test...</p>
            </div>
            
            <div id="data-card" class="card" style="display: none;">
                <h3>📊 Work Orders Data</h3>
                <div style="display: flex; gap: 20px; flex-wrap: wrap;">
                    <div>
                        <div class="big-number" id="total-count">-</div>
                        <p><strong>Total Work Orders</strong></p>
                    </div>
                    <div>
                        <div class="big-number" id="completion-rate">-</div>
                        <p><strong>Completion Rate</strong></p>
                    </div>
                    <div>
                        <div class="big-number" id="cp-ratio">-</div>
                        <p><strong>C:P Ratio</strong></p>
                    </div>
                </div>
                
                <h4>Job Types:</h4>
                <div id="job-types-list"></div>
                
                <h4>Top Equipment:</h4>
                <div id="equipment-list"></div>
            </div>
            
            <script>
            function updateStatus(text, isError = false) {
                const statusCard = document.getElementById('status-card');
                const statusText = document.getElementById('status-text');
                statusText.textContent = text;
                statusCard.className = 'card ' + (isError ? 'error' : 'loading');
                console.log('Status:', text);
            }
            
            function showData(data) {
                console.log('Showing data:', data);
                
                // Update KPIs
                document.getElementById('total-count').textContent = data.basic_stats.total_records.toLocaleString();
                document.getElementById('completion-rate').textContent = data.basic_stats.completed_percentage + '%';
                document.getElementById('cp-ratio').textContent = data.performance.corrective_preventive_ratio;
                
                // Job types
                let jobTypesHtml = '<ul>';
                data.job_types.slice(0, 5).forEach(jt => {
                    jobTypesHtml += '<li><strong>' + jt.name + ':</strong> ' + jt.count.toLocaleString() + ' (' + jt.percentage + '%)</li>';
                });
                jobTypesHtml += '</ul>';
                document.getElementById('job-types-list').innerHTML = jobTypesHtml;
                
                // Equipment
                let equipmentHtml = '<ul>';
                data.equipment.slice(0, 5).forEach(eq => {
                    equipmentHtml += '<li><strong>' + eq.equipment + ':</strong> ' + eq.work_orders + ' orders (' + eq.percentage + '%)</li>';
                });
                equipmentHtml += '</ul>';
                document.getElementById('equipment-list').innerHTML = equipmentHtml;
                
                // Show data card
                document.getElementById('status-card').className = 'card success';
                document.getElementById('status-text').textContent = '✅ Data loaded successfully!';
                document.getElementById('data-card').style.display = 'block';
            }
            
            updateStatus('🔍 Testing API connection...');
            
            fetch('/api/work-orders/analysis')
                .then(response => {
                    updateStatus('📡 Got response, status: ' + response.status);
                    if (!response.ok) {
                        throw new Error('HTTP ' + response.status);
                    }
                    return response.json();
                })
                .then(data => {
                    updateStatus('🔄 Processing data...');
                    console.log('API Response:', data);
                    
                    if (data.success) {
                        updateStatus('✅ Analysis data received!');
                        showData(data.analysis);
                    } else {
                        throw new Error('API error: ' + data.error);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    updateStatus('❌ Error: ' + error.message, true);
                });
            </script>
        </body>
        </html>
        """

    @app.route('/work-orders-direct')
    def work_orders_direct():
        """Direct work orders data display using raw database access."""
        try:
            import sqlite3
            import os
            
            # Direct database access
            db_path = os.path.join(app.instance_path, 'Workorder.db')
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get basic statistics
            cursor.execute("SELECT COUNT(*) FROM all_cm")
            total_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM all_cm WHERE etatjob = 'TER'")
            completed_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT etatjob, COUNT(*) FROM all_cm GROUP BY etatjob ORDER BY COUNT(*) DESC")
            status_data = cursor.fetchall()
            
            # Get recent work orders
            cursor.execute("""
                SELECT wo_key, wo_name, equipement, etatjob, order_date, priority_key 
                FROM all_cm 
                WHERE order_date IS NOT NULL 
                ORDER BY order_date DESC 
                LIMIT 10
            """)
            recent_orders = cursor.fetchall()
            
            conn.close()
            
            # Calculate completion percentage
            completion_pct = round((completed_count / total_count) * 100, 2) if total_count > 0 else 0
            
            # Format status distribution
            status_list = [f"{status}: {count:,}" for status, count in status_data]
            
            # Format recent orders HTML
            recent_html = ""
            for order in recent_orders:
                wo_key, wo_name, equipment, status, order_date, priority = order
                recent_html += f"""
                <tr>
                    <td><strong>{wo_key}</strong></td>
                    <td>{equipment or 'N/A'}</td>
                    <td><span class="status-{status.lower()}">{status}</span></td>
                    <td>{priority or 'N/A'}</td>
                    <td>{order_date}</td>
                    <td>{(wo_name or 'No description')[:60]}...</td>
                </tr>
                """
            
            return f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Work Orders Direct Access</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }}
                    .container {{ max-width: 1200px; margin: 0 auto; }}
                    .card {{ background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                    .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; }}
                    .stat-item {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px; border-radius: 6px; text-align: center; }}
                    .stat-number {{ font-size: 1.8em; font-weight: bold; }}
                    .stat-label {{ font-size: 0.9em; opacity: 0.9; }}
                    table {{ width: 100%; border-collapse: collapse; }}
                    th, td {{ padding: 10px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background: #f8f9fa; font-weight: bold; }}
                    .status-ter {{ background: #28a745; color: white; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; }}
                    .status-exe {{ background: #ffc107; color: black; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; }}
                    .status-ini {{ background: #17a2b8; color: white; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; }}
                    .success {{ color: #28a745; font-weight: bold; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>🔧 Work Orders - Direct Database Access</h1>
                    <p class="success">✅ Successfully accessing work order data from Workorder.db</p>
                    
                    <div class="card">
                        <h2>📊 Key Statistics</h2>
                        <div class="stats-grid">
                            <div class="stat-item">
                                <div class="stat-number">{total_count:,}</div>
                                <div class="stat-label">Total Work Orders</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{completed_count:,}</div>
                                <div class="stat-label">Completed Orders</div>
                            </div>
                            <div class="stat-item">
                                <div class="stat-number">{completion_pct}%</div>
                                <div class="stat-label">Completion Rate</div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="card">
                        <h2>📈 Status Distribution</h2>
                        <ul>
                            {chr(10).join([f"<li><strong>{status}:</strong> {count:,} orders</li>" for status, count in status_data[:5]])}
                        </ul>
                    </div>
                    
                    <div class="card">
                        <h2>🕒 Recent Work Orders</h2>
                        <table>
                            <thead>
                                <tr>
                                    <th>WO #</th>
                                    <th>Equipment</th>
                                    <th>Status</th>
                                    <th>Priority</th>
                                    <th>Order Date</th>
                                    <th>Description</th>
                                </tr>
                            </thead>
                            <tbody>
                                {recent_html}
                            </tbody>
                        </table>
                    </div>
                    
                    <div class="card">
                        <h3>🔗 Available Links</h3>
                        <ul>
                            <li><a href="/work-orders">Standard Work Orders Page</a></li>
                            <li><a href="/work-orders-analysis">Work Orders Analysis</a></li>
                            <li><a href="/maintenance-dashboard">Maintenance Dashboard</a></li>
                            <li><a href="/api/work-orders/active">API: Active Work Orders</a></li>
                        </ul>
                    </div>
                </div>
            </body>
            </html>
            """
        except Exception as e:
            import traceback
            return f"""
            <!DOCTYPE html>
            <html>
            <head><title>Work Orders Error</title></head>
            <body style="font-family: Arial; margin: 20px;">
                <h1>❌ Error</h1>
                <div style="background: #ffe6e6; padding: 20px; border-radius: 5px;">
                    <p><strong>Error:</strong> {str(e)}</p>
                    <pre>{traceback.format_exc()}</pre>
                </div>
            </body>
            </html>
            """

    @app.route('/chart-test')
    def chart_test():
        """Chart.js Test Page."""
        return render_template('chart_test.html')
    
    @app.route('/api/work-orders/analysis')
    def api_work_orders_analysis():
        """Get comprehensive work orders analysis."""
        try:
            from services.work_order_analysis_service import WorkOrderAnalysisService
            
            # Initialize service with instance path
            analysis_service = WorkOrderAnalysisService(app.instance_path)
            
            # Get comprehensive analysis
            analysis = analysis_service.get_comprehensive_analysis()
            
            if 'error' in analysis:
                return jsonify({'success': False, 'error': analysis['error']})
            
            return jsonify({'success': True, 'analysis': analysis})
            
        except Exception as e:
            logger.error(f"Error getting work orders analysis: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/search')
    def api_search_work_orders():
        """Search work orders with filters."""
        try:
            from services.work_order_analysis_service import WorkOrderAnalysisService
            
            # Initialize service
            analysis_service = WorkOrderAnalysisService(app.instance_path)
            
            # Get search parameters
            search_term = request.args.get('search', '')
            filters = {
                'job_type': request.args.get('job_type'),
                'priority': request.args.get('priority'),
                'equipment': request.args.get('equipment'),
                'status': request.args.get('status')
            }
            
            # Remove None values
            filters = {k: v for k, v in filters.items() if v}
            
            # Perform search
            work_orders = analysis_service.search_work_orders(search_term, filters)
            
            return jsonify({
                'success': True,
                'work_orders': work_orders[:100],  # Limit to 100 results
                'total': len(work_orders)
            })
            
        except Exception as e:
            logger.error(f"Error searching work orders: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/search', methods=['POST'])
    def api_search_work_orders_post():
        """Search work orders with filters (POST method for complex filters)."""
        try:
            from services.work_order_analysis_service import WorkOrderAnalysisService
            
            # Initialize service
            analysis_service = WorkOrderAnalysisService(app.instance_path)
            
            # Get request data
            data = request.get_json() or {}
            
            search_term = data.get('search', '')
            filters = {
                'etatjob': data.get('etatjob'),
                'work_supplier_key': data.get('work_supplier_key'),
                'job_type': data.get('job_type'),
                'pos_key': data.get('pos_key'),
                'cost_purpose_key': data.get('cost_purpose_key'),
                'inspector': data.get('inspector'),
                'equipment': data.get('equipment'),
                'priority': data.get('priority'),
                'startDate': data.get('startDate'),
                'endDate': data.get('endDate')
            }
            
            # Remove empty values
            filters = {k: v for k, v in filters.items() if v}
            
            # Perform search
            work_orders = analysis_service.search_work_orders(search_term, filters)
            
            return jsonify({
                'success': True,
                'data': work_orders,
                'total': len(work_orders)
            })
            
        except Exception as e:
            logger.error(f"Error searching work orders: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/filter-options')
    def api_work_orders_filter_options():
        """Get filter options for work orders dropdowns."""
        try:
            from services.work_order_analysis_service import WorkOrderAnalysisService
            
            # Initialize service
            analysis_service = WorkOrderAnalysisService(app.instance_path)
            
            # Get filter options
            filter_options = analysis_service.get_filter_options()
            
            return jsonify({
                'success': True,
                'data': filter_options
            })
            
        except Exception as e:
            logger.error(f"Error getting filter options: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/comprehensive-analysis')
    def api_work_orders_comprehensive_analysis():
        """Get comprehensive work orders analysis with all categories."""
        try:
            from services.work_order_analysis_service import WorkOrderAnalysisService
            
            # Initialize service
            analysis_service = WorkOrderAnalysisService(app.instance_path)
            
            # Get comprehensive analysis
            analysis = analysis_service.get_comprehensive_analysis()
            
            if 'error' in analysis:
                return jsonify({'success': False, 'error': analysis['error']})
            
            return jsonify({'success': True, 'data': analysis})
            
        except Exception as e:
            logger.error(f"Error getting comprehensive analysis: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/analytics-summary')
    def api_work_orders_analytics_summary():
        """Get quick analytics summary for dashboard widgets."""
        try:
            from services.work_orders_powerbi_service import WorkOrdersPowerBIService
            
            # Initialize service
            powerbi_service = WorkOrdersPowerBIService(app.instance_path)
            
            # Get quick summary
            summary = powerbi_service.get_quick_summary()
            
            return jsonify({'success': True, 'data': summary})
            
        except Exception as e:
            logger.error(f"Error getting analytics summary: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/powerbi-analysis')
    def api_work_orders_powerbi_analysis():
        """Get PowerBI-style comprehensive analysis."""
        try:
            from services.work_orders_powerbi_service import WorkOrdersPowerBIService
            
            # Initialize service
            powerbi_service = WorkOrdersPowerBIService(app.instance_path)
            
            # Get filters from request
            filters = {}
            for param in request.args:
                if request.args.get(param):
                    filters[param] = request.args.get(param)
            
            # Get comprehensive analysis
            analysis = powerbi_service.get_comprehensive_analysis(filters)
            
            return jsonify(analysis)
            
        except Exception as e:
            logger.error(f"Error getting PowerBI analysis: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/enhanced-work-orders-analysis')
    def enhanced_work_orders_analysis():
        """Enhanced Work Orders Analysis Page with comprehensive filters."""
        return render_template('enhanced_work_orders_analysis.html',
                             current_db_name=get_current_database_name())
    
    @app.route('/api/work-orders/export')
    def api_export_work_orders():
        """Export work orders with filters to CSV or Excel."""
        try:
            from services.work_order_analysis_service import WorkOrderAnalysisService
            from services.work_orders_powerbi_service import WorkOrdersPowerBIService
            import csv
            import io
            from flask import make_response
            
            # Initialize services
            analysis_service = WorkOrderAnalysisService(app.instance_path)
            powerbi_service = WorkOrdersPowerBIService(app.instance_path)
            
            # Get filter parameters
            filters = {}
            for param in ['startDate', 'endDate', 'dateType', 'etatjob', 'work_supplier_key', 'job_type', 
                         'pos_key', 'cost_purpose_key', 'inspector', 'equipment', 'priority', 'search', 'limit']:
                if request.args.get(param):
                    value = request.args.get(param)
                    # Handle multiple selections (comma-separated)
                    if ',' in value:
                        filters[param] = value.split(',')
                    else:
                        filters[param] = value
            
            export_format = request.args.get('format', 'csv').lower()
            
            # Get work orders data
            if filters.get('search'):
                search_filters = {k: v for k, v in filters.items() if k != 'search'}
                work_orders = analysis_service.search_work_orders(filters['search'], search_filters)
            else:
                work_orders = analysis_service.search_work_orders('', filters)
            
            if export_format == 'excel':
                # Use PowerBI service for Excel export
                excel_data = powerbi_service.export_to_excel(work_orders, filters)
                if excel_data:
                    response = make_response(excel_data)
                    response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                    response.headers['Content-Disposition'] = 'attachment; filename=work_orders_analysis.xlsx'
                    return response
                else:
                    # Fallback to CSV
                    export_format = 'csv'
            
            if export_format == 'csv':
                # Create CSV
                output = io.StringIO()
                
                if work_orders:
                    # Enhanced CSV with all available fields
                    fieldnames = [
                        'wo_number', 'description', 'etatjob', 'job_type', 'priority_key',
                        'equipement', 'work_supplier_key', 'cost_purpose_key', 'inspector',
                        'order_date', 'jobexec_dt', 'location', 'area', 'duration_hours'
                    ]
                    
                    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    
                    for wo in work_orders:
                        # Clean up the data for CSV export
                        clean_wo = {}
                        for field in fieldnames:
                            value = wo.get(field, '')
                            # Handle None values and clean strings
                            if value is None:
                                clean_wo[field] = ''
                            elif isinstance(value, str):
                                clean_wo[field] = value.replace(',', ';').replace('\n', ' ').replace('\r', '')
                            else:
                                clean_wo[field] = str(value)
                        writer.writerow(clean_wo)
                
                # Create response
                csv_content = output.getvalue()
                response = make_response(csv_content)
                response.headers['Content-Type'] = 'text/csv; charset=utf-8'
                response.headers['Content-Disposition'] = 'attachment; filename=work_orders_export.csv'
                
                return response
            
            return jsonify({'success': False, 'error': 'Unsupported export format'})
            
        except Exception as e:
            logger.error(f"Error exporting work orders: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders-simple', methods=['GET'])
    def api_work_orders_simple():
        """Simple work orders API endpoint using direct database access."""
        try:
            import sqlite3
            import os
            
            # Get parameters
            limit = request.args.get('limit', 50, type=int)
            status = request.args.get('status')
            equipment = request.args.get('equipment')
            
            # Direct database access
            db_path = os.path.join(app.instance_path, 'Workorder.db')
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Build query
            query = """
                SELECT wo_key, wo_name, equipement, etatjob, order_date, priority_key, 
                       mo_name, inspector, start_dt, end_dt, location
                FROM all_cm 
                WHERE 1=1
            """
            params = []
            
            if status:
                query += " AND etatjob = ?"
                params.append(status)
                
            if equipment:
                query += " AND equipement LIKE ?"
                params.append(f"%{equipment}%")
            
            query += " ORDER BY order_date DESC LIMIT ?"
            params.append(limit)
            
            cursor.execute(query, params)
            work_orders = cursor.fetchall()
            
            # Get summary statistics
            cursor.execute("SELECT COUNT(*) FROM all_cm")
            total_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT etatjob, COUNT(*) FROM all_cm GROUP BY etatjob")
            status_counts = dict(cursor.fetchall())
            
            conn.close()
            
            # Format response
            response_data = {
                'success': True,
                'total_count': total_count,
                'returned_count': len(work_orders),
                'status_summary': status_counts,
                'work_orders': []
            }
            
            for order in work_orders:
                wo_key, wo_name, equipement, etatjob, order_date, priority_key, mo_name, inspector, start_dt, end_dt, location = order
                response_data['work_orders'].append({
                    'wo_key': wo_key,
                    'wo_name': wo_name,
                    'equipment': equipement,
                    'status': etatjob,
                    'order_date': order_date,
                    'priority': priority_key,
                    'mo_name': mo_name,
                    'inspector': inspector,
                    'start_date': start_dt,
                    'end_date': end_dt,
                    'location': location
                })
            
            return jsonify(response_data)
            
        except Exception as e:
            import traceback
            return jsonify({
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }), 500

    @app.route('/api/work-orders', methods=['GET'])
    def api_get_work_orders():
        """Get work orders with optional filtering."""
        try:
            status = request.args.get('status')
            maintenance_type = request.args.get('type')
            priority = request.args.get('priority')
            limit = request.args.get('limit', 50, type=int)
            offset = request.args.get('offset', 0, type=int)
            
            work_orders = work_order_service.get_work_orders(
                status=status,
                maintenance_type=maintenance_type,
                priority=priority,
                limit=limit,
                offset=offset
            )
            
            return jsonify({'success': True, 'work_orders': work_orders})
            
        except Exception as e:
            logger.error(f"Error getting work orders: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/<int:work_order_id>', methods=['GET'])
    def api_get_work_order(work_order_id):
        """Get a specific work order."""
        try:
            work_order = work_order_service.get_work_order_by_id(work_order_id)
            
            if work_order:
                return jsonify({'success': True, 'work_order': work_order})
            else:
                return jsonify({'success': False, 'error': 'Work order not found'})
            
        except Exception as e:
            logger.error(f"Error getting work order {work_order_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/<int:work_order_id>', methods=['PUT'])
    def api_update_work_order(work_order_id):
        """Update a work order."""
        try:
            data = request.get_json()
            
            success = work_order_service.update_work_order(work_order_id, data)
            
            if success:
                return jsonify({'success': True, 'message': 'Work order updated successfully'})
            else:
                return jsonify({'success': False, 'error': 'Failed to update work order'})
            
        except Exception as e:
            logger.error(f"Error updating work order {work_order_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/<int:work_order_id>/complete', methods=['POST'])
    def api_complete_work_order(work_order_id):
        """Complete a work order."""
        try:
            data = request.get_json()
            
            success = work_order_service.complete_work_order(work_order_id, data)
            
            if success:
                return jsonify({'success': True, 'message': 'Work order completed successfully'})
            else:
                return jsonify({'success': False, 'error': 'Failed to complete work order'})
            
        except Exception as e:
            logger.error(f"Error completing work order {work_order_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    # ==========================================
    # MISSING NAVIGATION ROUTES
    # ==========================================
    
    # Add route for maintenance dashboard (already exists, but needs proper route mapping)
    @app.route('/maintenance')
    def maintenance():
        """Redirect to maintenance dashboard."""
        return redirect(url_for('maintenance_dashboard'))
    
    # ==========================================
    # WORK ORDERS POWERBI ANALYSIS ROUTES
    # ==========================================
    
    @app.route('/work-orders-powerbi')
    def work_orders_powerbi():
        """Work Orders PowerBI-style analysis interface."""
        try:
            # Check if user wants the original complex version
            complex_view = request.args.get('complex', 'false').lower() == 'true'
            
            template_name = 'work_orders_powerbi_analysis.html' if complex_view else 'work_orders_powerbi_clean.html'
            
            # For the clean template, provide basic data
            if not complex_view:
                try:
                    from services.work_orders_powerbi_service import WorkOrdersPowerBIService
                    powerbi_service = WorkOrdersPowerBIService(app.instance_path)
                    
                    # Get quick summary for initial load
                    summary_data = powerbi_service.get_quick_summary()
                    
                    return render_template(template_name, 
                                         summary=summary_data,
                                         has_data=True)
                except Exception as data_error:
                    logger.warning(f"Could not load PowerBI data: {data_error}")
                    # Render with empty data structure
                    empty_summary = {
                        'total_count': 0,
                        'active_count': 0,
                        'completed_this_month': 0,
                        'high_priority_count': 0,
                        'equipment_count': 0,
                        'equipment_efficiency': 0,
                        'unique_statuses': [],
                        'unique_priorities': []
                    }
                    return render_template(template_name, 
                                         summary=empty_summary,
                                         has_data=False)
            else:
                return render_template(template_name)
                
        except Exception as e:
            logger.error(f"Error rendering PowerBI analysis page: {str(e)}")
            flash('Error loading PowerBI analysis page, redirecting to simple view', 'warning')
            return redirect(url_for('work_orders_list', simple='true'))
    
    @app.route('/work-orders-powerbi-legacy')
    def work_orders_powerbi_legacy():
        """Original complex PowerBI analysis interface (legacy)."""
        try:
            return render_template('work_orders_powerbi_analysis.html')
        except Exception as e:
            logger.error(f"Error rendering legacy PowerBI analysis page: {str(e)}")
            flash('Error loading legacy PowerBI page, redirecting to clean version', 'warning')
            return redirect(url_for('work_orders_powerbi'))
    
    @app.route('/api/work-orders/powerbi-export')
    def api_work_orders_powerbi_export():
        """Export PowerBI analysis data to Excel."""
        try:
            from services.work_orders_powerbi_service import WorkOrdersPowerBIService
            from flask import make_response
            
            # Initialize service
            powerbi_service = WorkOrdersPowerBIService(app.instance_path)
            
            # Get filters from request parameters
            filters = {}
            filter_params = ['workOrderType', 'status', 'priority', 'jobType', 'equipment', 
                           'category', 'searchTerm', 'startDate', 'endDate', 'dateType']
            
            for param in filter_params:
                value = request.args.get(param)
                if value:
                    filters[param] = value
            
            # Get work orders data
            work_orders = powerbi_service.get_all_work_orders(filters)
            
            # Export to Excel
            excel_data = powerbi_service.export_to_excel(work_orders, filters)
            
            if excel_data:
                response = make_response(excel_data)
                response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                response.headers['Content-Disposition'] = f'attachment; filename=work_orders_powerbi_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                return response
            else:
                return jsonify({'success': False, 'error': 'No data to export'})
                
        except Exception as e:
            logger.error(f"Error exporting PowerBI data: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/equipment-performance')
    def api_work_orders_equipment_performance():
        """Get detailed equipment performance metrics."""
        try:
            from services.work_orders_powerbi_service import WorkOrdersPowerBIService
            
            # Initialize service
            powerbi_service = WorkOrdersPowerBIService(app.instance_path)
            
            # Get equipment ID if specified
            equipment_id = request.args.get('equipment_id')
            
            # Get equipment performance data
            performance_data = powerbi_service.get_equipment_performance(equipment_id)
            
            return jsonify({
                'success': True,
                'equipment_performance': performance_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error getting equipment performance: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/work-orders/maintenance-trends')
    def api_work_orders_maintenance_trends():
        """Get maintenance trends over specified period."""
        try:
            from services.work_orders_powerbi_service import WorkOrdersPowerBIService
            
            # Initialize service
            powerbi_service = WorkOrdersPowerBIService(app.instance_path)
            
            # Get period parameter (default 30 days)
            period_days = int(request.args.get('period_days', 30))
            
            # Get trends data
            trends_data = powerbi_service.get_maintenance_trends(period_days)
            
            return jsonify({
                'success': True,
                'trends': trends_data,
                'period_days': period_days,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error getting maintenance trends: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    @app.route('/api/ai-analysis/equipment-list')
    def api_ai_equipment_list():
        """Get list of equipment with fault data for AI analysis."""
        try:
            from services.ai_fault_analysis_service import AIFaultAnalysisService
            
            # Initialize AI service
            ai_service = AIFaultAnalysisService(app.instance_path)
            
            # Get equipment list
            equipment_list = ai_service.get_equipment_list()
            
            return jsonify({
                'success': True,
                'equipment': equipment_list,
                'total_equipment': len(equipment_list)
            })
            
        except Exception as e:
            logger.error(f"Error getting AI equipment list: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/ai-analysis/equipment/<equipment_id>')
    def api_ai_equipment_analysis(equipment_id):
        """Get AI analysis for specific equipment."""
        try:
            from services.ai_fault_analysis_service import AIFaultAnalysisService
            
            # Initialize AI service
            ai_service = AIFaultAnalysisService(app.instance_path)
            
            # Get fault patterns and insights
            patterns = ai_service.analyze_fault_patterns(equipment_id)
            insights = ai_service.generate_ai_insights(equipment_id)
            
            return jsonify({
                'success': True,
                'equipment_id': equipment_id,
                'fault_patterns': [ai_service._pattern_to_dict(p) for p in patterns],
                'ai_insights': [ai_service._insight_to_dict(i) for i in insights],
                'analysis_timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error in AI equipment analysis for {equipment_id}: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
    
    @app.route('/api/ai-analysis/comprehensive')
    def api_ai_comprehensive_analysis():
        """Get comprehensive AI analysis for all equipment."""
        try:
            from services.ai_fault_analysis_service import AIFaultAnalysisService
            
            # Initialize AI service
            ai_service = AIFaultAnalysisService(app.instance_path)
            
            # Get comprehensive analysis
            analysis = ai_service.get_comprehensive_ai_analysis()
            
            return jsonify({
                'success': True,
                'analysis': analysis,
                'generated_at': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error in comprehensive AI analysis: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})

    # AI Work Order Analysis Routes
    @app.route('/ai-work-order-analysis')
    def ai_work_order_analysis():
        """Display AI work order analysis page."""
        return render_template('ai_work_order_analysis.html')
    
    @app.route('/api/ai-work-order-analysis')
    def api_ai_work_order_analysis():
        """API endpoint for AI work order analysis."""
        try:
            from services.ai_work_order_analysis_service import AIWorkOrderAnalysisService
            
            # Get parameters
            limit = request.args.get('limit', 1000, type=int)
            min_frequency = request.args.get('min_frequency', 3, type=int)
            
            # Initialize service
            service = AIWorkOrderAnalysisService()
            
            # Run analysis
            results = service.run_comprehensive_analysis(limit=limit)
            
            if 'error' in results:
                return jsonify({
                    'success': False,
                    'error': results['error']
                }), 500
            
            # Return results
            return jsonify({
                'success': True,
                'data': results
            })
            
        except Exception as e:
            logger.error(f"Error in AI work order analysis API: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ai-work-order-analysis/export')
    def export_ai_analysis():
        """Export AI analysis results."""
        try:
            from services.ai_work_order_analysis_service import AIWorkOrderAnalysisService
            
            limit = request.args.get('limit', 1000, type=int)
            
            service = AIWorkOrderAnalysisService()
            results = service.run_comprehensive_analysis(limit=limit)
            
            if 'error' in results:
                return jsonify({
                    'success': False,
                    'error': results['error']
                }), 500
            
            # Export to file
            output_file = service.export_analysis_results(results)
            
            if output_file:
                return jsonify({
                    'success': True,
                    'filename': output_file,
                    'message': f'Analysis results exported to {output_file}'
                })
            else:
                return jsonify({
                    'success': False,
                    'error': 'Failed to export results'
                }), 500
                
        except Exception as e:
            logger.error(f"Error exporting AI analysis: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500

    # Enhanced AI Dashboard Routes
    @app.route('/ai-dashboard')
    def ai_dashboard():
        """Display unified AI dashboard page."""
        return render_template('ai_dashboard.html')
    
    @app.route('/api/ai-dashboard')
    def api_ai_dashboard():
        """API endpoint for unified AI dashboard data."""
        try:
            from services.enhanced_ai_service import EnhancedAIAnalysisService
            
            # Initialize enhanced AI service
            service = EnhancedAIAnalysisService(app.instance_path)
            
            # Get unified dashboard data
            dashboard_data = service.get_unified_dashboard_data()
            
            return jsonify({
                'success': True,
                **dashboard_data,
                'generated_at': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error generating AI dashboard data: {str(e)}")
            return jsonify({
                'success': False, 
                'error': str(e)
            }), 500
    
    @app.route('/api/ai-dashboard/export')
    def export_ai_dashboard():
        """Export AI dashboard data to JSON."""
        try:
            from services.enhanced_ai_service import EnhancedAIAnalysisService
            import json
            import os
            from datetime import datetime
            
            # Initialize service
            service = EnhancedAIAnalysisService(app.instance_path)
            
            # Get dashboard data
            dashboard_data = service.get_unified_dashboard_data()
            
            # Create exports directory if it doesn't exist
            exports_dir = os.path.join(app.instance_path, 'exports')
            os.makedirs(exports_dir, exist_ok=True)
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ai_dashboard_export_{timestamp}.json"
            filepath = os.path.join(exports_dir, filename)
            
            # Export data
            export_data = {
                'export_info': {
                    'exported_at': datetime.now().isoformat(),
                    'version': '1.0',
                    'type': 'ai_dashboard_export'
                },
                'dashboard_data': dashboard_data
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            return jsonify({
                'success': True,
                'filename': filename,
                'filepath': filepath,
                'message': f'AI dashboard data exported to {filename}'
            })
            
        except Exception as e:
            logger.error(f"Error exporting AI dashboard: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ai-dashboard/health-scores')
    def api_equipment_health_scores():
        """Get real-time equipment health scores."""
        try:
            from services.enhanced_ai_service import EnhancedAIAnalysisService
            
            service = EnhancedAIAnalysisService(app.instance_path)
            health_data = service.get_equipment_health_scores()
            
            return jsonify({
                'success': True,
                'health_scores': health_data,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error getting health scores: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    @app.route('/api/ai-dashboard/predictions')
    def api_ai_predictions():
        """Get AI predictions for maintenance."""
        try:
            from services.enhanced_ai_service import EnhancedAIAnalysisService
            
            service = EnhancedAIAnalysisService(app.instance_path)
            predictions = service.generate_predictions()
            
            return jsonify({
                'success': True,
                'predictions': predictions,
                'generated_at': datetime.now().isoformat()
            })
            
        except Exception as e:
            logger.error(f"Error generating predictions: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            }), 500
    
    # Health Check and Monitoring Endpoints
    @app.route('/health')
    def health_check():
        """Health check endpoint for monitoring and load balancers."""
        try:
            # Check database connectivity
            with app.app_context():
                db.engine.execute(text('SELECT 1'))
            
            # Check if AI services are available
            ai_status = "operational"
            try:
                from services.enhanced_ai_service import EnhancedAIAnalysisService
                ai_service = EnhancedAIAnalysisService(app.instance_path)
                ai_status = "operational"
            except Exception as e:
                ai_status = f"limited: {str(e)}"
            
            return jsonify({
                'status': 'healthy',
                'timestamp': datetime.now().isoformat(),
                'version': '2.0.0',
                'components': {
                    'database': 'operational',
                    'ai_services': ai_status,
                    'file_upload': 'operational'
                },
                'uptime': time.time() - app.start_time if hasattr(app, 'start_time') else 'unknown'
            }), 200
            
        except Exception as e:
            return jsonify({
                'status': 'unhealthy',
                'timestamp': datetime.now().isoformat(),
                'error': str(e)
            }), 503
    
    @app.route('/metrics')
    def metrics():
        """Basic metrics endpoint for monitoring."""
        try:
            # Get database file sizes
            instance_path = app.instance_path
            db_files = ['Stock.db', 'Workorder.db', 'excel_data.db']
            db_metrics = {}
            
            for db_file in db_files:
                db_path = os.path.join(instance_path, db_file)
                if os.path.exists(db_path):
                    size_mb = os.path.getsize(db_path) / (1024 * 1024)
                    db_metrics[db_file] = f"{size_mb:.2f}MB"
            
            # Get uploads directory size
            uploads_path = os.path.join(app.root_path, 'uploads')
            uploads_size = 0
            if os.path.exists(uploads_path):
                for dirpath, dirnames, filenames in os.walk(uploads_path):
                    for filename in filenames:
                        filepath = os.path.join(dirpath, filename)
                        uploads_size += os.path.getsize(filepath)
            
            return jsonify({
                'timestamp': datetime.now().isoformat(),
                'databases': db_metrics,
                'uploads_size_mb': f"{uploads_size / (1024 * 1024):.2f}MB",
                'instance_path': instance_path
            })
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # Production optimizations
    if not app.debug:
        # Set up production logging
        import logging
        from logging.handlers import RotatingFileHandler
        
        if not os.path.exists('logs'):
            os.mkdir('logs')
        
        file_handler = RotatingFileHandler('logs/sts_app.log', maxBytes=10240000, backupCount=10)
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'
        ))
        file_handler.setLevel(logging.INFO)
        app.logger.addHandler(file_handler)
        app.logger.setLevel(logging.INFO)
        app.logger.info('STS Maintenance App startup')
        
        # Add security headers
        @app.after_request
        def security_headers(response):
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['X-Frame-Options'] = 'DENY'
            response.headers['X-XSS-Protection'] = '1; mode=block'
            return response
    
    # Track app start time
    app.start_time = time.time()
    
    return app

# Create the application
app = create_app()

if __name__ == '__main__':
    # Tables are already created in create_app(), no need to create them again
    app.run(debug=True, host='0.0.0.0', port=5000)
