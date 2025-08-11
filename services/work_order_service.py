"""
LCT STS Work Order Service
Handles work order management for maintenance operations
"""

from models import db
from sqlalchemy import text, func, create_engine
from datetime import datetime, timedelta
import logging
import os

logger = logging.getLogger(__name__)

class WorkOrderService:
    """Service for work order management and operations."""
    
    def __init__(self):
        """Initialize the work order service."""
        self.active_db_path = None
        self._setup_active_db_path()
    
    def _setup_active_db_path(self):
        """Setup path to Active database."""
        try:
            # Try to get the instance folder path
            from flask import current_app
            instance_folder = current_app.instance_path
            self.active_db_path = os.path.join(instance_folder, 'Workorder.db')
        except:
            # Fallback to relative path
            self.active_db_path = os.path.join('instance', 'Workorder.db')
    
    def _get_active_engine(self):
        """Get engine connected to Active database."""
        if self.active_db_path and os.path.exists(self.active_db_path):
            return create_engine(f'sqlite:///{self.active_db_path}')
        return db.engine  # Fallback to default
    
    def get_work_orders(self, status=None, maintenance_type=None, priority=None, limit=50, offset=0, exclude_completed=False, date_from=None, date_to=None):
        """Get work orders with optional filtering."""
        try:
            query = "SELECT * FROM work_orders WHERE 1=1"
            params = []
            
            if status:
                query += " AND status = ?"
                params.append(status)
            elif exclude_completed:
                query += " AND status != 'Completed'"
            
            if maintenance_type:
                query += " AND maintenance_type = ?"
                params.append(maintenance_type)
            
            if priority:
                query += " AND priority = ?"
                params.append(priority)
            
            if date_from:
                query += " AND DATE(created_date) >= ?"
                params.append(date_from)
            
            if date_to:
                query += " AND DATE(created_date) <= ?"
                params.append(date_to)
            
            query += " ORDER BY created_date DESC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
                
            if offset:
                query += " OFFSET ?"
                params.append(offset)
            
            result = db.session.execute(text(query), params)
            
            work_orders = []
            for row in result.fetchall():
                work_orders.append({
                    'id': row[0],
                    'work_order_number': row[1] if len(row) > 1 else f'WO-{row[0]}',
                    'description': row[2] if len(row) > 2 else '',
                    'maintenance_type': row[3] if len(row) > 3 else 'CM',
                    'priority': row[4] if len(row) > 4 else 'Medium',
                    'status': row[5] if len(row) > 5 else 'Open',
                    'equipment_id': row[6] if len(row) > 6 else None,
                    'created_date': row[7] if len(row) > 7 else None,
                    'scheduled_date': row[8] if len(row) > 8 else None,
                    'completion_date': row[9] if len(row) > 9 else None,
                    'assigned_to': row[10] if len(row) > 10 else None,
                    'estimated_hours': row[11] if len(row) > 11 else 0,
                    'actual_hours': row[12] if len(row) > 12 else 0,
                    'labor_cost': row[13] if len(row) > 13 else 0,
                    'parts_cost': row[14] if len(row) > 14 else 0,
                    'total_cost': row[15] if len(row) > 15 else 0
                })
            
            return work_orders
            
        except Exception as e:
            logger.error(f"Error getting work orders: {str(e)}")
            return []
    
    def get_work_order_by_id(self, work_order_id):
        """Get a specific work order by ID."""
        try:
            result = db.session.execute(
                text("SELECT * FROM work_orders WHERE id = ?"), 
                [work_order_id]
            )
            
            row = result.fetchone()
            if row:
                return {
                    'id': row[0],
                    'work_order_number': row[1] if len(row) > 1 else f'WO-{row[0]}',
                    'description': row[2] if len(row) > 2 else '',
                    'maintenance_type': row[3] if len(row) > 3 else 'CM',
                    'priority': row[4] if len(row) > 4 else 'Medium',
                    'status': row[5] if len(row) > 5 else 'Open',
                    'equipment_id': row[6] if len(row) > 6 else None,
                    'created_date': row[7] if len(row) > 7 else None,
                    'scheduled_date': row[8] if len(row) > 8 else None,
                    'completion_date': row[9] if len(row) > 9 else None,
                    'assigned_to': row[10] if len(row) > 10 else None,
                    'estimated_hours': row[11] if len(row) > 11 else 0,
                    'actual_hours': row[12] if len(row) > 12 else 0,
                    'labor_cost': row[13] if len(row) > 13 else 0,
                    'parts_cost': row[14] if len(row) > 14 else 0,
                    'total_cost': row[15] if len(row) > 15 else 0
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting work order {work_order_id}: {str(e)}")
            return None
    
    def create_work_order(self, work_order_data):
        """Create a new work order."""
        try:
            # Generate work order number
            result = db.session.execute(text("SELECT MAX(id) FROM work_orders"))
            max_id = result.scalar() or 0
            wo_number = f"WO-{max_id + 1:06d}"
            
            query = """
                INSERT INTO work_orders (
                    work_order_number, description, maintenance_type, priority, 
                    status, equipment_id, created_date, scheduled_date, 
                    assigned_to, estimated_hours
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = [
                wo_number,
                work_order_data.get('description', ''),
                work_order_data.get('maintenance_type', 'CM'),
                work_order_data.get('priority', 'Medium'),
                work_order_data.get('status', 'Open'),
                work_order_data.get('equipment_id'),
                datetime.now(),
                work_order_data.get('scheduled_date'),
                work_order_data.get('assigned_to'),
                work_order_data.get('estimated_hours', 0)
            ]
            
            db.session.execute(text(query), params)
            db.session.commit()
            
            logger.info(f"Created work order: {wo_number}")
            return wo_number
            
        except Exception as e:
            logger.error(f"Error creating work order: {str(e)}")
            db.session.rollback()
            return None
    
    def update_work_order(self, work_order_id, update_data):
        """Update an existing work order."""
        try:
            # Build dynamic update query
            update_fields = []
            params = []
            
            for field, value in update_data.items():
                if field in ['description', 'maintenance_type', 'priority', 'status', 
                           'equipment_id', 'scheduled_date', 'completion_date', 
                           'assigned_to', 'estimated_hours', 'actual_hours', 
                           'labor_cost', 'parts_cost', 'total_cost']:
                    update_fields.append(f"{field} = ?")
                    params.append(value)
            
            if not update_fields:
                return False
            
            query = f"UPDATE work_orders SET {', '.join(update_fields)} WHERE id = ?"
            params.append(work_order_id)
            
            result = db.session.execute(text(query), params)
            db.session.commit()
            
            return result.rowcount > 0
            
        except Exception as e:
            logger.error(f"Error updating work order {work_order_id}: {str(e)}")
            db.session.rollback()
            return False
    
    def complete_work_order(self, work_order_id, completion_data):
        """Complete a work order with final details."""
        try:
            update_data = {
                'status': 'Completed',
                'completion_date': datetime.now(),
                'actual_hours': completion_data.get('actual_hours', 0),
                'labor_cost': completion_data.get('labor_cost', 0),
                'parts_cost': completion_data.get('parts_cost', 0)
            }
            
            # Calculate total cost
            labor_cost = float(completion_data.get('labor_cost', 0))
            parts_cost = float(completion_data.get('parts_cost', 0))
            update_data['total_cost'] = labor_cost + parts_cost
            
            return self.update_work_order(work_order_id, update_data)
            
        except Exception as e:
            logger.error(f"Error completing work order {work_order_id}: {str(e)}")
            return False
    
    def get_overdue_work_orders(self):
        """Get all overdue work orders."""
        try:
            result = db.session.execute(text("""
                SELECT * FROM work_orders 
                WHERE scheduled_date < ? 
                AND status NOT IN ('Completed', 'Cancelled')
                ORDER BY scheduled_date ASC
            """), [datetime.now()])
            
            overdue_orders = []
            for row in result.fetchall():
                overdue_orders.append({
                    'id': row[0],
                    'work_order_number': row[1] if len(row) > 1 else f'WO-{row[0]}',
                    'description': row[2] if len(row) > 2 else '',
                    'maintenance_type': row[3] if len(row) > 3 else 'CM',
                    'priority': row[4] if len(row) > 4 else 'Medium',
                    'scheduled_date': row[8] if len(row) > 8 else None,
                    'days_overdue': (datetime.now() - datetime.fromisoformat(str(row[8]))).days if len(row) > 8 and row[8] else 0
                })
            
            return overdue_orders
            
        except Exception as e:
            logger.error(f"Error getting overdue work orders: {str(e)}")
            return []
    
    def get_work_order_statistics(self):
        """Get comprehensive work order statistics."""
        try:
            stats = {}
            
            # Total counts by status
            result = db.session.execute(text("""
                SELECT status, COUNT(*) as count
                FROM work_orders
                GROUP BY status
            """))
            
            stats['by_status'] = {row[0]: row[1] for row in result.fetchall()}
            
            # Total counts by maintenance type
            result = db.session.execute(text("""
                SELECT maintenance_type, COUNT(*) as count
                FROM work_orders
                GROUP BY maintenance_type
            """))
            
            stats['by_type'] = {row[0]: row[1] for row in result.fetchall()}
            
            # Total counts by priority
            result = db.session.execute(text("""
                SELECT priority, COUNT(*) as count
                FROM work_orders
                GROUP BY priority
            """))
            
            stats['by_priority'] = {row[0]: row[1] for row in result.fetchall()}
            
            # Monthly trends (last 12 months)
            result = db.session.execute(text("""
                SELECT 
                    strftime('%Y-%m', created_date) as month,
                    COUNT(*) as count
                FROM work_orders
                WHERE created_date >= date('now', '-12 months')
                GROUP BY strftime('%Y-%m', created_date)
                ORDER BY month
            """))
            
            stats['monthly_trends'] = [
                {'month': row[0], 'count': row[1]} 
                for row in result.fetchall()
            ]
            
            # Average completion time
            result = db.session.execute(text("""
                SELECT AVG(
                    CASE 
                        WHEN completion_date IS NOT NULL AND created_date IS NOT NULL 
                        THEN julianday(completion_date) - julianday(created_date)
                        ELSE NULL 
                    END
                ) as avg_completion_days
                FROM work_orders
                WHERE status = 'Completed'
            """))
            
            avg_days = result.scalar()
            stats['avg_completion_days'] = round(avg_days, 1) if avg_days else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting work order statistics: {str(e)}")
            return {}
    
    def get_upcoming_pm_work_orders(self, days_ahead=30):
        """Get upcoming preventive maintenance work orders."""
        try:
            future_date = datetime.now() + timedelta(days=days_ahead)
            
            result = db.session.execute(text("""
                SELECT * FROM work_orders 
                WHERE maintenance_type = 'PM'
                AND scheduled_date BETWEEN ? AND ?
                AND status NOT IN ('Completed', 'Cancelled')
                ORDER BY scheduled_date ASC
            """), [datetime.now(), future_date])
            
            upcoming_pm = []
            for row in result.fetchall():
                upcoming_pm.append({
                    'id': row[0],
                    'work_order_number': row[1] if len(row) > 1 else f'WO-{row[0]}',
                    'description': row[2] if len(row) > 2 else '',
                    'equipment_id': row[6] if len(row) > 6 else None,
                    'scheduled_date': row[8] if len(row) > 8 else None,
                    'assigned_to': row[10] if len(row) > 10 else None,
                    'estimated_hours': row[11] if len(row) > 11 else 0
                })
            
            return upcoming_pm
            
        except Exception as e:
            logger.error(f"Error getting upcoming PM work orders: {str(e)}")
            return []
    
    def assign_work_order(self, work_order_id, technician):
        """Assign a work order to a technician."""
        try:
            return self.update_work_order(work_order_id, {
                'assigned_to': technician,
                'status': 'Assigned'
            })
            
        except Exception as e:
            logger.error(f"Error assigning work order {work_order_id}: {str(e)}")
            return False
    
    def get_technician_workload(self, technician=None):
        """Get workload for a specific technician or all technicians."""
        try:
            if technician:
                result = db.session.execute(text("""
                    SELECT 
                        assigned_to,
                        COUNT(*) as total_assigned,
                        SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                        SUM(estimated_hours) as total_estimated_hours,
                        SUM(actual_hours) as total_actual_hours
                    FROM work_orders
                    WHERE assigned_to = ?
                    GROUP BY assigned_to
                """), [technician])
            else:
                result = db.session.execute(text("""
                    SELECT 
                        assigned_to,
                        COUNT(*) as total_assigned,
                        SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed,
                        SUM(estimated_hours) as total_estimated_hours,
                        SUM(actual_hours) as total_actual_hours
                    FROM work_orders
                    WHERE assigned_to IS NOT NULL
                    GROUP BY assigned_to
                """))
            
            workload = []
            for row in result.fetchall():
                total_assigned = row[1] or 0
                completed = row[2] or 0
                completion_rate = round((completed / total_assigned * 100), 1) if total_assigned > 0 else 0
                
                workload.append({
                    'technician': row[0],
                    'total_assigned': total_assigned,
                    'completed': completed,
                    'in_progress': total_assigned - completed,
                    'completion_rate': completion_rate,
                    'total_estimated_hours': row[3] or 0,
                    'total_actual_hours': row[4] or 0
                })
            
            return workload if not technician else workload[0] if workload else None
            
        except Exception as e:
            logger.error(f"Error getting technician workload: {str(e)}")
            return [] if not technician else None

    def get_work_order_analytics(self):
        """Get comprehensive work order analytics."""
        try:
            analytics = {}
            
            # Total active work orders
            active_query = "SELECT COUNT(*) FROM work_orders WHERE status IN ('Open', 'In Progress', 'On Hold')"
            result = db.session.execute(text(active_query))
            analytics['total_active'] = result.scalar() or 0
            
            # Completed this month
            completed_query = """
                SELECT COUNT(*) FROM work_orders 
                WHERE status = 'Completed' 
                AND strftime('%Y-%m', completion_date) = strftime('%Y-%m', 'now')
            """
            result = db.session.execute(text(completed_query))
            analytics['completed_this_month'] = result.scalar() or 0
            
            # Overdue work orders
            overdue_query = """
                SELECT COUNT(*) FROM work_orders 
                WHERE status IN ('Open', 'In Progress') 
                AND scheduled_date < date('now')
            """
            result = db.session.execute(text(overdue_query))
            analytics['overdue'] = result.scalar() or 0
            
            # Average completion time
            avg_time_query = """
                SELECT AVG(julianday(completion_date) - julianday(created_date)) as avg_days
                FROM work_orders 
                WHERE status = 'Completed' 
                AND completion_date IS NOT NULL 
                AND created_date IS NOT NULL
            """
            result = db.session.execute(text(avg_time_query))
            avg_days = result.scalar()
            if avg_days:
                analytics['avg_completion_time'] = f"{avg_days:.1f} days"
            else:
                analytics['avg_completion_time'] = "N/A"
            
            # Performance metrics for chart
            performance_query = """
                SELECT 
                    strftime('%Y-%m', created_date) as month,
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'Completed' THEN 1 ELSE 0 END) as completed
                FROM work_orders 
                WHERE created_date >= date('now', '-12 months')
                GROUP BY strftime('%Y-%m', created_date)
                ORDER BY month
            """
            result = db.session.execute(text(performance_query))
            performance_data = []
            for row in result.fetchall():
                performance_data.append({
                    'month': row[0],
                    'total': row[1],
                    'completed': row[2],
                    'completion_rate': round((row[2] / row[1] * 100), 1) if row[1] > 0 else 0
                })
            analytics['performance_data'] = performance_data
            
            # Work order distribution by type
            type_query = """
                SELECT maintenance_type, COUNT(*) as count
                FROM work_orders 
                WHERE created_date >= date('now', '-12 months')
                GROUP BY maintenance_type
            """
            result = db.session.execute(text(type_query))
            type_distribution = []
            for row in result.fetchall():
                type_distribution.append({
                    'type': row[0],
                    'count': row[1]
                })
            analytics['type_distribution'] = type_distribution
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error getting work order analytics: {str(e)}")
            return {
                'total_active': 0,
                'completed_this_month': 0,
                'overdue': 0,
                'avg_completion_time': 'N/A',
                'performance_data': [],
                'type_distribution': []
            }
