"""
LCT STS Maintenance Service
Handles maintenance performance monitoring and analytics
"""

from models import db
from sqlalchemy import text, func
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class MaintenanceService:
    """Service for maintenance performance monitoring and analytics."""
    
    def __init__(self):
        """Initialize the maintenance service."""
        pass
    
    def get_maintenance_kpis(self):
        """Get key performance indicators for maintenance operations."""
        try:
            kpis = {}
            
            # Work Order Statistics
            kpis['work_orders'] = self._get_work_order_stats()
            
            # Maintenance Type Distribution
            kpis['maintenance_types'] = self._get_maintenance_type_distribution()
            
            # Equipment Performance
            kpis['equipment_performance'] = self._get_equipment_performance()
            
            # Spare Parts Utilization
            kpis['spare_parts'] = self._get_spare_parts_stats()
            
            # Cost Analysis
            kpis['cost_analysis'] = self._get_maintenance_costs()
            
            return kpis
            
        except Exception as e:
            logger.error(f"Error getting maintenance KPIs: {str(e)}")
            return {}
    
    def _get_work_order_stats(self):
        """Get work order statistics."""
        try:
            stats = {}
            
            # Total work orders
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders"))
            stats['total'] = result.scalar() or 0
            
            # Open work orders
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE status IN ('Open', 'In Progress')"))
            stats['open'] = result.scalar() or 0
            
            # Completed work orders
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE status = 'Completed'"))
            stats['completed'] = result.scalar() or 0
            
            # Overdue work orders
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE scheduled_date < ? AND status != 'Completed'"), [datetime.now()])
            stats['overdue'] = result.scalar() or 0
            
            # Completion rate
            if stats['total'] > 0:
                stats['completion_rate'] = round((stats['completed'] / stats['total']) * 100, 1)
            else:
                stats['completion_rate'] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting work order stats: {str(e)}")
            return {'total': 0, 'open': 0, 'completed': 0, 'overdue': 0, 'completion_rate': 0}
    
    def _get_maintenance_type_distribution(self):
        """Get distribution of maintenance types (PM, CM, Breakdown)."""
        try:
            distribution = {}
            
            # Preventive Maintenance
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE maintenance_type = 'PM'"))
            distribution['pm'] = result.scalar() or 0
            
            # Corrective Maintenance
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE maintenance_type = 'CM'"))
            distribution['cm'] = result.scalar() or 0
            
            # Breakdown Maintenance
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE maintenance_type = 'Breakdown'"))
            distribution['breakdown'] = result.scalar() or 0
            
            # Emergency Maintenance
            result = db.session.execute(text("SELECT COUNT(*) FROM work_orders WHERE priority = 'Emergency'"))
            distribution['emergency'] = result.scalar() or 0
            
            return distribution
            
        except Exception as e:
            logger.error(f"Error getting maintenance type distribution: {str(e)}")
            return {'pm': 0, 'cm': 0, 'breakdown': 0, 'emergency': 0}
    
    def _get_equipment_performance(self):
        """Get equipment performance metrics."""
        try:
            performance = {}
            
            # Equipment availability
            try:
                result = db.session.execute(text("""
                    SELECT 
                        AVG(CASE WHEN status = 'Running' THEN 100 ELSE 0 END) as availability
                    FROM equipment
                """))
                performance['availability'] = round(result.scalar() or 0, 1)
            except:
                performance['availability'] = 0
            
            # Mean Time Between Failures (MTBF)
            try:
                result = db.session.execute(text("""
                    SELECT AVG(time_between_failures) as mtbf
                    FROM equipment_metrics
                """))
                performance['mtbf'] = round(result.scalar() or 0, 1)
            except:
                performance['mtbf'] = 0
            
            # Mean Time To Repair (MTTR)
            try:
                result = db.session.execute(text("""
                    SELECT AVG(repair_time_hours) as mttr
                    FROM work_orders 
                    WHERE status = 'Completed' AND repair_time_hours IS NOT NULL
                """))
                performance['mttr'] = round(result.scalar() or 0, 1)
            except:
                performance['mttr'] = 0
            
            return performance
            
        except Exception as e:
            logger.error(f"Error getting equipment performance: {str(e)}")
            return {'availability': 0, 'mtbf': 0, 'mttr': 0}
    
    def _get_spare_parts_stats(self):
        """Get spare parts statistics."""
        try:
            stats = {}
            
            # Total spare parts
            try:
                result = db.session.execute(text("SELECT COUNT(*) FROM spare_parts"))
                stats['total'] = result.scalar() or 0
            except:
                try:
                    result = db.session.execute(text("SELECT COUNT(*) FROM Stock"))
                    stats['total'] = result.scalar() or 0
                except:
                    stats['total'] = 0
            
            # Critical stock levels
            try:
                result = db.session.execute(text("SELECT COUNT(*) FROM spare_parts WHERE quantity_on_hand <= reorder_level"))
                stats['critical'] = result.scalar() or 0
            except:
                stats['critical'] = 0
            
            # Out of stock
            try:
                result = db.session.execute(text("SELECT COUNT(*) FROM spare_parts WHERE quantity_on_hand = 0"))
                stats['out_of_stock'] = result.scalar() or 0
            except:
                stats['out_of_stock'] = 0
            
            # Stock turnover
            try:
                result = db.session.execute(text("""
                    SELECT AVG(annual_usage / NULLIF(quantity_on_hand, 0)) as turnover
                    FROM spare_parts 
                    WHERE quantity_on_hand > 0
                """))
                stats['turnover'] = round(result.scalar() or 0, 2)
            except:
                stats['turnover'] = 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting spare parts stats: {str(e)}")
            return {'total': 0, 'critical': 0, 'out_of_stock': 0, 'turnover': 0}
    
    def _get_maintenance_costs(self):
        """Get maintenance cost analysis."""
        try:
            costs = {}
            
            # Total maintenance costs
            try:
                result = db.session.execute(text("SELECT SUM(total_cost) FROM work_orders WHERE total_cost IS NOT NULL"))
                costs['total'] = round(result.scalar() or 0, 2)
            except:
                costs['total'] = 0
            
            # Labor costs
            try:
                result = db.session.execute(text("SELECT SUM(labor_cost) FROM work_orders WHERE labor_cost IS NOT NULL"))
                costs['labor'] = round(result.scalar() or 0, 2)
            except:
                costs['labor'] = 0
            
            # Parts costs
            try:
                result = db.session.execute(text("SELECT SUM(parts_cost) FROM work_orders WHERE parts_cost IS NOT NULL"))
                costs['parts'] = round(result.scalar() or 0, 2)
            except:
                costs['parts'] = 0
            
            # Monthly average
            try:
                result = db.session.execute(text("""
                    SELECT AVG(monthly_cost) FROM (
                        SELECT SUM(total_cost) as monthly_cost
                        FROM work_orders 
                        WHERE completion_date >= date('now', '-12 months')
                        GROUP BY strftime('%Y-%m', completion_date)
                    )
                """))
                costs['monthly_average'] = round(result.scalar() or 0, 2)
            except:
                costs['monthly_average'] = 0
            
            return costs
            
        except Exception as e:
            logger.error(f"Error getting maintenance costs: {str(e)}")
            return {'total': 0, 'labor': 0, 'parts': 0, 'monthly_average': 0}
    
    def get_maintenance_trends(self, days=30):
        """Get maintenance trends over specified period."""
        try:
            trends = {}
            
            # Work order trends
            result = db.session.execute(text("""
                SELECT 
                    date(created_date) as date,
                    COUNT(*) as count,
                    maintenance_type
                FROM work_orders 
                WHERE created_date >= date('now', '-{} days')
                GROUP BY date(created_date), maintenance_type
                ORDER BY date
            """.format(days)))
            
            trends['work_orders'] = [
                {
                    'date': row[0],
                    'count': row[1],
                    'type': row[2]
                }
                for row in result.fetchall()
            ]
            
            return trends
            
        except Exception as e:
            logger.error(f"Error getting maintenance trends: {str(e)}")
            return {'work_orders': []}
    
    def get_equipment_criticality_analysis(self):
        """Analyze equipment criticality based on maintenance frequency and costs."""
        try:
            result = db.session.execute(text("""
                SELECT 
                    equipment_id,
                    equipment_name,
                    COUNT(*) as work_order_count,
                    SUM(total_cost) as total_cost,
                    AVG(downtime_hours) as avg_downtime
                FROM work_orders wo
                LEFT JOIN equipment e ON wo.equipment_id = e.id
                WHERE wo.created_date >= date('now', '-1 year')
                GROUP BY equipment_id, equipment_name
                ORDER BY work_order_count DESC, total_cost DESC
                LIMIT 20
            """))
            
            return [
                {
                    'equipment_id': row[0],
                    'equipment_name': row[1] or f'Equipment {row[0]}',
                    'work_orders': row[2],
                    'total_cost': row[3] or 0,
                    'avg_downtime': round(row[4] or 0, 1)
                }
                for row in result.fetchall()
            ]
            
        except Exception as e:
            logger.error(f"Error getting equipment criticality analysis: {str(e)}")
            return []
    
    def get_maintenance_schedule_compliance(self):
        """Get preventive maintenance schedule compliance."""
        try:
            result = db.session.execute(text("""
                SELECT 
                    COUNT(*) as total_scheduled,
                    SUM(CASE WHEN completion_date <= scheduled_date THEN 1 ELSE 0 END) as on_time,
                    SUM(CASE WHEN completion_date > scheduled_date THEN 1 ELSE 0 END) as late,
                    SUM(CASE WHEN status != 'Completed' AND scheduled_date < date('now') THEN 1 ELSE 0 END) as overdue
                FROM work_orders 
                WHERE maintenance_type = 'PM' 
                AND scheduled_date >= date('now', '-3 months')
            """))
            
            row = result.fetchone()
            if row:
                total = row[0] or 0
                on_time = row[1] or 0
                late = row[2] or 0
                overdue = row[3] or 0
                
                compliance_rate = round((on_time / total * 100), 1) if total > 0 else 0
                
                return {
                    'total_scheduled': total,
                    'on_time': on_time,
                    'late': late,
                    'overdue': overdue,
                    'compliance_rate': compliance_rate
                }
            
            return {
                'total_scheduled': 0,
                'on_time': 0,
                'late': 0,
                'overdue': 0,
                'compliance_rate': 0
            }
            
        except Exception as e:
            logger.error(f"Error getting maintenance schedule compliance: {str(e)}")
            return {
                'total_scheduled': 0,
                'on_time': 0,
                'late': 0,
                'overdue': 0,
                'compliance_rate': 0
            }
