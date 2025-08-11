"""
Work Orders Analysis Service
Provides comprehensive analysis of the all_cm table (Work Orders data)
"""

import sqlite3
import os
from collections import Counter
from datetime import datetime, timedelta

class WorkOrderAnalysisService:
    def __init__(self, instance_path=None):
        if instance_path:
            self.db_path = os.path.join(instance_path, 'Workorder.db')
        else:
            # Default path
            self.db_path = os.path.join('instance', 'Workorder.db')
        
        # Code mappings for better analysis - Updated with provided categories
        self.code_mappings = {
            'etatjob': {
                'EXE': 'In Execution',
                'INI': 'Initiated',
                'TER': 'Terminated/Completed', 
                'PRT': 'Partially Complete',
                'APC': 'Awaiting Parts/Completion'
            },
            'work_supplier_key': {
                'PAINT': 'Paint Works',
                'ELEC/MEC': 'Electrical/Mechanical',
                'MEC': 'Mechanical',
                'CR': 'Crane Operations',
                'ELEC': 'Electrical',
                'WELD': 'Welding',
                'GLASS': 'Glass Works',
                'Projects': 'Project Works',
                'ICT': 'Information Technology',
                'PSTL': 'Postal Services',
                'UMTS': 'UMTS Systems',
                'AGPS': 'AGPS Systems', 
                'TTL': 'TTL Systems',
                'TEC': 'Technical Services',
                'WHS': 'Warehouse Services',
                'RFR': 'Refrigeration',
                'EPP': 'Equipment Protection',
                'CORPNIKS05': 'Corporate Systems 05',
                'CORPNIKS01': 'Corporate Systems 01'
            },
            'job_type': {
                'C': 'Corrective Maintenance',
                'O': 'Operational',
                'I': 'Inspection',
                'P': 'Preventive Maintenance', 
                'U': 'Unplanned/Urgent',
                'B': 'Breakdown',
                'L': 'Lubrication'
            },
            'pos_key': {
                'STS': 'Ship to Shore',
                'SPR': 'Spreader Systems'
            },
            'cost_purpose_key': {
                'COR': 'Corrective',
                'SUP': 'Support',
                'PROJ': 'Project',
                'PREV': 'Preventive',
                'COND': 'Conditional',
                'IT SUP': 'IT Support',
                'DOM': 'Damage/Dommage',
                'IMP': 'Improvement'
            },
            'inspector': {
                'RELIABILITY': 'Reliability Inspector',
                'EXCECUTION': 'Execution Inspector', 
                'APAVE': 'APAVE Inspector'
            },
            'crane_id': {
                'STS01': 'Ship to Shore Crane 01',
                'STS02': 'Ship to Shore Crane 02',
                'STS03': 'Ship to Shore Crane 03',
                'STS04': 'Ship to Shore Crane 04',
                'STS05': 'Ship to Shore Crane 05',
                'STS06': 'Ship to Shore Crane 06',
                'STS07': 'Ship to Shore Crane 07',
                'STS08': 'Ship to Shore Crane 08',
                'STS09': 'Ship to Shore Crane 09',
                'STS11': 'Ship to Shore Crane 11',
                'STS12': 'Ship to Shore Crane 12'
            },
            'priority': {
                '1-IMM': 'Immediate',
                '2-DAY': 'Within Day',
                '3-WEEK': 'Within Week',
                '4-PLAN': 'Planned',
                '5-GAP': 'Gap/Spare Time',
                '6-ONG': 'Ongoing',
                '1- PR IMM': 'Immediate (High Priority)',
                '2 - PR HIG': 'High Priority',
                '3- PR MED': 'Medium Priority',
                '4 - PR LOW': 'Low Priority'
            },
            'status': {
                'TER': 'Terminated/Completed',
                'INI': 'Initiated',
                'EXE': 'In Execution',
                'PRT': 'Partially Complete',
                'APC': 'Awaiting Parts/Completion'
            },
            'location': {
                'MNH': 'Main Hoist',
                'SPS': 'Spreader System',
                'HDB': 'Head Block',
                'ELE': 'Electrical',
                'GAN': 'Gantry',
                'TRL': 'Trolley',
                'ELV': 'Elevator',
                'BMH': 'Boom Hoist',
                'CAB': 'Cabin',
                'LIG': 'Lighting',
                'STR': 'Structure',
                'TRM': 'Terminal',
                'SLE': 'Slewring',
                'HYD': 'Hydraulic',
                'FES': 'Festoon',
                'HVS': 'High Voltage System',
                'SRC': 'Source',
                'TWL': 'Twist Lock',
                'TLS': 'Tools',
                'CMS': 'CMS System',
                'SCR': 'Screen',
                'FLP': 'Flipper',
                'BCO': 'Boom Cord',
                'ATR': 'Auto Transfer',
                'MTR': 'Motor',
                'LVS': 'Low Voltage System'
            }
        }
    
    def get_database_connection(self):
        """Get connection to Activity database"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Activity database not found at {self.db_path}")
        return sqlite3.connect(self.db_path)
    
    def get_basic_statistics(self):
        """Get basic statistics about work orders"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Total records
        cursor.execute("SELECT COUNT(*) FROM all_cm")
        total_records = cursor.fetchone()[0]
        
        # Date range
        cursor.execute("SELECT MIN(order_date), MAX(order_date) FROM all_cm WHERE order_date IS NOT NULL")
        date_range = cursor.fetchone()
        
        # Active vs completed
        cursor.execute("SELECT etatjob, COUNT(*) FROM all_cm GROUP BY etatjob")
        status_counts = dict(cursor.fetchall())
        
        conn.close()
        
        return {
            'total_records': total_records,
            'date_range': {
                'start': date_range[0] if date_range[0] else None,
                'end': date_range[1] if date_range[1] else None
            },
            'status_distribution': status_counts,
            'completed_percentage': round((status_counts.get('TER', 0) / total_records) * 100, 1) if total_records > 0 else 0
        }
    
    def get_job_type_analysis(self):
        """Analyze job types (maintenance categories)"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT job_type, COUNT(*) as count FROM all_cm GROUP BY job_type ORDER BY count DESC")
        job_types = cursor.fetchall()
        
        # Calculate total for percentages
        total = sum(count for _, count in job_types)
        
        result = []
        for job_type, count in job_types:
            result.append({
                'code': job_type,
                'name': self.code_mappings['job_type'].get(job_type, f'Unknown ({job_type})'),
                'count': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_priority_analysis(self):
        """Analyze priority distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT priority_key, COUNT(*) as count FROM all_cm GROUP BY priority_key ORDER BY count DESC")
        priorities = cursor.fetchall()
        
        total = sum(count for _, count in priorities)
        
        result = []
        for priority, count in priorities:
            result.append({
                'code': priority,
                'name': self.code_mappings['priority'].get(priority, f'Unknown ({priority})'),
                'count': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_equipment_analysis(self):
        """Analyze equipment/asset distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Top equipment by work order count
        cursor.execute("""
            SELECT equipement, COUNT(*) as count, AVG(stop_time) as avg_downtime
            FROM all_cm 
            WHERE equipement IS NOT NULL 
            GROUP BY equipement 
            ORDER BY count DESC 
            LIMIT 20
        """)
        equipment_data = cursor.fetchall()
        
        total_wos = sum(count for _, count, _ in equipment_data)
        
        result = []
        for equipment, count, avg_downtime in equipment_data:
            # Get crane description if it's an STS crane
            equipment_name = self.code_mappings['crane_id'].get(equipment, equipment)
            
            result.append({
                'equipment': equipment,
                'equipment_name': equipment_name,
                'work_orders': count,
                'percentage': round((count / total_wos) * 100, 1) if total_wos > 0 else 0,
                'avg_downtime': round(avg_downtime, 2) if avg_downtime else 0
            })
        
        conn.close()
        return result
    
    def get_supplier_analysis(self):
        """Analyze work supplier distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT work_supplier_key, COUNT(*) as count
            FROM all_cm 
            WHERE work_supplier_key IS NOT NULL 
            GROUP BY work_supplier_key 
            ORDER BY count DESC 
            LIMIT 15
        """)
        suppliers = cursor.fetchall()
        
        total = sum(count for _, count in suppliers)
        
        result = []
        for supplier, count in suppliers:
            result.append({
                'supplier': supplier,
                'supplier_name': self.code_mappings['work_supplier_key'].get(supplier, supplier),
                'work_orders': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_etatjob_analysis(self):
        """Analyze ETATJOB (work order status) distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT etatjob, COUNT(*) as count
            FROM all_cm 
            WHERE etatjob IS NOT NULL 
            GROUP BY etatjob 
            ORDER BY count DESC
        """)
        statuses = cursor.fetchall()
        
        total = sum(count for _, count in statuses)
        
        result = []
        for status, count in statuses:
            result.append({
                'code': status,
                'name': self.code_mappings['etatjob'].get(status, f'Unknown ({status})'),
                'count': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_pos_key_analysis(self):
        """Analyze POS_key distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT pos_key, COUNT(*) as count
            FROM all_cm 
            WHERE pos_key IS NOT NULL 
            GROUP BY pos_key 
            ORDER BY count DESC
        """)
        pos_data = cursor.fetchall()
        
        total = sum(count for _, count in pos_data)
        
        result = []
        for pos, count in pos_data:
            result.append({
                'code': pos,
                'name': self.code_mappings['pos_key'].get(pos, f'Unknown ({pos})'),
                'count': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_inspector_analysis(self):
        """Analyze Inspector distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT inspector, COUNT(*) as count
            FROM all_cm 
            WHERE inspector IS NOT NULL 
            GROUP BY inspector 
            ORDER BY count DESC
        """)
        inspector_data = cursor.fetchall()
        
        total = sum(count for _, count in inspector_data)
        
        result = []
        for inspector, count in inspector_data:
            result.append({
                'inspector': inspector,
                'inspector_name': self.code_mappings['inspector'].get(inspector, inspector),
                'count': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_comprehensive_category_analysis(self):
        """Get analysis for all the new categories"""
        try:
            return {
                'etatjob': self.get_etatjob_analysis(),
                'work_suppliers': self.get_supplier_analysis(), 
                'job_types': self.get_job_type_analysis(),
                'pos_keys': self.get_pos_key_analysis(),
                'cost_purposes': self.get_cost_analysis(),
                'inspectors': self.get_inspector_analysis()
            }
        except Exception as e:
            print(f"Error in get_comprehensive_category_analysis: {str(e)}")
            return {'error': str(e)}
    
    def get_location_analysis(self):
        """Analyze location/area distribution"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Location analysis
        cursor.execute("""
            SELECT location, COUNT(*) as count
            FROM all_cm 
            WHERE location IS NOT NULL 
            GROUP BY location 
            ORDER BY count DESC
        """)
        locations = cursor.fetchall()
        
        # Area analysis
        cursor.execute("""
            SELECT area, COUNT(*) as count
            FROM all_cm 
            WHERE area IS NOT NULL 
            GROUP BY area 
            ORDER BY count DESC
        """)
        areas = cursor.fetchall()
        
        conn.close()
        
        # Map locations to meaningful names
        mapped_locations = []
        for location, count in locations:
            mapped_locations.append({
                'code': location,
                'name': self.code_mappings['location'].get(location, location),
                'count': count
            })
        
        return {
            'locations': mapped_locations,
            'areas': [{'name': area, 'count': count} for area, count in areas]
        }
    
    def get_temporal_analysis(self):
        """Analyze temporal patterns"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Monthly trend for last 12 months
        cursor.execute("""
            SELECT créaannée, créamois, COUNT(*) as count
            FROM all_cm 
            WHERE créaannée IS NOT NULL AND créamois IS NOT NULL
            AND créaannée >= 2023
            GROUP BY créaannée, créamois 
            ORDER BY créaannée, créamois
        """)
        monthly_data = cursor.fetchall()
        
        # Yearly totals
        cursor.execute("""
            SELECT créaannée, COUNT(*) as count
            FROM all_cm 
            WHERE créaannée IS NOT NULL AND créaannée >= 2020
            GROUP BY créaannée 
            ORDER BY créaannée DESC
        """)
        yearly_data = cursor.fetchall()
        
        conn.close()
        
        return {
            'monthly': [{'year': year, 'month': month, 'count': count} for year, month, count in monthly_data],
            'yearly': [{'year': int(year), 'count': count} for year, count in yearly_data]
        }
    
    def get_performance_metrics(self):
        """Calculate key performance indicators"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Corrective vs Preventive ratio
        cursor.execute("SELECT job_type, COUNT(*) FROM all_cm WHERE job_type IN ('C', 'P') GROUP BY job_type")
        cp_data = dict(cursor.fetchall())
        
        corrective = cp_data.get('C', 0)
        preventive = cp_data.get('P', 0)
        cp_ratio = corrective / preventive if preventive > 0 else 0
        
        # Average completion time
        cursor.execute("""
            SELECT AVG(stop_time) as avg_time, 
                   MIN(stop_time) as min_time, 
                   MAX(stop_time) as max_time
            FROM all_cm 
            WHERE stop_time IS NOT NULL AND stop_time > 0
        """)
        time_stats = cursor.fetchone()
        
        # Urgent work order percentage
        cursor.execute("""
            SELECT COUNT(*) as urgent_count
            FROM all_cm 
            WHERE priority_key IN ('1-IMM', '1- PR IMM')
        """)
        urgent_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM all_cm")
        total_count = cursor.fetchone()[0]
        
        urgent_percentage = (urgent_count / total_count) * 100 if total_count > 0 else 0
        
        # Equipment availability (based on downtime)
        cursor.execute("""
            SELECT equipement, SUM(stop_time) as total_downtime, COUNT(*) as wo_count
            FROM all_cm 
            WHERE equipement IS NOT NULL AND stop_time IS NOT NULL AND stop_time > 0
            GROUP BY equipement
            ORDER BY total_downtime DESC
            LIMIT 10
        """)
        downtime_data = cursor.fetchall()
        
        conn.close()
        
        return {
            'corrective_preventive_ratio': round(cp_ratio, 2),
            'corrective_count': corrective,
            'preventive_count': preventive,
            'avg_completion_time': round(time_stats[0], 2) if time_stats[0] else 0,
            'min_completion_time': round(time_stats[1], 2) if time_stats[1] else 0,
            'max_completion_time': round(time_stats[2], 2) if time_stats[2] else 0,
            'urgent_percentage': round(urgent_percentage, 1),
            'urgent_count': urgent_count,
            'total_count': total_count,
            'equipment_downtime': [
                {
                    'equipment': equip,
                    'total_downtime': round(downtime, 2),
                    'work_orders': count
                }
                for equip, downtime, count in downtime_data
            ]
        }
    
    def get_cost_analysis(self):
        """Analyze cost purposes"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Try both possible column names for cost purpose
        try:
            cursor.execute("""
                SELECT cost_purpose_key, COUNT(*) as count
                FROM all_cm 
                WHERE cost_purpose_key IS NOT NULL
                GROUP BY cost_purpose_key 
                ORDER BY count DESC
            """)
            cost_purposes = cursor.fetchall()
        except:
            # Fallback to cost_purpose if cost_purpose_key doesn't exist
            cursor.execute("""
                SELECT cost_purpose, COUNT(*) as count
                FROM all_cm 
                WHERE cost_purpose IS NOT NULL
                GROUP BY cost_purpose 
                ORDER BY count DESC
            """)
            cost_purposes = cursor.fetchall()
        
        total = sum(count for _, count in cost_purposes)
        
        result = []
        for purpose, count in cost_purposes:
            result.append({
                'code': purpose,
                'name': self.code_mappings['cost_purpose_key'].get(purpose, f'Unknown ({purpose})'),
                'count': count,
                'percentage': round((count / total) * 100, 1) if total > 0 else 0
            })
        
        conn.close()
        return result
    
    def get_maintenance_insights(self):
        """Generate maintenance insights and recommendations"""
        metrics = self.get_performance_metrics()
        job_types = self.get_job_type_analysis()
        priorities = self.get_priority_analysis()
        
        insights = []
        
        # Corrective vs Preventive analysis
        if metrics['corrective_preventive_ratio'] > 3:
            insights.append({
                'type': 'warning',
                'title': 'High Corrective Maintenance Ratio',
                'message': f"Corrective to Preventive ratio is {metrics['corrective_preventive_ratio']}:1. Consider increasing preventive maintenance to reduce reactive work.",
                'recommendation': 'Develop more comprehensive preventive maintenance schedules'
            })
        elif metrics['corrective_preventive_ratio'] < 1:
            insights.append({
                'type': 'success',
                'title': 'Good Preventive Maintenance Balance',
                'message': f"Corrective to Preventive ratio is {metrics['corrective_preventive_ratio']}:1. Good balance between planned and reactive maintenance.",
                'recommendation': 'Continue current maintenance strategy'
            })
        
        # Urgent work order analysis
        if metrics['urgent_percentage'] > 50:
            insights.append({
                'type': 'warning',
                'title': 'High Urgent Work Orders',
                'message': f"{metrics['urgent_percentage']}% of work orders are marked as urgent/immediate. This indicates possible planning issues.",
                'recommendation': 'Review maintenance planning and priority assignment criteria'
            })
        
        # Equipment reliability analysis
        if metrics['equipment_downtime']:
            top_downtime_equipment = metrics['equipment_downtime'][0]
            insights.append({
                'type': 'info',
                'title': 'Equipment Reliability Focus',
                'message': f"Equipment {top_downtime_equipment['equipment']} has the highest total downtime ({top_downtime_equipment['total_downtime']} hours).",
                'recommendation': f"Consider focused reliability improvement program for {top_downtime_equipment['equipment']}"
            })
        
        return insights
    
    def get_comprehensive_analysis(self):
        """Get complete work order analysis with all categories"""
        try:
            return {
                'basic_stats': self.get_basic_statistics(),
                'categories': self.get_comprehensive_category_analysis(),
                'job_types': self.get_job_type_analysis(),
                'priorities': self.get_priority_analysis(),
                'equipment': self.get_equipment_analysis(),
                'suppliers': self.get_supplier_analysis(),
                'locations': self.get_location_analysis(),
                'temporal': self.get_temporal_analysis(),
                'performance': self.get_performance_metrics(),
                'cost_analysis': self.get_cost_analysis(),
                'insights': self.get_maintenance_insights()
            }
        except Exception as e:
            print(f"Error in get_comprehensive_analysis: {str(e)}")
            return {'error': str(e)}
    
    def search_work_orders(self, search_term='', filters=None):
        """Search work orders with comprehensive filters based on provided categories"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT *,
            CASE 
                WHEN POS_key = 'STS' THEN SUBSTR(MO_key, 1, 5)
                WHEN POS_key = 'SPR' THEN SUBSTR(MO_key, 1, 6)
                ELSE equipement
            END as calculated_equipment
        FROM all_cm WHERE 1=1
        """
        params = []
        
        if search_term:
            query += """ AND (description LIKE ? OR wo_name LIKE ? OR equipement LIKE ? OR 
                        CASE 
                            WHEN POS_key = 'STS' THEN SUBSTR(MO_key, 1, 5)
                            WHEN POS_key = 'SPR' THEN SUBSTR(MO_key, 1, 6)
                            ELSE equipement
                        END LIKE ?)"""
            search_pattern = f"%{search_term}%"
            params.extend([search_pattern, search_pattern, search_pattern, search_pattern])
        
        if filters:
            # Date range filtering
            if filters.get('startDate') and filters.get('endDate'):
                date_field = filters.get('dateType', 'order_date')
                query += f" AND {date_field} BETWEEN ? AND ?"
                params.extend([filters['startDate'], filters['endDate']])
            
            # ETATJOB filter (Work Order Status)
            if filters.get('etatjob'):
                query += " AND etatjob = ?"
                params.append(filters['etatjob'])
            
            # Work Supplier filter
            if filters.get('work_supplier_key'):
                query += " AND work_supplier_key = ?"
                params.append(filters['work_supplier_key'])
            
            # Job Type filter
            if filters.get('job_type') or filters.get('jobType'):
                job_type = filters.get('job_type') or filters.get('jobType')
                query += " AND job_type = ?"
                params.append(job_type)
            
            # POS Key filter
            if filters.get('pos_key'):
                query += " AND pos_key = ?"
                params.append(filters['pos_key'])
            
            # Cost Purpose filter
            if filters.get('cost_purpose_key'):
                # Try both possible column names
                try:
                    # Check if cost_purpose_key column exists
                    cursor.execute("PRAGMA table_info(all_cm)")
                    columns = [col[1] for col in cursor.fetchall()]
                    if 'cost_purpose_key' in columns:
                        query += " AND cost_purpose_key = ?"
                    else:
                        query += " AND cost_purpose = ?"
                    params.append(filters['cost_purpose_key'])
                except:
                    query += " AND cost_purpose = ?"
                    params.append(filters['cost_purpose_key'])
            
            # Inspector filter
            if filters.get('inspector'):
                query += " AND inspector = ?"
                params.append(filters['inspector'])
            
            # Legacy filters for backward compatibility
            if filters.get('category'):
                query += " AND cost_purpose_key = ?"
                params.append(filters['category'])
            
            if filters.get('priority'):
                query += " AND priority_key = ?"
                params.append(filters['priority'])
            
            if filters.get('equipment'):
                # Use the same equipment logic as filter options
                equipment_condition = """
                AND (
                    (POS_key = 'STS' AND SUBSTR(MO_key, 1, 5) = ?) OR
                    (POS_key = 'SPR' AND SUBSTR(MO_key, 1, 6) = ?) OR
                    (POS_key NOT IN ('STS', 'SPR') AND equipement = ?)
                )
                """
                query += equipment_condition
                equipment_value = filters['equipment']
                params.extend([equipment_value, equipment_value, equipment_value])
            
            if filters.get('status'):
                query += " AND etatjob = ?"
                params.append(filters['status'])
        
        query += " ORDER BY order_date DESC LIMIT 1000"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        # Get column names
        cursor.execute("PRAGMA table_info(all_cm)")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Convert to list of dictionaries
        work_orders = []
        for row in results:
            work_order = dict(zip(columns, row))
            work_orders.append(work_order)
        
        conn.close()
        return work_orders
    
    def get_filter_options(self):
        """Get all available filter options for dropdowns"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Check if table exists and get correct name
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('all_cm', 'allCM')")
        table_result = cursor.fetchone()
        
        if not table_result:
            conn.close()
            return {
                'etatjob': [],
                'work_supplier_key': [],
                'job_type': [],
                'pos_key': [],
                'cost_purpose_key': [],
                'inspector': [],
                'equipment': [],
                'priority': []
            }
        
        table_name = table_result[0]
        filter_options = {}
        
        try:
            # ETATJOB options - check both cases
            try:
                cursor.execute(f"SELECT DISTINCT ETATJOB FROM {table_name} WHERE ETATJOB IS NOT NULL ORDER BY ETATJOB")
                filter_options['etatjob'] = [{'value': row[0], 'label': self.code_mappings['etatjob'].get(row[0], row[0])} for row in cursor.fetchall()]
            except:
                cursor.execute(f"SELECT DISTINCT etatjob FROM {table_name} WHERE etatjob IS NOT NULL ORDER BY etatjob")
                filter_options['etatjob'] = [{'value': row[0], 'label': self.code_mappings['etatjob'].get(row[0], row[0])} for row in cursor.fetchall()]
            
            # Work Supplier options
            cursor.execute(f"SELECT DISTINCT work_supplier_key FROM {table_name} WHERE work_supplier_key IS NOT NULL ORDER BY work_supplier_key")
            filter_options['work_supplier_key'] = [{'value': row[0], 'label': self.code_mappings['work_supplier_key'].get(row[0], row[0])} for row in cursor.fetchall()]
            
            # Job Type options
            cursor.execute(f"SELECT DISTINCT job_type FROM {table_name} WHERE job_type IS NOT NULL ORDER BY job_type")
            filter_options['job_type'] = [{'value': row[0], 'label': self.code_mappings['job_type'].get(row[0], row[0])} for row in cursor.fetchall()]
            
            # POS Key options - check both cases
            try:
                cursor.execute(f"SELECT DISTINCT POS_key FROM {table_name} WHERE POS_key IS NOT NULL ORDER BY POS_key")
                filter_options['pos_key'] = [{'value': row[0], 'label': self.code_mappings['pos_key'].get(row[0], row[0])} for row in cursor.fetchall()]
            except:
                cursor.execute(f"SELECT DISTINCT pos_key FROM {table_name} WHERE pos_key IS NOT NULL ORDER BY pos_key")
                filter_options['pos_key'] = [{'value': row[0], 'label': self.code_mappings['pos_key'].get(row[0], row[0])} for row in cursor.fetchall()]
            
            # Cost Purpose options
            try:
                cursor.execute(f"SELECT DISTINCT cost_purpose_key FROM {table_name} WHERE cost_purpose_key IS NOT NULL ORDER BY cost_purpose_key")
                filter_options['cost_purpose_key'] = [{'value': row[0], 'label': self.code_mappings['cost_purpose_key'].get(row[0], row[0])} for row in cursor.fetchall()]
            except:
                cursor.execute(f"SELECT DISTINCT cost_purpose FROM {table_name} WHERE cost_purpose IS NOT NULL ORDER BY cost_purpose")
                filter_options['cost_purpose_key'] = [{'value': row[0], 'label': self.code_mappings['cost_purpose_key'].get(row[0], row[0])} for row in cursor.fetchall()]
            
            # Inspector options
            cursor.execute(f"SELECT DISTINCT inspector FROM {table_name} WHERE inspector IS NOT NULL ORDER BY inspector")
            filter_options['inspector'] = [{'value': row[0], 'label': self.code_mappings['inspector'].get(row[0], row[0])} for row in cursor.fetchall()]
            
            # Equipment options - extract from MO_key based on POS_key (handle case sensitivity)
            try:
                equipment_query = f"""
                SELECT DISTINCT 
                    CASE 
                        WHEN POS_key = 'STS' THEN SUBSTR(MO_key, 1, 5)
                        WHEN POS_key = 'SPR' THEN SUBSTR(MO_key, 1, 6)
                        ELSE equipement
                    END as equipment_code,
                    COUNT(*) as cnt
                FROM {table_name} 
                WHERE MO_key IS NOT NULL 
                    AND POS_key IN ('STS', 'SPR')
                    AND CASE 
                        WHEN POS_key = 'STS' THEN SUBSTR(MO_key, 1, 5)
                        WHEN POS_key = 'SPR' THEN SUBSTR(MO_key, 1, 6)
                        ELSE equipement
                    END IS NOT NULL
                GROUP BY equipment_code 
                ORDER BY cnt DESC 
                LIMIT 50
                """
                cursor.execute(equipment_query)
                filter_options['equipment'] = [{'value': row[0], 'label': row[0]} for row in cursor.fetchall()]
            except:
                # Fallback to regular equipement column
                cursor.execute(f"SELECT DISTINCT equipement, COUNT(*) as cnt FROM {table_name} WHERE equipement IS NOT NULL GROUP BY equipement ORDER BY cnt DESC LIMIT 20")
                filter_options['equipment'] = [{'value': row[0], 'label': row[0]} for row in cursor.fetchall()]
            
            # Priority options
            cursor.execute(f"SELECT DISTINCT priority_key FROM {table_name} WHERE priority_key IS NOT NULL ORDER BY priority_key")
            filter_options['priority'] = [{'value': row[0], 'label': self.code_mappings['priority'].get(row[0], row[0])} for row in cursor.fetchall()]
            
        except Exception as e:
            print(f"Error in get_filter_options: {str(e)}")
            # Return empty options if there's an error
            filter_options = {
                'etatjob': [],
                'work_supplier_key': [],
                'job_type': [],
                'pos_key': [],
                'cost_purpose_key': [],
                'inspector': [],
                'equipment': [],
                'priority': []
            }
        
        conn.close()
        return filter_options
