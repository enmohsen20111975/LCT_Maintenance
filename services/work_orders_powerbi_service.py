"""
Work Orders PowerBI Analysis Service - Updated
Provides comprehensive PowerBI-style analysis for work orders with advanced filtering and insights
"""

import sqlite3
import os
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import json

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    pd = None

class WorkOrdersPowerBIService:
    def __init__(self, instance_path=None):
        if instance_path:
            self.db_path = os.path.join(instance_path, 'Workorder.db')
        else:
            self.db_path = os.path.join('instance', 'Workorder.db')
    
    def get_database_connection(self):
        """Get database connection"""
        if not os.path.exists(self.db_path):
            raise Exception("Activity database not found")
        return sqlite3.connect(self.db_path)
    
    def get_all_work_orders(self, filters=None, limit=1000):
        """Get all work orders with optional filters and performance optimization"""
        if not PANDAS_AVAILABLE:
            return self._get_all_work_orders_without_pandas(filters, limit)
            
        conn = self.get_database_connection()
        
        # Build query with filters
        query = """
            SELECT 
                id,
                wo_name as wo_number,
                description,
                equipement as equipment,
                etatjob as status,
                priority_key as priority,
                job_type,
                cost_purpose_key as category,
                order_date as created_date,
                jobexec_dt as execution_date,
                CASE 
                    WHEN order_date IS NOT NULL AND jobexec_dt IS NOT NULL 
                    THEN ROUND((julianday(jobexec_dt) - julianday(order_date)) * 24, 2)
                    ELSE NULL 
                END as duration_hours,
                0 as total_cost,
                location,
                work_supplier_key as supplier,
                'Active' as source_table
            FROM all_cm
            WHERE 1=1
        """
        
        params = []
        
        # Default to current year for performance unless date filters are specified
        current_year = datetime.now().year
        has_date_filter = filters and (filters.get('startDate') or filters.get('endDate'))
        
        if not has_date_filter:
            # Default to current year for performance
            query += " AND strftime('%Y', order_date) = ?"
            params.append(str(current_year))
        
        if filters:
            # Work order type filter
            work_order_type = filters.get('workOrderType')
            if work_order_type == 'active':
                query += " AND (etatjob NOT IN ('TER', 'Completed', 'Done') OR etatjob IS NULL)"
            elif work_order_type == 'history':
                query += " AND etatjob IN ('TER', 'Completed', 'Done')"
            
            # Date range filter
            if filters.get('startDate') and filters.get('endDate'):
                date_field = filters.get('dateType', 'order_date')
                # Map frontend field names to database field names
                field_mapping = {
                    'order_date': 'order_date',        # Creation date
                    'created_date': 'order_date',      # Alternative name for creation date
                    'exec_date': 'jobexec_dt',         # Execution date
                    'execution_date': 'jobexec_dt'     # Alternative name for execution date
                }
                db_field = field_mapping.get(date_field, 'order_date')
                query += f" AND {db_field} BETWEEN ? AND ?"
                params.extend([filters['startDate'], filters['endDate']])
            
            # Status filter
            if filters.get('status'):
                query += " AND etatjob = ?"
                params.append(filters['status'])
            
            # Priority filter
            if filters.get('priority'):
                query += " AND priority_key = ?"
                params.append(filters['priority'])
            
            # Job type filter
            if filters.get('jobType'):
                query += " AND job_type = ?"
                params.append(filters['jobType'])
            
            # Equipment filter
            if filters.get('equipment'):
                query += " AND equipement = ?"
                params.append(filters['equipment'])
            
            # Category filter
            if filters.get('category'):
                query += " AND cost_purpose_key = ?"
                params.append(filters['category'])
            
            # Search term
            if filters.get('searchTerm'):
                search_term = f"%{filters['searchTerm']}%"
                query += " AND (description LIKE ? OR wo_name LIKE ? OR equipement LIKE ?)"
                params.extend([search_term, search_term, search_term])
        
        query += " ORDER BY order_date DESC"
        
        # Add limit for performance
        if limit:
            query += f" LIMIT {limit}"
        
        try:
            df = pd.read_sql_query(query, conn, params=params)
            conn.close()
            return df.to_dict('records')
        except Exception as e:
            conn.close()
            print(f"Error in pandas query: {e}")
            # Fallback to manual method
            return self._get_all_work_orders_without_pandas(filters, limit)
    
    def _get_all_work_orders_without_pandas(self, filters=None, limit=1000):
        """Fallback method when pandas is not available"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Build query with filters (same as above)
        query = """
            SELECT 
                id,
                wo_name as wo_number,
                description,
                equipement as equipment,
                etatjob as status,
                priority_key as priority,
                job_type,
                cost_purpose_key as category,
                order_date as created_date,
                jobexec_dt as execution_date,
                CASE 
                    WHEN order_date IS NOT NULL AND jobexec_dt IS NOT NULL 
                    THEN ROUND((julianday(jobexec_dt) - julianday(order_date)) * 24, 2)
                    ELSE NULL 
                END as duration_hours,
                0 as total_cost,
                location,
                work_supplier_key as supplier,
                'Active' as source_table
            FROM all_cm
            WHERE 1=1
        """
        
        params = []
        
        # Default to current year for performance unless date filters are specified
        current_year = datetime.now().year
        has_date_filter = filters and (filters.get('startDate') or filters.get('endDate'))
        
        if not has_date_filter:
            # Default to current year for performance
            query += " AND strftime('%Y', order_date) = ?"
            params.append(str(current_year))
        
        if filters:
            # Apply same filters as pandas version
            work_order_type = filters.get('workOrderType')
            if work_order_type == 'active':
                query += " AND (etatjob NOT IN ('TER', 'Completed', 'Done') OR etatjob IS NULL)"
            elif work_order_type == 'history':
                query += " AND etatjob IN ('TER', 'Completed', 'Done')"
            
            if filters.get('startDate') and filters.get('endDate'):
                date_field = filters.get('dateType', 'order_date')
                # Map frontend field names to database field names
                field_mapping = {
                    'order_date': 'order_date',        # Creation date
                    'created_date': 'order_date',      # Alternative name for creation date
                    'exec_date': 'jobexec_dt',         # Execution date
                    'execution_date': 'jobexec_dt'     # Alternative name for execution date
                }
                db_field = field_mapping.get(date_field, 'order_date')
                query += f" AND {db_field} BETWEEN ? AND ?"
                params.extend([filters['startDate'], filters['endDate']])
            
            if filters.get('status'):
                query += " AND etatjob = ?"
                params.append(filters['status'])
            
            if filters.get('priority'):
                query += " AND priority_key = ?"
                params.append(filters['priority'])
            
            if filters.get('jobType'):
                query += " AND job_type = ?"
                params.append(filters['jobType'])
            
            if filters.get('equipment'):
                query += " AND equipement = ?"
                params.append(filters['equipment'])
            
            if filters.get('category'):
                query += " AND cost_purpose_key = ?"
                params.append(filters['category'])
            
            if filters.get('searchTerm'):
                search_term = f"%{filters['searchTerm']}%"
                query += " AND (description LIKE ? OR wo_name LIKE ? OR equipement LIKE ?)"
                params.extend([search_term, search_term, search_term])
        
        query += " ORDER BY order_date DESC"
        
        # Add limit for performance
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        # Get column names
        columns = [description[0] for description in cursor.description]
        
        # Convert to list of dictionaries
        work_orders = []
        for row in rows:
            work_order = dict(zip(columns, row))
            work_orders.append(work_order)
        
        conn.close()
        return work_orders
    
    def calculate_kpis(self, work_orders):
        """Calculate KPI metrics"""
        if not work_orders:
            return {
                'total_count': 0,
                'active_count': 0,
                'completed_this_month': 0,
                'avg_resolution_time': 0,
                'high_priority_count': 0,
                'equipment_efficiency': 0
            }
        
        if PANDAS_AVAILABLE:
            return self._calculate_kpis_with_pandas(work_orders)
        else:
            return self._calculate_kpis_without_pandas(work_orders)
    
    def _calculate_kpis_with_pandas(self, work_orders):
        """Calculate KPIs using pandas"""
        df = pd.DataFrame(work_orders)
        
        # Total count
        total_count = len(df)
        
        # Active count (not completed)
        active_statuses = ['INI', 'EXE', 'PRT', 'APC']
        active_count = len(df[df['status'].isin(active_statuses)]) if 'status' in df.columns else 0
        
        # Completed this month (based on execution date)
        current_month = datetime.now().strftime('%Y-%m')
        completed_this_month = 0
        if 'execution_date' in df.columns:
            df['execution_month'] = pd.to_datetime(df['execution_date'], errors='coerce').dt.strftime('%Y-%m')
            completed_this_month = len(df[df['execution_month'] == current_month])
        
        # Average resolution time (from creation to execution)
        avg_resolution_time = 0
        if 'duration_hours' in df.columns:
            durations = pd.to_numeric(df['duration_hours'], errors='coerce').dropna()
            avg_resolution_time = durations.mean() if not durations.empty else 0
        
        # High priority count
        high_priority_keywords = ['IMM', 'HIGH', 'URGENT', '1-']
        high_priority_count = 0
        if 'priority' in df.columns:
            priority_series = df['priority'].fillna('').astype(str).str.upper()
            high_priority_count = sum(any(keyword in priority for keyword in high_priority_keywords) 
                                    for priority in priority_series)
        
        # Equipment efficiency (completion rate)
        equipment_efficiency = 85  # Default value, can be calculated based on actual data
        if 'status' in df.columns:
            completed_statuses = ['TER', 'Completed', 'Done']
            completed_count = len(df[df['status'].isin(completed_statuses)])
            equipment_efficiency = (completed_count / total_count * 100) if total_count > 0 else 0
        
        return {
            'total_count': total_count,
            'active_count': active_count,
            'completed_this_month': completed_this_month,
            'avg_resolution_time': round(avg_resolution_time, 1),
            'high_priority_count': high_priority_count,
            'equipment_efficiency': round(equipment_efficiency, 1)
        }
    
    def _calculate_kpis_without_pandas(self, work_orders):
        """Calculate KPIs without pandas"""
        total_count = len(work_orders)
        
        # Active count (not completed)
        active_statuses = ['INI', 'EXE', 'PRT', 'APC']
        active_count = sum(1 for wo in work_orders if wo.get('status') in active_statuses)
        
        # Completed this month (based on execution date)
        current_month = datetime.now().strftime('%Y-%m')
        completed_this_month = 0
        for wo in work_orders:
            execution_date = wo.get('execution_date')
            if execution_date:
                try:
                    date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
                    if date_obj.strftime('%Y-%m') == current_month:
                        completed_this_month += 1
                except:
                    pass
        
        # Average resolution time (from creation to execution)
        durations = []
        for wo in work_orders:
            duration = wo.get('duration_hours')
            if duration:
                try:
                    durations.append(float(duration))
                except:
                    pass
        avg_resolution_time = sum(durations) / len(durations) if durations else 0
        
        # High priority count
        high_priority_keywords = ['IMM', 'HIGH', 'URGENT', '1-']
        high_priority_count = 0
        for wo in work_orders:
            priority = wo.get('priority', '').upper()
            if any(keyword in priority for keyword in high_priority_keywords):
                high_priority_count += 1
        
        # Equipment efficiency (completion rate)
        completed_statuses = ['TER', 'Completed', 'Done']
        completed_count = sum(1 for wo in work_orders if wo.get('status') in completed_statuses)
        equipment_efficiency = (completed_count / total_count * 100) if total_count > 0 else 0
        
        return {
            'total_count': total_count,
            'active_count': active_count,
            'completed_this_month': completed_this_month,
            'avg_resolution_time': round(avg_resolution_time, 1),
            'high_priority_count': high_priority_count,
            'equipment_efficiency': round(equipment_efficiency, 1)
        }
    
    def generate_chart_data(self, work_orders):
        """Generate chart data for PowerBI-style visualizations"""
        if not work_orders:
            return {}
        
        if PANDAS_AVAILABLE:
            return self._generate_chart_data_with_pandas(work_orders)
        else:
            return self._generate_chart_data_without_pandas(work_orders)
    
    def _generate_chart_data_with_pandas(self, work_orders):
        """Generate chart data using pandas"""
        df = pd.DataFrame(work_orders)
        chart_data = {}
        
        # Status distribution
        if 'status' in df.columns:
            status_counts = df['status'].value_counts()
            chart_data['status_distribution'] = {
                'labels': status_counts.index.tolist(),
                'datasets': [{
                    'data': status_counts.values.tolist(),
                    'backgroundColor': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF']
                }]
            }
        
        # Priority distribution
        if 'priority' in df.columns:
            priority_counts = df['priority'].value_counts()
            chart_data['priority_distribution'] = {
                'labels': priority_counts.index.tolist(),
                'datasets': [{
                    'label': 'Work Orders',
                    'data': priority_counts.values.tolist(),
                    'backgroundColor': '#667eea'
                }]
            }
        
        # Equipment distribution (top 10)
        if 'equipment' in df.columns:
            equipment_counts = df['equipment'].value_counts().head(10)
            chart_data['equipment_distribution'] = {
                'labels': equipment_counts.index.tolist(),
                'datasets': [{
                    'label': 'Work Orders',
                    'data': equipment_counts.values.tolist(),
                    'backgroundColor': '#36A2EB'
                }]
            }
        
        # Timeline data (work orders over time - creation date)
        if 'created_date' in df.columns:
            df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
            df['month'] = df['created_date'].dt.strftime('%Y-%m')
            timeline_counts = df['month'].value_counts().sort_index()
            chart_data['timeline_data'] = {
                'labels': timeline_counts.index.tolist(),
                'datasets': [{
                    'label': 'Work Orders Created',
                    'data': timeline_counts.values.tolist(),
                    'borderColor': '#667eea',
                    'backgroundColor': 'rgba(102, 126, 234, 0.1)',
                    'fill': True
                }]
            }
        
        # Execution timeline data (based on execution date)
        if 'execution_date' in df.columns:
            df['execution_date'] = pd.to_datetime(df['execution_date'], errors='coerce')
            df['exec_month'] = df['execution_date'].dt.strftime('%Y-%m')
            exec_timeline_counts = df[df['exec_month'].notna()]['exec_month'].value_counts().sort_index()
            chart_data['execution_timeline_data'] = {
                'labels': exec_timeline_counts.index.tolist(),
                'datasets': [{
                    'label': 'Work Orders Executed',
                    'data': exec_timeline_counts.values.tolist(),
                    'borderColor': '#28a745',
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)',
                    'fill': True
                }]
            }
        
        # Resolution time analysis (from creation to execution)
        if 'duration_hours' in df.columns:
            df['duration_numeric'] = pd.to_numeric(df['duration_hours'], errors='coerce')
            duration_ranges = pd.cut(df['duration_numeric'].dropna(), 
                                   bins=[0, 8, 24, 72, 168, float('inf')], 
                                   labels=['<8h', '8-24h', '1-3d', '3-7d', '>7d'])
            duration_counts = duration_ranges.value_counts()
            chart_data['resolution_time'] = {
                'labels': duration_counts.index.tolist(),
                'datasets': [{
                    'label': 'Work Orders',
                    'data': duration_counts.values.tolist(),
                    'backgroundColor': '#FFCE56'
                }]
            }
        
        # Category distribution
        if 'category' in df.columns:
            category_counts = df['category'].value_counts()
            chart_data['category_distribution'] = {
                'labels': category_counts.index.tolist(),
                'datasets': [{
                    'data': category_counts.values.tolist(),
                    'backgroundColor': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
                }]
            }
        
        return chart_data
    
    def _generate_chart_data_without_pandas(self, work_orders):
        """Generate chart data without pandas"""
        chart_data = {}
        
        # Status distribution
        status_counts = Counter(wo.get('status') for wo in work_orders if wo.get('status'))
        if status_counts:
            chart_data['status_distribution'] = {
                'labels': list(status_counts.keys()),
                'datasets': [{
                    'data': list(status_counts.values()),
                    'backgroundColor': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF']
                }]
            }
        
        # Priority distribution
        priority_counts = Counter(wo.get('priority') for wo in work_orders if wo.get('priority'))
        if priority_counts:
            chart_data['priority_distribution'] = {
                'labels': list(priority_counts.keys()),
                'datasets': [{
                    'label': 'Work Orders',
                    'data': list(priority_counts.values()),
                    'backgroundColor': '#667eea'
                }]
            }
        
        # Equipment distribution (top 10)
        equipment_counts = Counter(wo.get('equipment') for wo in work_orders if wo.get('equipment'))
        top_equipment = dict(equipment_counts.most_common(10))
        if top_equipment:
            chart_data['equipment_distribution'] = {
                'labels': list(top_equipment.keys()),
                'datasets': [{
                    'label': 'Work Orders',
                    'data': list(top_equipment.values()),
                    'backgroundColor': '#36A2EB'
                }]
            }
        
        # Timeline data (work orders over time - creation date)
        monthly_counts = defaultdict(int)
        for wo in work_orders:
            created_date = wo.get('created_date')
            if created_date:
                try:
                    date_obj = datetime.strptime(created_date, '%Y-%m-%d')
                    month_key = date_obj.strftime('%Y-%m')
                    monthly_counts[month_key] += 1
                except:
                    pass
        
        if monthly_counts:
            sorted_months = sorted(monthly_counts.keys())
            chart_data['timeline_data'] = {
                'labels': sorted_months,
                'datasets': [{
                    'label': 'Work Orders Created',
                    'data': [monthly_counts[month] for month in sorted_months],
                    'borderColor': '#667eea',
                    'backgroundColor': 'rgba(102, 126, 234, 0.1)',
                    'fill': True
                }]
            }
        
        # Execution timeline data (based on execution date)
        exec_monthly_counts = defaultdict(int)
        for wo in work_orders:
            execution_date = wo.get('execution_date')
            if execution_date:
                try:
                    date_obj = datetime.strptime(execution_date, '%Y-%m-%d')
                    month_key = date_obj.strftime('%Y-%m')
                    exec_monthly_counts[month_key] += 1
                except:
                    pass
        
        if exec_monthly_counts:
            sorted_exec_months = sorted(exec_monthly_counts.keys())
            chart_data['execution_timeline_data'] = {
                'labels': sorted_exec_months,
                'datasets': [{
                    'label': 'Work Orders Executed',
                    'data': [exec_monthly_counts[month] for month in sorted_exec_months],
                    'borderColor': '#28a745',
                    'backgroundColor': 'rgba(40, 167, 69, 0.1)',
                    'fill': True
                }]
            }
        
        # Resolution time analysis (from creation to execution)
        duration_ranges = {'<8h': 0, '8-24h': 0, '1-3d': 0, '3-7d': 0, '>7d': 0}
        for wo in work_orders:
            duration = wo.get('duration_hours')
            if duration:
                try:
                    hours = float(duration)
                    if hours < 8:
                        duration_ranges['<8h'] += 1
                    elif hours < 24:
                        duration_ranges['8-24h'] += 1
                    elif hours < 72:
                        duration_ranges['1-3d'] += 1
                    elif hours < 168:
                        duration_ranges['3-7d'] += 1
                    else:
                        duration_ranges['>7d'] += 1
                except:
                    pass
        
        if any(duration_ranges.values()):
            chart_data['resolution_time'] = {
                'labels': list(duration_ranges.keys()),
                'datasets': [{
                    'label': 'Work Orders',
                    'data': list(duration_ranges.values()),
                    'backgroundColor': '#FFCE56'
                }]
            }
        
        # Category distribution
        category_counts = Counter(wo.get('category') for wo in work_orders if wo.get('category'))
        if category_counts:
            chart_data['category_distribution'] = {
                'labels': list(category_counts.keys()),
                'datasets': [{
                    'data': list(category_counts.values()),
                    'backgroundColor': ['#FF6384', '#36A2EB', '#FFCE56', '#4BC0C0', '#9966FF', '#FF9F40']
                }]
            }
        
        return chart_data
    
    def get_comprehensive_analysis(self, filters=None, limit=5000):
        """Get complete PowerBI-style analysis with performance optimization"""
        try:
            # Get work orders data with limit for performance
            work_orders = self.get_all_work_orders(filters, limit)
            
            # Calculate KPIs
            kpis = self.calculate_kpis(work_orders)
            
            # Generate chart data
            charts = self.generate_chart_data(work_orders)
            
            # Generate insights
            insights = self.generate_insights(work_orders, kpis)
            
            return {
                'success': True,
                'work_orders': work_orders,
                'kpis': kpis,
                'charts': charts,
                'insights': insights,
                'filters_applied': filters or {},
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def generate_insights(self, work_orders, kpis):
        """Generate actionable insights from work orders data"""
        if not work_orders:
            return []
        
        insights = []
        
        # High priority analysis
        if kpis['high_priority_count'] > kpis['total_count'] * 0.3:
            insights.append({
                'type': 'warning',
                'title': 'High Priority Work Orders Alert',
                'message': f"{kpis['high_priority_count']} work orders ({kpis['high_priority_count']/kpis['total_count']*100:.1f}%) are high priority",
                'recommendation': 'Review maintenance planning to reduce urgent work'
            })
        
        # Equipment analysis
        equipment_counts = Counter(wo.get('equipment') for wo in work_orders if wo.get('equipment'))
        if equipment_counts:
            top_equipment = equipment_counts.most_common(1)[0]
            equipment_name = top_equipment[0]
            equipment_count = top_equipment[1]
            insights.append({
                'type': 'info',
                'title': 'Equipment Focus Required',
                'message': f"{equipment_name} has {equipment_count} work orders",
                'recommendation': f'Consider preventive maintenance program for {equipment_name}'
            })
        
        # Efficiency analysis
        if kpis['equipment_efficiency'] < 70:
            insights.append({
                'type': 'warning',
                'title': 'Low Completion Rate',
                'message': f"Equipment efficiency is at {kpis['equipment_efficiency']}%",
                'recommendation': 'Review work order completion processes'
            })
        elif kpis['equipment_efficiency'] > 90:
            insights.append({
                'type': 'success',
                'title': 'Excellent Performance',
                'message': f"Equipment efficiency is at {kpis['equipment_efficiency']}%",
                'recommendation': 'Maintain current excellent performance standards'
            })
        
        # Resolution time analysis
        if kpis['avg_resolution_time'] > 48:
            insights.append({
                'type': 'warning',
                'title': 'Long Resolution Times',
                'message': f"Average resolution time is {kpis['avg_resolution_time']} hours",
                'recommendation': 'Optimize maintenance processes and resource allocation'
            })
        
        return insights
    
    def export_to_excel(self, work_orders, filters=None):
        """Export work orders data to Excel format"""
        if not work_orders:
            return None
        
        try:
            if PANDAS_AVAILABLE:
                return self._export_to_excel_with_pandas(work_orders, filters)
            else:
                return self._export_to_csv(work_orders, filters)
        except ImportError:
            # Fallback to CSV if openpyxl is not available
            return self._export_to_csv(work_orders, filters)
    
    def _export_to_excel_with_pandas(self, work_orders, filters=None):
        """Export using pandas and openpyxl"""
        df = pd.DataFrame(work_orders)
        
        # Create Excel file in memory
        from io import BytesIO
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name='Work Orders', index=False)
            
            # Summary sheet
            summary_data = {
                'Metric': ['Total Work Orders', 'Active Work Orders', 'Completed Work Orders', 'Average Resolution Time (hours)'],
                'Value': [
                    len(df),
                    len(df[df['status'].isin(['INI', 'EXE', 'PRT', 'APC']) if 'status' in df.columns else []]),
                    len(df[df['status'].isin(['TER', 'Completed', 'Done']) if 'status' in df.columns else []]),
                    df['duration'].mean() if 'duration' in df.columns and df['duration'].notna().any() else 0
                ]
            }
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Filters applied sheet
            if filters:
                filters_data = pd.DataFrame(list(filters.items()), columns=['Filter', 'Value'])
                filters_data.to_excel(writer, sheet_name='Filters Applied', index=False)
        
        output.seek(0)
        return output.getvalue()
    
    def get_quick_summary(self):
        """Get quick summary statistics without loading all data (current year only for performance)"""
        try:
            conn = self.get_database_connection()
            cursor = conn.cursor()
            
            # Current year for performance optimization
            current_year = str(datetime.now().year)
            
            # Get basic counts (current year only)
            cursor.execute("SELECT COUNT(*) FROM all_cm WHERE strftime('%Y', order_date) = ?", [current_year])
            total_count = cursor.fetchone()[0]
            
            # Get active count (current year only)
            cursor.execute("SELECT COUNT(*) FROM all_cm WHERE (etatjob NOT IN ('TER', 'Completed', 'Done') OR etatjob IS NULL) AND strftime('%Y', order_date) = ?", [current_year])
            active_count = cursor.fetchone()[0]
            
            # Get completed this month (based on execution date)
            current_month = datetime.now().strftime('%Y-%m')
            cursor.execute("SELECT COUNT(*) FROM all_cm WHERE etatjob IN ('TER', 'Completed', 'Done') AND strftime('%Y-%m', jobexec_dt) = ? AND strftime('%Y', order_date) = ?", [current_month, current_year])
            completed_this_month = cursor.fetchone()[0]
            
            # Get high priority count (current year only)
            cursor.execute("SELECT COUNT(*) FROM all_cm WHERE (priority_key LIKE '%IMM%' OR priority_key LIKE '%HIGH%' OR priority_key LIKE '%1-%') AND strftime('%Y', order_date) = ?", [current_year])
            high_priority_count = cursor.fetchone()[0]
            
            # Get unique equipment count (current year only)
            cursor.execute("SELECT COUNT(DISTINCT equipement) FROM all_cm WHERE equipement IS NOT NULL AND strftime('%Y', order_date) = ?", [current_year])
            equipment_count = cursor.fetchone()[0]
            
            # Get unique statuses (current year only)
            cursor.execute("SELECT DISTINCT etatjob FROM all_cm WHERE etatjob IS NOT NULL AND strftime('%Y', order_date) = ? ORDER BY etatjob", [current_year])
            statuses = [row[0] for row in cursor.fetchall()]
            
            # Get unique priorities (current year only)
            cursor.execute("SELECT DISTINCT priority_key FROM all_cm WHERE priority_key IS NOT NULL AND strftime('%Y', order_date) = ? ORDER BY priority_key", [current_year])
            priorities = [row[0] for row in cursor.fetchall()]
            
            conn.close()
            
            return {
                'total_count': total_count,
                'active_count': active_count,
                'completed_this_month': completed_this_month,
                'high_priority_count': high_priority_count,
                'equipment_count': equipment_count,
                'unique_statuses': statuses,
                'unique_priorities': priorities,
                'equipment_efficiency': round((total_count - active_count) / total_count * 100, 1) if total_count > 0 else 0
            }
            
        except Exception as e:
            print(f"Error getting quick summary: {e}")
            return {
                'total_count': 0,
                'active_count': 0,
                'completed_this_month': 0,
                'high_priority_count': 0,
                'equipment_count': 0,
                'unique_statuses': [],
                'unique_priorities': [],
                'equipment_efficiency': 0
            }
    
    def _export_to_csv(self, work_orders, filters=None):
        """Export to CSV format as fallback"""
        import csv
        from io import StringIO, BytesIO
        
        output = StringIO()
        
        if work_orders:
            fieldnames = work_orders[0].keys()
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(work_orders)
        
        # Convert to bytes
        csv_string = output.getvalue()
        output.close()
        
        bytes_output = BytesIO()
        bytes_output.write(csv_string.encode('utf-8'))
        bytes_output.seek(0)
        
        return bytes_output.getvalue()
    
    def search_work_orders(self, search_term, filters=None):
        """Search work orders with advanced filtering"""
        # Add search term to filters
        if not filters:
            filters = {}
        filters['searchTerm'] = search_term
        
        return self.get_all_work_orders(filters)
    
    def get_equipment_performance(self, equipment_id=None):
        """Get detailed equipment performance metrics"""
        conn = self.get_database_connection()
        
        query = """
            SELECT 
                equipement as equipment,
                COUNT(*) as total_work_orders,
                SUM(CASE WHEN etatjob IN ('TER', 'Completed', 'Done') THEN 1 ELSE 0 END) as completed,
                AVG(CASE WHEN duration_hours IS NOT NULL THEN duration_hours ELSE 0 END) as avg_duration,
                SUM(CASE WHEN priority_key LIKE '%IMM%' OR priority_key LIKE '%HIGH%' THEN 1 ELSE 0 END) as urgent_count
            FROM all_cm 
            WHERE equipement IS NOT NULL
        """
        
        params = []
        if equipment_id:
            query += " AND equipement = ?"
            params.append(equipment_id)
        
        query += " GROUP BY equipement ORDER BY total_work_orders DESC"
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        # Calculate performance score
        if not df.empty:
            df['completion_rate'] = (df['completed'] / df['total_work_orders'] * 100).round(1)
            df['performance_score'] = (
                (df['completion_rate'] * 0.4) +
                ((100 - df['avg_duration']) * 0.3) +
                ((100 - df['urgent_count'] / df['total_work_orders'] * 100) * 0.3)
            ).round(1)
        
        return df.to_dict('records')
    
    def get_maintenance_trends(self, period_days=30):
        """Get maintenance trends over specified period"""
        conn = self.get_database_connection()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=period_days)
        
        query = """
            SELECT 
                DATE(order_date) as creation_date,
                DATE(jobexec_dt) as execution_date,
                job_type,
                COUNT(*) as count,
                AVG(CASE 
                    WHEN order_date IS NOT NULL AND jobexec_dt IS NOT NULL 
                    THEN ROUND((julianday(jobexec_dt) - julianday(order_date)) * 24, 2)
                    ELSE NULL 
                END) as avg_duration_hours
            FROM all_cm 
            WHERE order_date BETWEEN ? AND ?
            GROUP BY DATE(order_date), DATE(jobexec_dt), job_type
            ORDER BY creation_date DESC
        """
        
        df = pd.read_sql_query(query, conn, params=[start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])
        conn.close()
        
        return df.to_dict('records')
