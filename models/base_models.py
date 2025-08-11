from models import db
from datetime import datetime

class UploadHistory(db.Model):
    __tablename__ = 'upload_history'
    
    id = db.Column(db.Integer, primary_key=True)
    filename = db.Column(db.String(255), nullable=False)
    original_filename = db.Column(db.String(255), nullable=False)
    upload_date = db.Column(db.DateTime, default=datetime.utcnow)
    file_type = db.Column(db.String(20), nullable=True)  # excel, csv, text, pdf
    total_sheets = db.Column(db.Integer, default=0)
    total_records = db.Column(db.Integer, default=0)
    file_size = db.Column(db.Integer, default=0)
    status = db.Column(db.String(50), default='processing')  # processing, completed, failed
    error_message = db.Column(db.Text, nullable=True)
    
    def __repr__(self):
        return f'<UploadHistory {self.original_filename}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'filename': self.filename,
            'original_filename': self.original_filename,
            'upload_date': self.upload_date.isoformat() if self.upload_date else None,
            'file_type': self.file_type,
            'total_sheets': self.total_sheets,
            'total_records': self.total_records,
            'file_size': self.file_size,
            'status': self.status,
            'error_message': self.error_message
        }

class TableMetadata(db.Model):
    __tablename__ = 'table_metadata'
    
    id = db.Column(db.Integer, primary_key=True)
    table_name = db.Column(db.String(255), nullable=False, unique=True)
    original_sheet_name = db.Column(db.String(255), nullable=False)
    upload_id = db.Column(db.Integer, db.ForeignKey('upload_history.id'), nullable=False)
    column_count = db.Column(db.Integer, default=0)
    row_count = db.Column(db.Integer, default=0)
    created_date = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationship
    upload = db.relationship('UploadHistory', backref='tables')
    
    def __repr__(self):
        return f'<TableMetadata {self.table_name}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'table_name': self.table_name,
            'original_sheet_name': self.original_sheet_name,
            'upload_id': self.upload_id,
            'column_count': self.column_count,
            'row_count': self.row_count,
            'created_date': self.created_date.isoformat() if self.created_date else None
        }


# =============================================
# LCT STS MAINTENANCE MODELS
# =============================================

class WorkOrder(db.Model):
    __tablename__ = 'work_orders'
    
    id = db.Column(db.Integer, primary_key=True)
    work_order_number = db.Column(db.String(50), nullable=False, unique=True)
    description = db.Column(db.Text, nullable=False)
    maintenance_type = db.Column(db.String(20), nullable=False)  # PM, CM, Breakdown, Emergency
    priority = db.Column(db.String(20), default='Medium')  # Low, Medium, High, Emergency
    status = db.Column(db.String(20), default='Open')  # Open, In Progress, Completed, On Hold, Cancelled
    equipment_id = db.Column(db.String(50), nullable=True)
    equipment_name = db.Column(db.String(255), nullable=True)
    
    # Dates
    created_date = db.Column(db.DateTime, default=datetime.utcnow)
    scheduled_date = db.Column(db.DateTime, nullable=True)
    start_date = db.Column(db.DateTime, nullable=True)
    completion_date = db.Column(db.DateTime, nullable=True)
    
    # Assignment
    assigned_to = db.Column(db.String(100), nullable=True)
    supervisor = db.Column(db.String(100), nullable=True)
    
    # Time tracking
    estimated_hours = db.Column(db.Float, default=0)
    actual_hours = db.Column(db.Float, default=0)
    downtime_hours = db.Column(db.Float, default=0)
    
    # Cost tracking
    labor_cost = db.Column(db.Float, default=0)
    parts_cost = db.Column(db.Float, default=0)
    external_cost = db.Column(db.Float, default=0)
    total_cost = db.Column(db.Float, default=0)
    
    # Additional fields
    location = db.Column(db.String(100), nullable=True)
    notes = db.Column(db.Text, nullable=True)
    
    def __repr__(self):
        return f'<WorkOrder {self.work_order_number}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'work_order_number': self.work_order_number,
            'description': self.description,
            'maintenance_type': self.maintenance_type,
            'priority': self.priority,
            'status': self.status,
            'equipment_id': self.equipment_id,
            'equipment_name': self.equipment_name,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'scheduled_date': self.scheduled_date.isoformat() if self.scheduled_date else None,
            'start_date': self.start_date.isoformat() if self.start_date else None,
            'completion_date': self.completion_date.isoformat() if self.completion_date else None,
            'assigned_to': self.assigned_to,
            'supervisor': self.supervisor,
            'estimated_hours': self.estimated_hours,
            'actual_hours': self.actual_hours,
            'downtime_hours': self.downtime_hours,
            'labor_cost': self.labor_cost,
            'parts_cost': self.parts_cost,
            'external_cost': self.external_cost,
            'total_cost': self.total_cost,
            'location': self.location,
            'notes': self.notes
        }


class SparePart(db.Model):
    __tablename__ = 'spare_parts'
    
    id = db.Column(db.Integer, primary_key=True)
    part_number = db.Column(db.String(100), nullable=False, unique=True)
    part_name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    category = db.Column(db.String(100), nullable=True)
    subcategory = db.Column(db.String(100), nullable=True)
    
    # Inventory
    unit_of_measure = db.Column(db.String(20), default='EA')
    unit_cost = db.Column(db.Float, default=0)
    quantity_on_hand = db.Column(db.Integer, default=0)
    quantity_allocated = db.Column(db.Integer, default=0)
    quantity_available = db.Column(db.Integer, default=0)
    
    # Stock levels
    reorder_level = db.Column(db.Integer, default=0)
    max_stock_level = db.Column(db.Integer, default=0)
    safety_stock = db.Column(db.Integer, default=0)
    
    # Supplier information
    supplier = db.Column(db.String(255), nullable=True)
    supplier_part_number = db.Column(db.String(100), nullable=True)
    lead_time_days = db.Column(db.Integer, default=0)
    
    # Storage and usage
    storage_location = db.Column(db.String(100), nullable=True)
    bin_location = db.Column(db.String(50), nullable=True)
    annual_usage = db.Column(db.Integer, default=0)
    
    # Dates
    created_date = db.Column(db.DateTime, default=datetime.utcnow)
    last_updated = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_received_date = db.Column(db.DateTime, nullable=True)
    last_issued_date = db.Column(db.DateTime, nullable=True)
    
    # Status
    status = db.Column(db.String(20), default='Active')  # Active, Inactive, Obsolete
    criticality = db.Column(db.String(20), default='Medium')  # High, Medium, Low
    
    def __repr__(self):
        return f'<SparePart {self.part_number}>'
    
    def to_dict(self):
        return {
            'id': self.id,
            'part_number': self.part_number,
            'part_name': self.part_name,
            'description': self.description,
            'category': self.category,
            'subcategory': self.subcategory,
            'unit_of_measure': self.unit_of_measure,
            'unit_cost': self.unit_cost,
            'quantity_on_hand': self.quantity_on_hand,
            'quantity_allocated': self.quantity_allocated,
            'quantity_available': self.quantity_available,
            'reorder_level': self.reorder_level,
            'max_stock_level': self.max_stock_level,
            'safety_stock': self.safety_stock,
            'supplier': self.supplier,
            'supplier_part_number': self.supplier_part_number,
            'lead_time_days': self.lead_time_days,
            'storage_location': self.storage_location,
            'bin_location': self.bin_location,
            'annual_usage': self.annual_usage,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None,
            'last_received_date': self.last_received_date.isoformat() if self.last_received_date else None,
            'last_issued_date': self.last_issued_date.isoformat() if self.last_issued_date else None,
            'status': self.status,
            'criticality': self.criticality
        }


class Equipment(db.Model):
    __tablename__ = 'equipment'
    
    id = db.Column(db.Integer, primary_key=True)
    equipment_id = db.Column(db.String(50), nullable=False, unique=True)
    equipment_name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    equipment_type = db.Column(db.String(100), nullable=True)
    manufacturer = db.Column(db.String(100), nullable=True)
    model = db.Column(db.String(100), nullable=True)
    serial_number = db.Column(db.String(100), nullable=True)
    
    # Location and status
    location = db.Column(db.String(100), nullable=True)
    department = db.Column(db.String(100), nullable=True)
    status = db.Column(db.String(20), default='Active')  # Active, Inactive, Maintenance, Decommissioned
    criticality = db.Column(db.String(20), default='Medium')  # High, Medium, Low
    
    # Dates
    installation_date = db.Column(db.DateTime, nullable=True)
    warranty_expiry = db.Column(db.DateTime, nullable=True)
    last_maintenance_date = db.Column(db.DateTime, nullable=True)
    next_maintenance_date = db.Column(db.DateTime, nullable=True)
    
    # Performance metrics
    availability = db.Column(db.Float, default=100.0)  # Percentage
    mtbf = db.Column(db.Float, default=0)  # Mean Time Between Failures (hours)
    mttr = db.Column(db.Float, default=0)  # Mean Time To Repair (hours)
    
    def __repr__(self):
        return f'<Equipment {self.equipment_id}>'


class MaintenanceSchedule(db.Model):
    __tablename__ = 'maintenance_schedule'
    
    id = db.Column(db.Integer, primary_key=True)
    equipment_id = db.Column(db.String(50), db.ForeignKey('equipment.equipment_id'), nullable=False)
    maintenance_type = db.Column(db.String(20), default='PM')
    description = db.Column(db.Text, nullable=False)
    frequency_type = db.Column(db.String(20), nullable=False)  # Daily, Weekly, Monthly, Quarterly, Yearly
    frequency_value = db.Column(db.Integer, default=1)  # Every X frequency_type
    estimated_hours = db.Column(db.Float, default=0)
    
    # Scheduling
    last_performed = db.Column(db.DateTime, nullable=True)
    next_due = db.Column(db.DateTime, nullable=True)
    
    # Status
    status = db.Column(db.String(20), default='Active')  # Active, Inactive, Completed
    
    # Relationship
    equipment = db.relationship('Equipment', backref='maintenance_schedules')
    
    def __repr__(self):
        return f'<MaintenanceSchedule {self.equipment_id}: {self.description}>'