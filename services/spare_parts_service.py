"""
LCT STS Spare Parts Service
Handles spare parts inventory management using the existing Stock table
"""

from models import db
from sqlalchemy import text
from datetime import datetime, timedelta
import logging
from .currency_service import currency_service

logger = logging.getLogger(__name__)

class SparePartsService:
    """Service for spare parts inventory management using the Stock table."""
    
    def __init__(self):
        self.currency_service = currency_service
    
    def format_price(self, amount, show_eur=True):
        """Format price with CFA and optional EUR conversion."""
        return self.currency_service.format_currency(amount, 'XOF', show_eur)
    
    def convert_to_eur(self, cfa_amount):
        """Convert CFA amount to EUR."""
        return self.currency_service.convert_to_eur(cfa_amount)
    
    def __init__(self):
        """Initialize the spare parts service."""
        pass
    
    def get_spare_parts_inventory(self, low_stock_only=False, out_of_stock_only=False, limit=100, offset=0):
        """Get spare parts inventory from the Stock table."""
        try:
            query = """
                SELECT 
                    id,
                    reference_article as part_number,
                    designation_1 as part_name,
                    designation_2 as description,
                    categorie_article as category,
                    quantite_en_stock as quantity_on_hand,
                    seuil_de_reappro_min as reorder_level,
                    quantite_maximum_max as max_quantity,
                    stock_securite as safety_stock,
                    pmp as unit_cost,
                    unite_de_stock as unit_of_measure,
                    emplacement_de_l_article as location,
                    date_derniere_entree as last_received_date,
                    date_derniere_sortie as last_issued_date,
                    sous_min as below_minimum
                FROM Stock 
                WHERE 1=1
            """
            params = {}
            
            if low_stock_only:
                query += " AND quantite_en_stock <= seuil_de_reappro_min"
            
            if out_of_stock_only:
                query += " AND quantite_en_stock = 0"
            
            query += " ORDER BY designation_1 ASC LIMIT :limit OFFSET :offset"
            params.update({"limit": limit, "offset": offset})
            
            result = db.session.execute(text(query), params)
            rows = result.fetchall()
            
            # Convert to list of dictionaries
            spare_parts = []
            for row in rows:
                unit_cost = row[9] or 0.0
                quantity = row[5] or 0
                total_value = unit_cost * quantity
                
                spare_parts.append({
                    'id': row[0],
                    'part_number': row[1] or '',
                    'part_name': row[2] or '',
                    'description': row[3] or '',
                    'category': row[4] or '',
                    'quantity_on_hand': quantity,
                    'reorder_level': row[6] or 0,
                    'max_quantity': row[7] or 0,
                    'safety_stock': row[8] or 0,
                    'unit_cost': unit_cost,
                    'unit_cost_formatted': self.format_price(unit_cost),
                    'total_value': total_value,
                    'total_value_formatted': self.format_price(total_value),
                    'unit_cost_eur': self.convert_to_eur(unit_cost),
                    'total_value_eur': self.convert_to_eur(total_value),
                    'unit_of_measure': row[10] or '',
                    'location': row[11] or '',
                    'last_received_date': row[12] or '',
                    'last_issued_date': row[13] or '',
                    'below_minimum': row[14] or '',
                    'status': 'Critical' if quantity <= (row[6] or 0) else 'Normal'
                })
            
            return spare_parts
            
        except Exception as e:
            logger.error(f"Error getting spare parts inventory: {str(e)}")
            return []
    
    def get_critical_spare_parts(self, limit=50):
        """Get spare parts that are at or below reorder level from Stock table."""
        try:
            query = """
                SELECT 
                    id,
                    reference_article as part_number,
                    designation_1 as part_name,
                    designation_2 as description,
                    categorie_article as category,
                    quantite_en_stock as quantity_on_hand,
                    seuil_de_reappro_min as reorder_level,
                    pmp as unit_cost,
                    emplacement_de_l_article as location,
                    CASE 
                        WHEN quantite_en_stock = 0 THEN 'Out of Stock'
                        WHEN quantite_en_stock <= seuil_de_reappro_min THEN 'Critical'
                        ELSE 'Normal'
                    END as status
                FROM Stock 
                WHERE quantite_en_stock <= seuil_de_reappro_min
                   OR quantite_en_stock = 0
                ORDER BY 
                    CASE 
                        WHEN quantite_en_stock = 0 THEN 1
                        WHEN quantite_en_stock <= seuil_de_reappro_min THEN 2
                        ELSE 3
                    END,
                    designation_1 ASC 
                LIMIT :limit
            """
            
            result = db.session.execute(text(query), {"limit": limit})
            rows = result.fetchall()
            
            critical_parts = []
            for row in rows:
                critical_parts.append({
                    'id': row[0],
                    'part_number': row[1] or '',
                    'part_name': row[2] or '',
                    'description': row[3] or '',
                    'category': row[4] or '',
                    'quantity_on_hand': row[5] or 0,
                    'reorder_level': row[6] or 0,
                    'unit_cost': row[7] or 0.0,
                    'location': row[8] or '',
                    'status': row[9]
                })
            
            return critical_parts
            
        except Exception as e:
            logger.error(f"Error getting critical spare parts: {str(e)}")
            return []
    
    def get_out_of_stock_parts(self):
        """Get spare parts that are out of stock."""
        return self.get_spare_parts_inventory(out_of_stock_only=True)
    
    def get_spare_parts_statistics(self):
        """Get comprehensive spare parts statistics from Stock table."""
        try:
            stats = {}
            
            # Total parts count
            result = db.session.execute(text("SELECT COUNT(*) FROM Stock"))
            stats['total_parts'] = result.scalar() or 0
            
            # Critical stock count (at or below reorder level)
            result = db.session.execute(text("""
                SELECT COUNT(*) FROM Stock 
                WHERE quantite_en_stock <= seuil_de_reappro_min
                AND seuil_de_reappro_min > 0
            """))
            stats['critical_stock'] = result.scalar() or 0
            
            # Out of stock count
            result = db.session.execute(text("""
                SELECT COUNT(*) FROM Stock 
                WHERE quantite_en_stock = 0
            """))
            stats['out_of_stock'] = result.scalar() or 0
            
            # Total inventory value
            result = db.session.execute(text("""
                SELECT SUM(quantite_en_stock * pmp) 
                FROM Stock
                WHERE pmp IS NOT NULL AND quantite_en_stock IS NOT NULL
            """))
            stats['total_value'] = result.scalar() or 0.0
            
            # Average days since last movement
            result = db.session.execute(text("""
                SELECT AVG(
                    CASE 
                        WHEN date_derniere_sortie IS NOT NULL AND date_derniere_sortie != ''
                        THEN julianday('now') - julianday(date_derniere_sortie)
                        ELSE NULL
                    END
                ) FROM Stock
                WHERE date_derniere_sortie IS NOT NULL AND date_derniere_sortie != ''
            """))
            stats['avg_days_since_movement'] = result.scalar() or 0
            
            # Items below safety stock
            result = db.session.execute(text("""
                SELECT COUNT(*) FROM Stock 
                WHERE quantite_en_stock < stock_securite
                AND stock_securite > 0
            """))
            stats['below_safety_stock'] = result.scalar() or 0
            
            # Categories breakdown
            result = db.session.execute(text("""
                SELECT categorie_article, COUNT(*) as count
                FROM Stock 
                WHERE categorie_article IS NOT NULL
                GROUP BY categorie_article
                ORDER BY count DESC
                LIMIT 10
            """))
            stats['categories'] = [{'category': row[0], 'count': row[1]} for row in result.fetchall()]
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting spare parts statistics: {str(e)}")
            return {
                'total_parts': 0,
                'critical_stock': 0,
                'out_of_stock': 0,
                'total_value': 0.0,
                'avg_days_since_movement': 0,
                'below_safety_stock': 0,
                'categories': []
            }
    
    def search_spare_parts(self, search_term, limit=50):
        """Search spare parts by name, number, or description."""
        try:
            query = """
                SELECT 
                    id,
                    reference_article as part_number,
                    designation_1 as part_name,
                    designation_2 as description,
                    categorie_article as category,
                    quantite_en_stock as quantity_on_hand,
                    seuil_de_reappro_min as reorder_level,
                    pmp as unit_cost,
                    emplacement_de_l_article as location
                FROM Stock 
                WHERE designation_1 LIKE ? 
                   OR reference_article LIKE ?
                   OR designation_2 LIKE ?
                   OR categorie_article LIKE ?
                ORDER BY designation_1 ASC 
                LIMIT :limit
            """
            
            search_pattern = f"%{search_term}%"
            result = db.session.execute(text(query), {
                "search1": search_pattern,
                "search2": search_pattern, 
                "search3": search_pattern,
                "search4": search_pattern,
                "limit": limit
            })
            rows = result.fetchall()
            
            search_results = []
            for row in rows:
                search_results.append({
                    'id': row[0],
                    'part_number': row[1] or '',
                    'part_name': row[2] or '',
                    'description': row[3] or '',
                    'category': row[4] or '',
                    'quantity_on_hand': row[5] or 0,
                    'reorder_level': row[6] or 0,
                    'unit_cost': row[7] or 0.0,
                    'location': row[8] or ''
                })
            
            return search_results
            
        except Exception as e:
            logger.error(f"Error searching spare parts: {str(e)}")
            return []
    
    def get_reorder_suggestions(self, days_ahead=30):
        """Get reorder suggestions based on stock levels and usage patterns."""
        try:
            query = """
                SELECT 
                    id,
                    reference_article as part_number,
                    designation_1 as part_name,
                    quantite_en_stock as current_stock,
                    seuil_de_reappro_min as reorder_level,
                    quantite_maximum_max as max_quantity,
                    lot_economique as economic_order_qty,
                    pmp as unit_cost,
                    CASE 
                        WHEN quantite_en_stock <= seuil_de_reappro_min THEN 'Immediate'
                        WHEN quantite_en_stock <= (seuil_de_reappro_min * 1.5) THEN 'Soon'
                        ELSE 'Monitor'
                    END as urgency
                FROM Stock 
                WHERE quantite_en_stock <= (seuil_de_reappro_min * 1.5)
                  AND seuil_de_reappro_min > 0
                ORDER BY 
                    CASE 
                        WHEN quantite_en_stock <= seuil_de_reappro_min THEN 1
                        ELSE 2
                    END,
                    quantite_en_stock ASC
            """
            
            result = db.session.execute(text(query))
            rows = result.fetchall()
            
            suggestions = []
            for row in rows:
                suggested_qty = max(row[6] or 0, (row[4] or 0) * 2)  # Economic order qty or 2x reorder level
                
                suggestions.append({
                    'id': row[0],
                    'part_number': row[1] or '',
                    'part_name': row[2] or '',
                    'current_stock': row[3] or 0,
                    'reorder_level': row[4] or 0,
                    'max_quantity': row[5] or 0,
                    'suggested_order_qty': suggested_qty,
                    'unit_cost': row[7] or 0.0,
                    'estimated_cost': suggested_qty * (row[7] or 0.0),
                    'urgency': row[8]
                })
            
            return suggestions
            
        except Exception as e:
            logger.error(f"Error getting reorder suggestions: {str(e)}")
            return []
    
    def get_inventory_overview(self):
        """Get inventory overview with categories and status."""
        try:
            query = """
                SELECT 
                    categorie_article as category,
                    COUNT(*) as total_items,
                    SUM(quantite_en_stock) as total_quantity,
                    SUM(quantite_en_stock * pmp) as total_value,
                    SUM(CASE WHEN quantite_en_stock <= seuil_de_reappro_min THEN 1 ELSE 0 END) as critical_items,
                    SUM(CASE WHEN quantite_en_stock = 0 THEN 1 ELSE 0 END) as out_of_stock_items
                FROM Stock 
                WHERE categorie_article IS NOT NULL
                GROUP BY categorie_article
                ORDER BY total_value DESC
            """
            
            result = db.session.execute(text(query))
            rows = result.fetchall()
            
            overview = []
            for row in rows:
                overview.append({
                    'category': row[0] or 'Uncategorized',
                    'total_items': row[1] or 0,
                    'total_quantity': row[2] or 0,
                    'total_value': row[3] or 0.0,
                    'critical_items': row[4] or 0,
                    'out_of_stock_items': row[5] or 0
                })
            
            return overview
            
        except Exception as e:
            logger.error(f"Error getting inventory overview: {str(e)}")
            return []
    
    def get_critical_alerts(self):
        """Get critical alerts for spare parts management."""
        try:
            alerts = []
            
            # Out of stock alerts
            result = db.session.execute(text("""
                SELECT COUNT(*), GROUP_CONCAT(designation_1, ', ') as items
                FROM (
                    SELECT designation_1 FROM Stock 
                    WHERE quantite_en_stock = 0 
                    LIMIT 5
                )
            """))
            row = result.fetchone()
            if row and row[0] > 0:
                alerts.append({
                    'type': 'out_of_stock',
                    'severity': 'critical',
                    'count': row[0],
                    'message': f"{row[0]} items are out of stock",
                    'items': row[1] if row[1] else ''
                })
            
            # Critical stock alerts
            result = db.session.execute(text("""
                SELECT COUNT(*), GROUP_CONCAT(designation_1, ', ') as items
                FROM (
                    SELECT designation_1 FROM Stock 
                    WHERE quantite_en_stock <= seuil_de_reappro_min 
                    AND quantite_en_stock > 0
                    LIMIT 5
                )
            """))
            row = result.fetchone()
            if row and row[0] > 0:
                alerts.append({
                    'type': 'critical_stock',
                    'severity': 'warning',
                    'count': row[0],
                    'message': f"{row[0]} items are at or below reorder level",
                    'items': row[1] if row[1] else ''
                })
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error getting critical alerts: {str(e)}")
            return []
    
    # ===== ADVANCED STOCK MANAGEMENT METHODS =====
    
    def update_stock_quantity(self, part_id, new_quantity, transaction_type='manual', notes=''):
        """Update stock quantity with audit trail."""
        try:
            # Get current part data
            result = db.session.execute(text("""
                SELECT reference_article, designation_1, quantite_en_stock, pmp 
                FROM Stock WHERE id = ?
            """), [part_id])
            row = result.fetchone()
            
            if not row:
                return {'success': False, 'error': 'Part not found'}
            
            old_quantity = row[2] or 0
            quantity_change = new_quantity - old_quantity
            
            # Update stock quantity
            db.session.execute(text("""
                UPDATE Stock 
                SET quantite_en_stock = ?,
                    date_derniere_entree = CASE WHEN ? > 0 THEN date('now') ELSE date_derniere_entree END,
                    date_derniere_sortie = CASE WHEN ? < 0 THEN date('now') ELSE date_derniere_sortie END
                WHERE id = ?
            """), [new_quantity, quantity_change, quantity_change, part_id])
            
            # Create movement record (if stock_movements table exists)
            try:
                db.session.execute(text("""
                    INSERT OR IGNORE INTO stock_movements 
                    (part_id, reference_article, movement_type, quantity_change, new_quantity, transaction_date, notes, user_id)
                    VALUES (?, ?, ?, ?, ?, datetime('now'), ?, 'system')
                """), [part_id, row[0], transaction_type, quantity_change, new_quantity, notes])
            except:
                # Table might not exist, continue without logging movement
                pass
            
            db.session.commit()
            
            return {
                'success': True,
                'message': f'Stock updated for {row[1]}. Quantity changed from {old_quantity} to {new_quantity}',
                'old_quantity': old_quantity,
                'new_quantity': new_quantity,
                'quantity_change': quantity_change
            }
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error updating stock for part {part_id}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def record_stock_movement(self, part_id, movement_type, quantity, reference_doc='', notes='', cost_per_unit=None):
        """Record stock movement with proper accounting."""
        try:
            # Get current part data
            result = db.session.execute(text("""
                SELECT reference_article, designation_1, quantite_en_stock, pmp 
                FROM Stock WHERE id = ?
            """), [part_id])
            row = result.fetchone()
            
            if not row:
                return {'success': False, 'error': 'Part not found'}
            
            current_quantity = row[2] or 0
            current_cost = row[3] or 0
            
            # Calculate new quantity based on movement type
            if movement_type in ['receipt', 'return']:
                new_quantity = current_quantity + quantity
                quantity_change = quantity
            elif movement_type in ['issue', 'transfer']:
                if quantity > current_quantity:
                    return {'success': False, 'error': f'Insufficient stock. Available: {current_quantity}'}
                new_quantity = current_quantity - quantity
                quantity_change = -quantity
            elif movement_type == 'adjustment':
                new_quantity = quantity  # Direct adjustment to specified quantity
                quantity_change = quantity - current_quantity
            else:
                return {'success': False, 'error': 'Invalid movement type'}
            
            # Update weighted average cost if cost provided and it's a receipt
            new_cost = current_cost
            if movement_type == 'receipt' and cost_per_unit and cost_per_unit > 0:
                total_value = (current_quantity * current_cost) + (quantity * cost_per_unit)
                new_cost = total_value / new_quantity if new_quantity > 0 else cost_per_unit
            
            # Update stock in database
            db.session.execute(text("""
                UPDATE Stock 
                SET quantite_en_stock = ?,
                    pmp = ?,
                    date_derniere_entree = CASE WHEN ? IN ('receipt', 'return') THEN date('now') ELSE date_derniere_entree END,
                    date_derniere_sortie = CASE WHEN ? IN ('issue', 'transfer') THEN date('now') ELSE date_derniere_sortie END
                WHERE id = ?
            """), [new_quantity, new_cost, movement_type, movement_type, part_id])
            
            # Log movement (create table if it doesn't exist)
            try:
                db.session.execute(text("""
                    CREATE TABLE IF NOT EXISTS stock_movements (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        part_id INTEGER,
                        reference_article TEXT,
                        movement_type TEXT,
                        quantity_change INTEGER,
                        new_quantity INTEGER,
                        unit_cost REAL,
                        total_value REAL,
                        reference_doc TEXT,
                        notes TEXT,
                        transaction_date DATETIME,
                        user_id TEXT
                    )
                """))
                
                db.session.execute(text("""
                    INSERT INTO stock_movements 
                    (part_id, reference_article, movement_type, quantity_change, new_quantity, 
                     unit_cost, total_value, reference_doc, notes, transaction_date, user_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), 'system')
                """), [part_id, row[0], movement_type, quantity_change, new_quantity, 
                       cost_per_unit or new_cost, abs(quantity_change) * (cost_per_unit or new_cost),
                       reference_doc, notes])
            except Exception as e:
                logger.warning(f"Could not log stock movement: {e}")
            
            db.session.commit()
            
            return {
                'success': True,
                'message': f'{movement_type.title()} recorded for {row[1]}. New quantity: {new_quantity}',
                'old_quantity': current_quantity,
                'new_quantity': new_quantity,
                'quantity_change': quantity_change
            }
            
        except Exception as e:
            db.session.rollback()
            logger.error(f"Error recording stock movement for part {part_id}: {str(e)}")
            return {'success': False, 'error': str(e)}
    
    def get_stock_movements(self, part_id=None, limit=100):
        """Get stock movement history."""
        try:
            if part_id:
                query = """
                    SELECT sm.*, s.designation_1 as part_name 
                    FROM stock_movements sm
                    LEFT JOIN Stock s ON sm.part_id = s.id
                    WHERE sm.part_id = :part_id
                    ORDER BY sm.transaction_date DESC 
                    LIMIT :limit
                """
                params = {"part_id": part_id, "limit": limit}
            else:
                query = """
                    SELECT sm.*, s.designation_1 as part_name 
                    FROM stock_movements sm
                    LEFT JOIN Stock s ON sm.part_id = s.id
                    ORDER BY sm.transaction_date DESC 
                    LIMIT :limit
                """
                params = {"limit": limit}
            
            result = db.session.execute(text(query), params)
            rows = result.fetchall()
            
            movements = []
            for row in rows:
                movements.append({
                    'id': row[0],
                    'part_id': row[1],
                    'reference_article': row[2],
                    'movement_type': row[3],
                    'quantity_change': row[4],
                    'new_quantity': row[5],
                    'unit_cost': row[6],
                    'total_value': row[7],
                    'reference_doc': row[8],
                    'notes': row[9],
                    'transaction_date': row[10],
                    'user_id': row[11],
                    'part_name': row[12] if len(row) > 12 else ''
                })
            
            return movements
            
        except Exception as e:
            logger.error(f"Error getting stock movements: {str(e)}")
            return []
    
    def get_stock_analytics(self):
        """Get advanced stock analytics and insights."""
        try:
            analytics = {}
            
            # ABC Analysis - classify parts by value
            abc_result = db.session.execute(text("""
                WITH part_values AS (
                    SELECT 
                        reference_article,
                        designation_1,
                        quantite_en_stock * pmp as total_value,
                        quantite_en_stock,
                        pmp
                    FROM Stock 
                    WHERE quantite_en_stock > 0 AND pmp > 0
                ),
                ranked_parts AS (
                    SELECT *,
                        SUM(total_value) OVER () as total_inventory_value,
                        SUM(total_value) OVER (ORDER BY total_value DESC) as cumulative_value,
                        ROW_NUMBER() OVER (ORDER BY total_value DESC) as rank_num,
                        COUNT(*) OVER () as total_parts
                    FROM part_values
                ),
                classified_parts AS (
                    SELECT *,
                        CASE 
                            WHEN cumulative_value / total_inventory_value <= 0.8 THEN 'A'
                            WHEN cumulative_value / total_inventory_value <= 0.95 THEN 'B'
                            ELSE 'C'
                        END as abc_class
                    FROM ranked_parts
                )
                SELECT 
                    abc_class,
                    COUNT(*) as part_count,
                    SUM(total_value) as class_value,
                    AVG(total_value) as avg_value,
                    MAX(total_value) as max_value,
                    MIN(total_value) as min_value
                FROM classified_parts
                GROUP BY abc_class
                ORDER BY abc_class
            """))
            
            abc_data = []
            for row in abc_result.fetchall():
                abc_data.append({
                    'class': row[0],
                    'part_count': row[1],
                    'total_value': row[2],
                    'avg_value': row[3],
                    'max_value': row[4],
                    'min_value': row[5]
                })
            analytics['abc_analysis'] = abc_data
            
            # Stock turnover analysis
            turnover_result = db.session.execute(text("""
                SELECT 
                    categorie_article,
                    COUNT(*) as parts_count,
                    SUM(quantite_en_stock * pmp) as total_value,
                    AVG(quantite_en_stock) as avg_quantity,
                    SUM(CASE WHEN quantite_en_stock = 0 THEN 1 ELSE 0 END) as zero_stock_count,
                    SUM(CASE WHEN quantite_en_stock <= seuil_de_reappro_min THEN 1 ELSE 0 END) as low_stock_count
                FROM Stock 
                WHERE categorie_article IS NOT NULL
                GROUP BY categorie_article
                ORDER BY total_value DESC
            """))
            
            category_analysis = []
            for row in turnover_result.fetchall():
                category_analysis.append({
                    'category': row[0],
                    'parts_count': row[1],
                    'total_value': row[2],
                    'avg_quantity': row[3],
                    'zero_stock_count': row[4],
                    'low_stock_count': row[5],
                    'stock_health': 'Good' if row[4] == 0 and row[5] < row[1] * 0.1 else 'Needs Attention'
                })
            analytics['category_analysis'] = category_analysis
            
            # Dead stock analysis (no movement in last 90 days)
            dead_stock_result = db.session.execute(text("""
                SELECT 
                    reference_article,
                    designation_1,
                    quantite_en_stock,
                    pmp,
                    quantite_en_stock * pmp as dead_value,
                    date_derniere_sortie,
                    COALESCE(julianday('now') - julianday(date_derniere_sortie), 999) as days_since_movement
                FROM Stock 
                WHERE quantite_en_stock > 0 
                  AND (date_derniere_sortie IS NULL OR julianday('now') - julianday(date_derniere_sortie) > 90)
                ORDER BY dead_value DESC
                LIMIT 20
            """))
            
            dead_stock = []
            for row in dead_stock_result.fetchall():
                dead_stock.append({
                    'reference_article': row[0],
                    'part_name': row[1],
                    'quantity': row[2],
                    'unit_cost': row[3],
                    'total_value': row[4],
                    'last_movement': row[5],
                    'days_since_movement': int(row[6]) if row[6] != 999 else 'Never'
                })
            analytics['dead_stock'] = dead_stock
            
            # Inventory aging
            aging_result = db.session.execute(text("""
                SELECT 
                    CASE 
                        WHEN date_derniere_entree IS NULL THEN 'Unknown'
                        WHEN julianday('now') - julianday(date_derniere_entree) <= 30 THEN '0-30 days'
                        WHEN julianday('now') - julianday(date_derniere_entree) <= 90 THEN '31-90 days'
                        WHEN julianday('now') - julianday(date_derniere_entree) <= 180 THEN '91-180 days'
                        WHEN julianday('now') - julianday(date_derniere_entree) <= 365 THEN '181-365 days'
                        ELSE 'Over 1 year'
                    END as age_group,
                    COUNT(*) as part_count,
                    SUM(quantite_en_stock * pmp) as total_value
                FROM Stock 
                WHERE quantite_en_stock > 0
                GROUP BY age_group
                ORDER BY 
                    CASE age_group
                        WHEN '0-30 days' THEN 1
                        WHEN '31-90 days' THEN 2
                        WHEN '91-180 days' THEN 3
                        WHEN '181-365 days' THEN 4
                        WHEN 'Over 1 year' THEN 5
                        ELSE 6
                    END
            """))
            
            inventory_aging = []
            for row in aging_result.fetchall():
                inventory_aging.append({
                    'age_group': row[0],
                    'part_count': row[1],
                    'total_value': row[2]
                })
            analytics['inventory_aging'] = inventory_aging
            
            return analytics
            
        except Exception as e:
            logger.error(f"Error getting stock analytics: {str(e)}")
            return {}
    
    def advanced_search(self, search_term='', category='', location='', status=''):
        """Advanced search with multiple filters."""
        try:
            query = """
                SELECT 
                    id,
                    reference_article as part_number,
                    designation_1 as part_name,
                    designation_2 as description,
                    categorie_article as category,
                    quantite_en_stock as quantity_on_hand,
                    seuil_de_reappro_min as reorder_level,
                    pmp as unit_cost,
                    emplacement_de_l_article as location,
                    CASE 
                        WHEN quantite_en_stock = 0 THEN 'out_of_stock'
                        WHEN quantite_en_stock <= seuil_de_reappro_min THEN 'critical'
                        ELSE 'normal'
                    END as stock_status
                FROM Stock 
                WHERE 1=1
            """
            params = []
            
            # Search term filter
            if search_term:
                query += """ AND (
                    designation_1 LIKE ? OR 
                    reference_article LIKE ? OR 
                    designation_2 LIKE ?
                )"""
                search_pattern = f"%{search_term}%"
                params.extend([search_pattern, search_pattern, search_pattern])
            
            # Category filter
            if category:
                query += " AND categorie_article = ?"
                params.append(category)
            
            # Location filter
            if location:
                query += " AND emplacement_de_l_article LIKE ?"
                params.append(f"%{location}%")
            
            # Status filter
            if status == 'critical':
                query += " AND quantite_en_stock <= seuil_de_reappro_min AND quantite_en_stock > 0"
            elif status == 'out_of_stock':
                query += " AND quantite_en_stock = 0"
            elif status == 'normal':
                query += " AND quantite_en_stock > seuil_de_reappro_min"
            
            query += " ORDER BY designation_1 ASC LIMIT 100"
            
            result = db.session.execute(text(query), params)
            rows = result.fetchall()
            
            search_results = []
            for row in rows:
                search_results.append({
                    'id': row[0],
                    'part_number': row[1] or '',
                    'part_name': row[2] or '',
                    'description': row[3] or '',
                    'category': row[4] or '',
                    'quantity_on_hand': row[5] or 0,
                    'reorder_level': row[6] or 0,
                    'unit_cost': row[7] or 0.0,
                    'location': row[8] or '',
                    'stock_status': row[9]
                })
            
            return search_results
            
        except Exception as e:
            logger.error(f"Error in advanced search: {str(e)}")
            return []
    
    def generate_purchase_order(self, part_ids, supplier_id=None):
        """Generate purchase order for selected parts."""
        try:
            if not part_ids:
                return {'success': False, 'error': 'No parts selected'}
            
            # Get part details
            placeholders = ','.join(['?' for _ in part_ids])
            query = f"""
                SELECT 
                    id,
                    reference_article,
                    designation_1,
                    quantite_en_stock,
                    seuil_de_reappro_min,
                    lot_economique,
                    pmp
                FROM Stock 
                WHERE id IN ({placeholders})
            """
            
            result = db.session.execute(text(query), part_ids)
            parts = result.fetchall()
            
            if not parts:
                return {'success': False, 'error': 'No valid parts found'}
            
            # Calculate order quantities and total
            po_items = []
            total_amount = 0.0
            
            for part in parts:
                current_stock = part[3] or 0
                reorder_level = part[4] or 0
                economic_qty = part[5] or (reorder_level * 2)
                unit_cost = part[6] or 0.0
                
                # Calculate order quantity
                if current_stock <= reorder_level:
                    order_qty = max(economic_qty, reorder_level * 2)
                else:
                    order_qty = economic_qty
                
                line_total = order_qty * unit_cost
                total_amount += line_total
                
                po_items.append({
                    'part_id': part[0],
                    'part_number': part[1],
                    'part_name': part[2],
                    'current_stock': current_stock,
                    'order_quantity': order_qty,
                    'unit_cost': unit_cost,
                    'line_total': line_total
                })
            
            # Create PO record (if purchase_orders table exists)
            po_number = f"PO-{datetime.now().strftime('%Y%m%d')}-{len(po_items):03d}"
            
            try:
                # Create purchase_orders table if it doesn't exist
                db.session.execute(text("""
                    CREATE TABLE IF NOT EXISTS purchase_orders (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        po_number TEXT UNIQUE,
                        supplier_id TEXT,
                        total_amount REAL,
                        status TEXT DEFAULT 'draft',
                        created_date DATETIME,
                        expected_delivery DATE,
                        notes TEXT
                    )
                """))
                
                # Create purchase_order_items table if it doesn't exist
                db.session.execute(text("""
                    CREATE TABLE IF NOT EXISTS purchase_order_items (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        po_id INTEGER,
                        part_id INTEGER,
                        part_number TEXT,
                        part_name TEXT,
                        quantity INTEGER,
                        unit_cost REAL,
                        line_total REAL,
                        FOREIGN KEY (po_id) REFERENCES purchase_orders (id)
                    )
                """))
                
                # Insert PO header
                result = db.session.execute(text("""
                    INSERT INTO purchase_orders 
                    (po_number, supplier_id, total_amount, created_date, notes)
                    VALUES (?, ?, ?, datetime('now'), ?)
                """), [po_number, supplier_id or 'default', total_amount, 'Auto-generated from reorder suggestions'])
                
                po_id = result.lastrowid
                
                # Insert PO items
                for item in po_items:
                    db.session.execute(text("""
                        INSERT INTO purchase_order_items 
                        (po_id, part_id, part_number, part_name, quantity, unit_cost, line_total)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """), [po_id, item['part_id'], item['part_number'], item['part_name'],
                           item['order_quantity'], item['unit_cost'], item['line_total']])
                
                db.session.commit()
                
                return {
                    'success': True,
                    'message': f'Purchase order {po_number} generated successfully',
                    'po_number': po_number,
                    'po_id': po_id,
                    'total_amount': total_amount,
                    'items_count': len(po_items),
                    'items': po_items
                }
                
            except Exception as e:
                logger.warning(f"Could not create PO tables: {e}")
                # Return data without persisting
                return {
                    'success': True,
                    'message': f'Purchase order data prepared (not saved to database)',
                    'po_number': po_number,
                    'total_amount': total_amount,
                    'items_count': len(po_items),
                    'items': po_items
                }
            
        except Exception as e:
            logger.error(f"Error generating purchase order: {str(e)}")
            return {'success': False, 'error': str(e)}
