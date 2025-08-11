"""
Stock Analysis Service
Provides comprehensive stock analysis by combining data from Stock, PO, PR, and Annual tables
using REFERENCE_ARTICLE as the primary key for joining.
"""

from models import db
from sqlalchemy import text
import pandas as pd
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class StockAnalysisService:
    """Service for comprehensive stock analysis across multiple tables."""
    
    def __init__(self):
        self.stock_ref_column = 'reference_article'
        self.po_ref_column = 'code_article'
        self.pr_ref_column = 'article'
        self.annual_ref_column = 'article'
    
    def get_comprehensive_stock_analysis(self, limit=None, article_filter=None):
        """
        Get comprehensive stock analysis combining data from all tables.
        
        Args:
            limit (int): Limit number of results
            article_filter (str): Filter by article reference
            
        Returns:
            dict: Comprehensive analysis results
        """
        try:
            # Build the main query
            query = self._build_comprehensive_query(limit, article_filter)
            
            # Execute query
            result = db.session.execute(text(query))
            columns = list(result.keys())
            data = result.fetchall()
            
            # Convert to DataFrame for analysis
            df = pd.DataFrame(data, columns=columns)
            
            # Perform analysis
            analysis = self._analyze_stock_data(df)
            
            return {
                'success': True,
                'data': df.to_dict('records'),
                'analysis': analysis,
                'total_records': len(data),
                'columns': columns
            }
            
        except Exception as e:
            logger.error(f"Error in comprehensive stock analysis: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _build_comprehensive_query(self, limit=None, article_filter=None):
        """Build the comprehensive SQL query."""
        
        query = f"""
        SELECT 
            -- Stock table data (main reference)
            s.reference_article,
            s.designation_1 as stock_designation,
            s.categorie_article,
            s.quantite_en_stock,
            s.pmp as unit_price,
            s.quantite_en_commande,
            s.seuil_de_reappro_min as min_reorder_level,
            s.quantite_maximum_max as max_stock_level,
            s.stock_securite as safety_stock,
            s.acheteur as buyer,
            s.date_derniere_entree as last_receipt_date,
            s.date_derniere_sortie as last_issue_date,
            s.site,
            s.depôt_de_l_article as warehouse_location,
            
            -- PO table data (Purchase Orders)
            po.n_commande as po_number,
            po.qté_commandée as po_quantity_ordered,
            po.date_commande as po_order_date,
            po.date_livraison as po_delivery_date,
            po.prix_net as po_net_price,
            po.nom_founisseur as supplier_name,
            po.montant_ht_ligne as po_line_amount,
            po.qté_réceptionnée as po_quantity_received,
            po.ligne_soldée as po_line_closed,
            
            -- PR table data (Purchase Requests)
            pr.no_demande as pr_number,
            pr.quantitã_ua as pr_quantity_requested,
            pr.date_crã_ation as pr_creation_date,
            pr.date_souhaitã_e as pr_requested_date,
            pr.prix_net as pr_net_price,
            pr.raison_sociale as pr_supplier,
            pr.demandeur as pr_requester,
            pr.ligne_signã_e as pr_line_approved,
            
            -- Annual 2024 data
            a24.qty_stk as annual_2024_stock,
            a24.min_qty as annual_2024_min,
            a24.max_qty as annual_2024_max,
            a24.col_2024 as usage_2024,
            a24.avg_23_24 as avg_usage_23_24,
            a24.trending as trend_2024,
            
            -- Annual 2025 data
            a25.qty_stk as annual_2025_stock,
            a25.min_qty as annual_2025_min,
            a25.max_qty as annual_2025_max,
            a25.col_2025 as usage_2025,
            a25.avg_24_25 as avg_usage_24_25,
            a25.trending as trend_2025,
            
            -- Calculated fields
            (s.quantite_en_stock * s.pmp) as stock_value,
            CASE 
                WHEN s.quantite_en_stock <= s.seuil_de_reappro_min THEN 'CRITICAL'
                WHEN s.quantite_en_stock <= (s.seuil_de_reappro_min * 1.2) THEN 'LOW'
                WHEN s.quantite_en_stock >= s.quantite_maximum_max THEN 'EXCESS'
                ELSE 'NORMAL'
            END as stock_status,
            
            CASE 
                WHEN s.quantite_en_stock = 0 THEN 'OUT_OF_STOCK'
                WHEN s.quantite_en_stock > 0 AND s.quantite_en_stock <= s.seuil_de_reappro_min THEN 'REORDER_NEEDED'
                ELSE 'IN_STOCK'
            END as availability_status
            
        FROM Stock s
        
        -- LEFT JOIN with PO table
        LEFT JOIN (
            SELECT DISTINCT 
                code_article,
                n_commande,
                qté_commandée,
                date_commande,
                date_livraison,
                prix_net,
                nom_founisseur,
                montant_ht_ligne,
                qté_réceptionnée,
                ligne_soldée,
                ROW_NUMBER() OVER (PARTITION BY code_article ORDER BY date_commande DESC) as rn
            FROM po 
            WHERE code_article IS NOT NULL
        ) po ON s.reference_article = po.code_article AND po.rn = 1
        
        -- LEFT JOIN with PR table
        LEFT JOIN (
            SELECT DISTINCT
                article,
                no_demande,
                quantitã_ua,
                date_crã_ation,
                date_souhaitã_e,
                prix_net,
                raison_sociale,
                demandeur,
                ligne_signã_e,
                ROW_NUMBER() OVER (PARTITION BY article ORDER BY date_crã_ation DESC) as rn
            FROM PR 
            WHERE article IS NOT NULL
        ) pr ON s.reference_article = pr.article AND pr.rn = 1
        
        -- LEFT JOIN with Annual_2024
        LEFT JOIN Annual_2024 a24 ON s.reference_article = a24.article
        
        -- LEFT JOIN with Annual_2025
        LEFT JOIN Annual_2025 a25 ON s.reference_article = a25.article
        
        WHERE s.reference_article IS NOT NULL
        """
        
        # Add article filter if provided
        if article_filter:
            query += f" AND s.reference_article LIKE '%{article_filter}%'"
        
        # Add ordering
        query += " ORDER BY s.reference_article"
        
        # Add limit if provided
        if limit:
            query += f" LIMIT {limit}"
        
        return query
    
    def _analyze_stock_data(self, df):
        """Perform comprehensive analysis on the stock data."""
        if df.empty:
            return {}
        
        analysis = {}
        
        # Basic statistics
        analysis['summary'] = {
            'total_items': len(df),
            'total_stock_value': df['stock_value'].sum() if 'stock_value' in df.columns else 0,
            'avg_stock_value': df['stock_value'].mean() if 'stock_value' in df.columns else 0,
            'categories_count': df['categorie_article'].nunique() if 'categorie_article' in df.columns else 0
        }
        
        # Stock status distribution
        if 'stock_status' in df.columns:
            status_counts = df['stock_status'].value_counts().to_dict()
            analysis['stock_status_distribution'] = status_counts
        
        # Availability status distribution
        if 'availability_status' in df.columns:
            availability_counts = df['availability_status'].value_counts().to_dict()
            analysis['availability_distribution'] = availability_counts
        
        # Category analysis
        if 'categorie_article' in df.columns:
            category_stats = df.groupby('categorie_article').agg({
                'stock_value': ['sum', 'count', 'mean'],
                'quantite_en_stock': 'sum'
            }).round(2)
            
            # Flatten column names
            category_stats.columns = ['_'.join(col).strip() for col in category_stats.columns]
            analysis['category_analysis'] = category_stats.to_dict('index')
        
        # Critical items analysis
        if 'stock_status' in df.columns:
            critical_items = df[df['stock_status'] == 'CRITICAL']
            analysis['critical_items'] = {
                'count': len(critical_items),
                'total_value': critical_items['stock_value'].sum() if 'stock_value' in critical_items.columns else 0,
                'items': critical_items[['reference_article', 'stock_designation', 'quantite_en_stock', 'min_reorder_level']].to_dict('records')[:10]
            }
        
        # Top value items
        if 'stock_value' in df.columns:
            top_value_items = df.nlargest(10, 'stock_value')
            analysis['top_value_items'] = top_value_items[['reference_article', 'stock_designation', 'stock_value', 'quantite_en_stock']].to_dict('records')
        
        # Purchase order analysis
        po_data = df[df['po_number'].notna()]
        if not po_data.empty:
            analysis['purchase_orders'] = {
                'items_with_po': len(po_data),
                'total_po_value': po_data['po_line_amount'].sum() if 'po_line_amount' in po_data.columns else 0,
                'pending_pos': len(po_data[po_data['po_line_closed'] != 'Oui']) if 'po_line_closed' in po_data.columns else 0
            }
        
        # Purchase request analysis
        pr_data = df[df['pr_number'].notna()]
        if not pr_data.empty:
            analysis['purchase_requests'] = {
                'items_with_pr': len(pr_data),
                'approved_prs': len(pr_data[pr_data['pr_line_approved'] == 'Oui']) if 'pr_line_approved' in pr_data.columns else 0,
                'pending_prs': len(pr_data[pr_data['pr_line_approved'] != 'Oui']) if 'pr_line_approved' in pr_data.columns else 0
            }
        
        # Annual trends analysis
        annual_data = df[df['usage_2024'].notna() | df['usage_2025'].notna()]
        if not annual_data.empty:
            analysis['annual_trends'] = {
                'items_with_annual_data': len(annual_data),
                'average_usage_2024': annual_data['usage_2024'].mean() if 'usage_2024' in annual_data.columns else 0,
                'average_usage_2025': annual_data['usage_2025'].mean() if 'usage_2025' in annual_data.columns else 0,
                'trending_up': len(annual_data[annual_data['trend_2025'] == 'UP']) if 'trend_2025' in annual_data.columns else 0,
                'trending_down': len(annual_data[annual_data['trend_2025'] == 'DOWN']) if 'trend_2025' in annual_data.columns else 0
            }
        
        return analysis
    
    def get_article_details(self, reference_article):
        """Get detailed information for a specific article."""
        try:
            query = self._build_comprehensive_query(limit=1, article_filter=reference_article)
            result = db.session.execute(text(query))
            columns = list(result.keys())
            data = result.fetchone()
            
            if data:
                return {
                    'success': True,
                    'data': dict(zip(columns, data))
                }
            else:
                return {
                    'success': False,
                    'error': 'Article not found'
                }
                
        except Exception as e:
            logger.error(f"Error getting article details: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_stock_alerts(self):
        """Get stock alerts for items requiring attention."""
        try:
            query = """
            SELECT 
                reference_article,
                designation_1,
                quantite_en_stock,
                seuil_de_reappro_min,
                quantite_maximum_max,
                (quantite_en_stock * pmp) as stock_value,
                CASE 
                    WHEN quantite_en_stock = 0 THEN 'OUT_OF_STOCK'
                    WHEN quantite_en_stock <= seuil_de_reappro_min THEN 'REORDER_NEEDED'
                    WHEN quantite_en_stock >= quantite_maximum_max THEN 'EXCESS_STOCK'
                    ELSE 'NORMAL'
                END as alert_type,
                acheteur as buyer
            FROM Stock 
            WHERE quantite_en_stock = 0 
               OR quantite_en_stock <= seuil_de_reappro_min 
               OR quantite_en_stock >= quantite_maximum_max
            ORDER BY 
                CASE 
                    WHEN quantite_en_stock = 0 THEN 1
                    WHEN quantite_en_stock <= seuil_de_reappro_min THEN 2
                    WHEN quantite_en_stock >= quantite_maximum_max THEN 3
                    ELSE 4
                END,
                stock_value DESC
            """
            
            result = db.session.execute(text(query))
            columns = list(result.keys())
            data = result.fetchall()
            
            alerts = []
            for row in data:
                alerts.append(dict(zip(columns, row)))
            
            # Group by alert type
            grouped_alerts = {}
            for alert in alerts:
                alert_type = alert['alert_type']
                if alert_type not in grouped_alerts:
                    grouped_alerts[alert_type] = []
                grouped_alerts[alert_type].append(alert)
            
            return {
                'success': True,
                'alerts': grouped_alerts,
                'total_alerts': len(alerts)
            }
            
        except Exception as e:
            logger.error(f"Error getting stock alerts: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def search_articles(self, search_term, search_field='reference_article'):
        """Search for articles based on various criteria."""
        try:
            valid_fields = ['reference_article', 'designation_1', 'categorie_article']
            if search_field not in valid_fields:
                search_field = 'reference_article'
            
            query = f"""
            SELECT 
                reference_article,
                designation_1,
                categorie_article,
                quantite_en_stock,
                pmp,
                (quantite_en_stock * pmp) as stock_value,
                CASE 
                    WHEN quantite_en_stock <= seuil_de_reappro_min THEN 'CRITICAL'
                    WHEN quantite_en_stock <= (seuil_de_reappro_min * 1.2) THEN 'LOW'
                    WHEN quantite_en_stock >= quantite_maximum_max THEN 'EXCESS'
                    ELSE 'NORMAL'
                END as stock_status
            FROM Stock 
            WHERE {search_field} LIKE '%{search_term}%'
            ORDER BY reference_article
            LIMIT 100
            """
            
            result = db.session.execute(text(query))
            columns = list(result.keys())
            data = result.fetchall()
            
            results = []
            for row in data:
                results.append(dict(zip(columns, row)))
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
            
        except Exception as e:
            logger.error(f"Error searching articles: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def export_stock_analysis(self, format='excel', limit=None):
        """Export stock analysis data to Excel or CSV."""
        try:
            analysis_result = self.get_comprehensive_stock_analysis(limit=limit)
            
            if not analysis_result['success']:
                return analysis_result
            
            df = pd.DataFrame(analysis_result['data'])
            
            if format.lower() == 'excel':
                filename = f'stock_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                filepath = f'static/exports/{filename}'
                
                # Create the exports directory if it doesn't exist
                import os
                os.makedirs('static/exports', exist_ok=True)
                
                with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                    df.to_excel(writer, sheet_name='Stock Analysis', index=False)
                    
                    # Add analysis summary to a separate sheet
                    if analysis_result.get('analysis'):
                        summary_df = pd.DataFrame.from_dict(analysis_result['analysis']['summary'], orient='index', columns=['Value'])
                        summary_df.to_excel(writer, sheet_name='Summary')
                
                return {
                    'success': True,
                    'filename': filename,
                    'filepath': filepath,
                    'records_exported': len(df)
                }
            
            elif format.lower() == 'csv':
                filename = f'stock_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
                filepath = f'static/exports/{filename}'
                
                # Create the exports directory if it doesn't exist
                import os
                os.makedirs('static/exports', exist_ok=True)
                
                df.to_csv(filepath, index=False, encoding='utf-8-sig')
                
                return {
                    'success': True,
                    'filename': filename,
                    'filepath': filepath,
                    'records_exported': len(df)
                }
            
            else:
                return {
                    'success': False,
                    'error': 'Unsupported format. Use "excel" or "csv".'
                }
                
        except Exception as e:
            logger.error(f"Error exporting stock analysis: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
