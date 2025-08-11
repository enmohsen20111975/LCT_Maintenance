"""
Database Schema Validation Utility for LCT STS Maintenance App
"""

from models import db
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def validate_database_schema():
    """
    Validate that all required columns exist in critical database tables.
    Returns a dict with validation results.
    """
    
    # Define required columns for critical tables
    required_columns = {
        'PO': [
            'id', 'n_commande', 'code_article', 'designation', 'qte_commandee',
            'prix_unitaire', 'montant_ligne', 'fournisseur', 'statut',
            'date_commande', 'date_prevue_livraison'
        ],
        'PR': [
            'id', 'no_demande', 'article', 'designation', 'quantita_ua',
            'prix_unitaire', 'prix_total', 'statut', 'date_demande',
            'demandeur', 'justification'
        ],
        'Stock': [
            'id', 'reference_article', 'designation_1', 'categorie_article',
            'quantite_en_stock', 'seuil_de_reappro_min', 'pmp'
        ]
    }
    
    validation_results = {
        'valid': True,
        'issues': [],
        'tables_checked': [],
        'warnings': []
    }
    
    for table_name, required_cols in required_columns.items():
        try:
            # Get table schema
            result = db.session.execute(text(f'PRAGMA table_info({table_name})'))
            existing_columns = [col[1] for col in result.fetchall()]
            
            validation_results['tables_checked'].append(table_name)
            
            # Check for missing columns
            missing_columns = [col for col in required_cols if col not in existing_columns]
            
            if missing_columns:
                validation_results['valid'] = False
                validation_results['issues'].append({
                    'table': table_name,
                    'type': 'missing_columns',
                    'columns': missing_columns,
                    'message': f'Table {table_name} is missing required columns: {", ".join(missing_columns)}'
                })
            
            # Check for extra columns that might indicate schema drift
            extra_columns = [col for col in existing_columns if col not in required_cols]
            if extra_columns:
                validation_results['warnings'].append({
                    'table': table_name,
                    'type': 'extra_columns',
                    'columns': extra_columns,
                    'message': f'Table {table_name} has additional columns not in validation schema: {", ".join(extra_columns[:5])}{"..." if len(extra_columns) > 5 else ""}'
                })
                
        except Exception as e:
            validation_results['valid'] = False
            validation_results['issues'].append({
                'table': table_name,
                'type': 'table_access_error',
                'error': str(e),
                'message': f'Could not access table {table_name}: {str(e)}'
            })
    
    return validation_results

def fix_missing_columns():
    """
    Automatically fix missing columns in database tables.
    Returns a dict with fix results.
    """
    
    fix_results = {
        'success': True,
        'fixes_applied': [],
        'errors': []
    }
    
    # Column fixes to apply
    column_fixes = {
        'PO': {
            'designation': 'TEXT',
        },
        'PR': {
            'designation': 'TEXT',
            'quantita_ua': 'INTEGER DEFAULT 0'
        }
    }
    
    for table_name, columns in column_fixes.items():
        for column_name, column_type in columns.items():
            try:
                # Check if column exists
                result = db.session.execute(text(f"SELECT {column_name} FROM {table_name} LIMIT 1"))
                logger.info(f"Column {column_name} already exists in {table_name}")
                
            except Exception:
                # Column doesn't exist, add it
                try:
                    db.session.execute(text(f'ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}'))
                    db.session.commit()
                    
                    fix_results['fixes_applied'].append({
                        'table': table_name,
                        'column': column_name,
                        'type': column_type,
                        'action': 'added'
                    })
                    
                    logger.info(f"Added column {column_name} to table {table_name}")
                    
                except Exception as e:
                    fix_results['success'] = False
                    fix_results['errors'].append({
                        'table': table_name,
                        'column': column_name,
                        'error': str(e)
                    })
                    logger.error(f"Failed to add column {column_name} to {table_name}: {e}")
    
    return fix_results

def test_critical_queries():
    """
    Test critical queries to ensure they work with current schema.
    Returns a dict with test results.
    """
    
    test_results = {
        'success': True,
        'tests_passed': [],
        'tests_failed': []
    }
    
    # Critical queries to test
    test_queries = {
        'purchase_orders': """
            SELECT id, n_commande, code_article, designation, qte_commandee,
                   prix_unitaire, montant_ligne, fournisseur, statut,
                   date_commande, date_prevue_livraison
            FROM PO LIMIT 1
        """,
        'purchase_requests': """
            SELECT id, no_demande, article, designation, quantita_ua,
                   prix_unitaire, prix_total, statut, date_demande,
                   demandeur, justification
            FROM PR LIMIT 1
        """,
        'stock_inventory': """
            SELECT id, reference_article, designation_1, categorie_article,
                   quantite_en_stock, seuil_de_reappro_min, pmp
            FROM Stock LIMIT 1
        """
    }
    
    for test_name, query in test_queries.items():
        try:
            result = db.session.execute(text(query))
            result.fetchone()  # Try to fetch one row
            
            test_results['tests_passed'].append({
                'test': test_name,
                'query': query,
                'status': 'passed'
            })
            
        except Exception as e:
            test_results['success'] = False
            test_results['tests_failed'].append({
                'test': test_name,
                'query': query,
                'error': str(e),
                'status': 'failed'
            })
    
    return test_results

def run_database_health_check():
    """
    Run a comprehensive database health check.
    Returns a complete health report.
    """
    
    health_report = {
        'timestamp': None,
        'overall_status': 'healthy',
        'validation': None,
        'query_tests': None,
        'recommendations': []
    }
    
    from datetime import datetime
    health_report['timestamp'] = datetime.now().isoformat()
    
    # Run validation
    health_report['validation'] = validate_database_schema()
    if not health_report['validation']['valid']:
        health_report['overall_status'] = 'issues_found'
    
    # Run query tests
    health_report['query_tests'] = test_critical_queries()
    if not health_report['query_tests']['success']:
        health_report['overall_status'] = 'critical_issues'
    
    # Generate recommendations
    if health_report['validation']['issues']:
        health_report['recommendations'].append(
            "Run fix_missing_columns() to automatically fix missing columns"
        )
    
    if health_report['validation']['warnings']:
        health_report['recommendations'].append(
            "Review extra columns in tables - they may indicate schema drift"
        )
    
    if health_report['query_tests']['tests_failed']:
        health_report['recommendations'].append(
            "Critical queries are failing - immediate attention required"
        )
    
    return health_report
