"""
AI-Powered Work Order Analysis Service
Analyzes WO_name and description fields to extract main data points and identify repeated patterns
"""

import sqlite3
import os
import re
import logging
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Any
import json

logger = logging.getLogger(__name__)

class AIWorkOrderAnalysisService:
    """AI service for analyzing work order names and descriptions."""
    
    def __init__(self):
        """Initialize the AI analysis service."""
        self.workorder_db_path = 'instance/Workorder.db'
        self.excel_db_path = 'instance/excel_data.db'
        
        # Common stop words for filtering
        self.stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did',
            'will', 'would', 'could', 'should', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those'
        }
        
        # Equipment patterns with French/English terms
        self.equipment_patterns = {
            'crane': ['crane', 'sts', 'gantry', 'grue'],
            'spreader': ['spreader', 'spr', 'palonnier'],
            'hoist': ['hoist', 'winch', 'treuil', 'palan'],
            'trolley': ['trolley', 'trol', 'chariot'],
            'boom': ['boom', 'jib', 'flèche'],
            'motor': ['motor', 'drive', 'moteur', 'entraînement'],
            'brake': ['brake', 'frein', 'freinage'],
            'cable': ['cable', 'wire', 'câble', 'fil'],
            'bearing': ['bearing', 'roulement', 'palier'],
            'pump': ['pump', 'pompe'],
            'valve': ['valve', 'vanne', 'soupape'],
            'sensor': ['sensor', 'capteur', 'détecteur'],
            'light': ['light', 'lighting', 'lampe', 'éclairage', 'luminaire'],
            'panel': ['panel', 'panneau', 'electrical', 'électrique'],
            'structure': ['structure', 'beam', 'frame', 'support', 'poutre', 'châssis'],
            'hydraulic': ['hydraulic', 'hydraulique', 'hydr', 'hyd'],
            'electrical': ['electrical', 'électrique', 'ele', 'electr'],
            'flipper': ['flipper', 'flp'],
            'trolley_system': ['trolley', 'twl', 'chariot'],
            'headblock': ['headblock', 'hdb', 'moufle'],
            'main_hoist': ['hoist', 'mnh', 'treuil_principal']
        }
        
        # Maintenance action patterns with French/English terms
        self.action_patterns = {
            'inspection': ['inspect', 'inspection', 'check', 'verify', 'control', 'contrôle', 'vérifi', 'controle'],
            'repair': ['repair', 'fix', 'répar', 'remplac', 'replace', 'reparation'],
            'maintenance': ['maintenance', 'service', 'entretien', 'révision', 'mainten'],
            'cleaning': ['clean', 'cleaning', 'nettoyage', 'wash', 'lavage'],
            'painting': ['paint', 'painting', 'peinture', 'rust', 'corrosion', 'rouille'],
            'installation': ['install', 'mount', 'setup', 'installation', 'pose', 'montage'],
            'calibration': ['calibrat', 'adjust', 'setting', 'réglage', 'ajustement'],
            'lubrication': ['lubricat', 'grease', 'oil', 'graissage', 'huile', 'graisse'],
            'testing': ['test', 'testing', 'essai', 'trial', 'test'],
            'upgrade': ['upgrade', 'update', 'amélioration', 'modernisation'],
            'welding': ['welding', 'soudure', 'soudage'],
            'assembly': ['assembly', 'assemblage', 'montage'],
            'disassembly': ['disassembly', 'démontage', 'demontage'],
            'replacement': ['replacement', 'remplacement', 'changement']
        }
        
        # Problem/fault patterns with French/English terms
        self.problem_patterns = {
            'fault': ['fault', 'failure', 'defect', 'défaut', 'panne', 'defaillance'],
            'leak': ['leak', 'leakage', 'fuite', 'écoulement'],
            'noise': ['noise', 'sound', 'bruit', 'son'],
            'vibration': ['vibration', 'shake', 'vibrating', 'tremblement'],
            'overheating': ['overheat', 'hot', 'temperature', 'surchauffe', 'chaud'],
            'wear': ['wear', 'worn', 'usure', 'usé', 'détérioré'],
            'crack': ['crack', 'fissure', 'break', 'cassure', 'rupture'],
            'loose': ['loose', 'slack', 'desserré', 'relâché'],
            'blocked': ['block', 'stuck', 'jam', 'bloqué', 'coincé'],
            'misalignment': ['misalign', 'alignment', 'désalignement', 'décentrage'],
            'corrosion': ['corrosion', 'rust', 'rouille', 'oxydation'],
            'breakdown': ['breakdown', 'panne', 'arrêt', 'défaillance']
        }
        
        # French to English translations for common terms
        self.french_translations = {
            'réparation': 'repair',
            'remplacement': 'replacement',
            'maintenance': 'maintenance',
            'inspection': 'inspection',
            'nettoyage': 'cleaning',
            'contrôle': 'control/check',
            'vérification': 'verification',
            'défaut': 'defect/fault',
            'panne': 'breakdown',
            'fuite': 'leak',
            'usure': 'wear',
            'graissage': 'lubrication',
            'éclairage': 'lighting',
            'électrique': 'electrical',
            'hydraulique': 'hydraulic',
            'mécanique': 'mechanical',
            'câble': 'cable',
            'moteur': 'motor',
            'frein': 'brake',
            'capteur': 'sensor',
            'valve': 'valve',
            'pompe': 'pump',
            'roulement': 'bearing',
            'joint': 'seal/gasket',
            'tuyau': 'pipe/hose',
            'courroie': 'belt',
            'chaîne': 'chain',
            'vis': 'screw',
            'boulon': 'bolt',
            'écrou': 'nut',
            'ressort': 'spring',
            'filtre': 'filter',
            'huile': 'oil',
            'graisse': 'grease',
            'peinture': 'paint',
            'corrosion': 'corrosion',
            'rouille': 'rust',
            'assemblage': 'assembly',
            'démontage': 'disassembly',
            'montage': 'assembly/installation',
            'réglage': 'adjustment',
            'calibrage': 'calibration',
            'essai': 'test',
            'mesure': 'measurement',
            'soudure': 'welding'
        }
    
    def get_database_connection(self, db_path: str) -> sqlite3.Connection:
        """Get database connection."""
        if not os.path.exists(db_path):
            raise FileNotFoundError(f"Database not found: {db_path}")
        return sqlite3.connect(db_path)
    
    def extract_work_order_data(self, limit: int = None, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Extract work order data from all available tables with filtering capabilities."""
        work_orders = []
        filters = filters or {}
        
        # Extract from Workorder.db
        if os.path.exists(self.workorder_db_path):
            conn = self.get_database_connection(self.workorder_db_path)
            cursor = conn.cursor()
            
            # Build filter conditions
            def build_where_clause(filters):
                conditions = []
                params = []
                
                if filters.get('job_types'):
                    job_types = filters['job_types']
                    placeholders = ','.join(['?' for _ in job_types])
                    conditions.append(f"job_type IN ({placeholders})")
                    params.extend(job_types)
                
                if filters.get('equipment_categories'):
                    equipment_cats = filters['equipment_categories']
                    equipment_conditions = []
                    for cat in equipment_cats:
                        if cat == 'STS_ALL':
                            equipment_conditions.append("mo_key LIKE 'STS%'")
                        elif cat == 'SPS_ALL':
                            equipment_conditions.append("mo_key LIKE 'SPS%'")
                        elif cat.startswith('STS') or cat.startswith('SPS'):
                            equipment_conditions.append("mo_key LIKE ?")
                            params.append(f"{cat}%")
                        else:
                            equipment_conditions.append("mo_key LIKE ?")
                            params.append(f"%{cat}%")
                    if equipment_conditions:
                        conditions.append(f"({' OR '.join(equipment_conditions)})")
                
                if filters.get('date_from'):
                    conditions.append("order_date >= ?")
                    params.append(filters['date_from'])
                
                if filters.get('date_to'):
                    conditions.append("order_date <= ?")
                    params.append(filters['date_to'])
                
                if filters.get('equipment_specific'):
                    equipment_specific = filters['equipment_specific']
                    equipment_conditions = []
                    for eq in equipment_specific:
                        equipment_conditions.append("mo_key = ?")
                        params.append(eq)
                    if equipment_conditions:
                        conditions.append(f"({' OR '.join(equipment_conditions)})")
                
                where_clause = ""
                if conditions:
                    where_clause = "WHERE " + " AND ".join(conditions)
                
                return where_clause, params
            
            # Extract from all_cm (primary source with most data)
            try:
                where_clause, params = build_where_clause(filters)
                query = f"SELECT wo_key, wo_name, description, job_type, mo_key, work_supplier_key, order_date, jobexec_dt FROM all_cm {where_clause}"
                
                if limit:
                    query += f" ORDER BY order_date DESC LIMIT {limit}"
                else:
                    query += " ORDER BY order_date DESC"
                
                cursor.execute(query, params)
                for row in cursor.fetchall():
                    work_orders.append({
                        'source': 'all_cm',
                        'wo_key': str(row[0]) if row[0] else '',
                        'wo_name': row[1] or '',
                        'description': row[2] or '',
                        'job_type': row[3] or '',
                        'equipment': row[4] or '',
                        'supplier': row[5] or '',
                        'order_date': row[6] or '',
                        'execution_date': row[7] or ''
                    })
            except Exception as e:
                logger.warning(f"Error extracting from all_cm: {e}")
            
            # Extract from wo_active if needed and no specific table filtering
            if not filters.get('table_source') or 'wo_active' in filters.get('table_source', []):
                try:
                    # Adapt filters for wo_active table structure
                    wo_active_filters = {k: v for k, v in filters.items() if k in ['job_types', 'date_from', 'date_to']}
                    where_clause, params = build_where_clause(wo_active_filters)
                    
                    query = f"SELECT wo_key, wo_name, description, job_type, pos_key, work_supplier_key, order_date FROM wo_active {where_clause}"
                    
                    if limit and len(work_orders) < limit:
                        remaining_limit = limit - len(work_orders)
                        query += f" ORDER BY order_date DESC LIMIT {remaining_limit}"
                    else:
                        query += " ORDER BY order_date DESC"
                    
                    cursor.execute(query, params)
                    for row in cursor.fetchall():
                        work_orders.append({
                            'source': 'wo_active',
                            'wo_key': str(row[0]) if row[0] else '',
                            'wo_name': row[1] or '',
                            'description': row[2] or '',
                            'job_type': row[3] or '',
                            'equipment': row[4] or '',
                            'supplier': row[5] or '',
                            'order_date': row[6] or '',
                            'execution_date': ''
                        })
                except Exception as e:
                    logger.warning(f"Error extracting from wo_active: {e}")
            
            conn.close()
        
        return work_orders
    
    def translate_french_terms(self, text: str) -> str:
        """Translate common French terms to English for better analysis."""
        if not text:
            return ""
        
        translated_text = text.lower()
        
        # Apply translations
        for french_term, english_term in self.french_translations.items():
            translated_text = translated_text.replace(french_term, f"{french_term}({english_term})")
        
        return translated_text
    
    def get_equipment_category(self, equipment_code: str) -> Dict[str, str]:
        """Categorize equipment code and provide detailed information."""
        if not equipment_code:
            return {'type': 'unknown', 'unit': '', 'component': '', 'description': ''}
        
        code = equipment_code.upper()
        result = {'type': 'unknown', 'unit': '', 'component': '', 'description': ''}
        
        # STS Equipment (Ship-to-Shore Cranes)
        if code.startswith('STS'):
            result['type'] = 'STS_Crane'
            
            # Extract STS number
            sts_match = re.search(r'STS(\d+)', code)
            if sts_match:
                result['unit'] = f"STS{sts_match.group(1)}"
            
            # Identify components
            if 'MNH' in code:
                result['component'] = 'Main_Hoist'
                result['description'] = 'Main Hoist System'
            elif 'HDB' in code:
                result['component'] = 'Head_Block'
                result['description'] = 'Head Block System'
            elif 'ELE' in code:
                result['component'] = 'Electrical'
                result['description'] = 'Electrical System'
            elif 'STR' in code:
                result['component'] = 'Structure'
                result['description'] = 'Structural Components'
            elif 'HYD' in code:
                result['component'] = 'Hydraulic'
                result['description'] = 'Hydraulic System'
            elif 'GAN' in code:
                result['component'] = 'Gantry'
                result['description'] = 'Gantry System'
            elif 'LIG' in code:
                result['component'] = 'Lighting'
                result['description'] = 'Lighting System'
            else:
                result['component'] = 'General'
                result['description'] = 'General STS Equipment'
        
        # SPS Equipment (Ship-to-Shore Spreaders)
        elif code.startswith('SPS'):
            result['type'] = 'SPS_Spreader'
            
            # Extract SPS number
            sps_match = re.search(r'SPS(\d+)', code)
            if sps_match:
                result['unit'] = f"SPS{sps_match.group(1)}"
            
            # Identify components for combined SPS-STS codes
            if 'STS' in code:
                if 'ELE' in code:
                    result['component'] = 'Electrical'
                    result['description'] = 'Spreader Electrical System'
                elif 'HYD' in code:
                    result['component'] = 'Hydraulic'
                    result['description'] = 'Spreader Hydraulic System'
                elif 'FLP' in code:
                    result['component'] = 'Flipper'
                    result['description'] = 'Spreader Flipper System'
                elif 'TWL' in code:
                    result['component'] = 'Trolley'
                    result['description'] = 'Spreader Trolley System'
                elif 'STR' in code:
                    result['component'] = 'Structure'
                    result['description'] = 'Spreader Structure'
                else:
                    result['component'] = 'General'
                    result['description'] = 'General Spreader Equipment'
            else:
                result['component'] = 'Spreader'
                result['description'] = 'Container Spreader'
        
        # SPR Equipment (Spreader specific)
        elif code.startswith('SPR'):
            result['type'] = 'Spreader'
            result['component'] = 'Spreader'
            result['description'] = 'Container Spreader'
        
        return result
    
    def analyze_spreader_fault(self, short_desc: str, machine: str, bdn: str) -> str:
        """Analyze spreader faults based on VBA logic patterns."""
        if not short_desc:
            return ""
        
        d = short_desc.upper().strip()
        causes = ""
        
        # Check if it's spreader-related work
        if ('SPR' in d or 'SPS' in d or 'SPREADER' in d.upper()) or (machine == "SPREADER" and bdn == "CMU"):
            # Priority order from VBA - each check can override the previous
            
            # Twin related (VBA checks this first in priority)
            if 'TWIN' in d:
                causes = "twin"
            
            # Telescopic related
            if 'TELESCO' in d or 'TELESCOPIE' in d or 'TÉLESCOPIE' in d:
                causes = "telescopic"
            
            # Lock/Unlock related (higher priority, can override twin)
            if any(term in d for term in ['UNLOCK', 'SIGNAL', 'LOCK', 'DEVEROUILLAGE', 'DEVERROUILLAGE', 'VERROUILLAGE']):
                causes = "Lock/Unlock"
            
            # Signal check (VBA has separate check for signal = Lock/Unlock)
            if 'SIGNAL' in d:
                causes = "Lock/Unlock"
            
            # Bad container
            if 'BAD CONT' in d or 'CORNER' in d:
                causes = "Bad contenair"
            
            # Change spreader
            if any(term in d for term in ['CHANGEMENT', 'REMPLACE', 'CHANGE']):
                causes = "Change spreader"
            
            # Flipper related (VBA checks this but twin and telescopic have higher priority)
            if 'FLIPPER' in d or 'FLIP' in d:
                # Only set flipper if no higher priority fault was found
                if not causes:
                    causes = "flipper"
        
        return causes
    
    def get_equipment_type(self, machine: str) -> str:
        """Get equipment type based on machine code (from VBA Get_Type function)."""
        if not machine or len(machine) <= 7:
            return "Other"
        
        # Extract 3-character code from the correct position based on VBA logic
        # VBA: s = Left(Right(Machine, Len(Machine) - 5), 3)
        # This means take from position 6 onward, then take first 3 characters
        if len(machine) > 7:
            start_pos = len(machine) - (len(machine) - 5)  # Position 6 (0-indexed: 5)
            s = machine[5:8] if len(machine) > 8 else machine[5:]
        else:
            return "Other"
        
        equipment_types = {
            "MNH": "HOIST",
            "HDB": "SPREADER", 
            "GAN": "GANTRY",
            "ELE": "ELECTRICAL",
            "TRL": "TROLLEY",
            "LIG": "LIGHTING",
            "CAB": "OPERATOR CABIN",
            "HYD": "HYDRAULIC",
            "FES": "FESTOON",
            "ELV": "ELEVATOR",
            "TRM": "TLS"
        }
        
        return equipment_types.get(s.upper(), "Other")
    
    def analyze_fault_causes(self, short_desc: str, machine: str, bdn: str) -> str:
        """Comprehensive fault analysis based on VBA Get_cuases function."""
        if bdn != "CMU":
            return ""
        
        d = f"{short_desc} {machine}".upper().strip()
        causes = ""
        
        # Priority order of fault detection (from VBA logic)
        # Note: VBA uses priority where later checks override earlier ones
        
        # Twin fault (highest priority after spreader analysis)
        if 'TWIN' in d:
            causes = "twin"
        
        # Telescopic fault
        if 'TELESCO' in d or 'TELESCOPIE' in d:
            causes = "telescopic"
        
        # Lock/Unlock faults (check before twin to match VBA priority)
        if any(term in d for term in ['UNLOCK', 'SIGNAL', 'LOCK', 'DEVEROUILLAGE', 'DEVERROUILLAGE', 'VERROUILLAGE']):
            causes = "Lock/Unlock"
            
        # Signal specifically for Lock/Unlock (VBA has this as separate check)
        if 'SIGNAL' in d:
            causes = "Lock/Unlock"
        
        # Bad container
        if 'BAD CONT' in d:
            causes = "Bad contenair"
        
        # Assistance
        if 'ASSIST' in d:
            causes = f"{machine} Assistance"
        
        # Check spreader-specific faults (this can override above)
        spreader_fault = self.analyze_spreader_fault(short_desc, machine, bdn)
        if spreader_fault:
            causes = spreader_fault
        
        # Following faults follow VBA priority order (later overrides earlier)
        
        # Boom fault
        if 'BOOM' in d:
            causes = "Boom"
        
        # Wheel brake - specific pattern
        if 'WHEEL' in d and 'BRAKE' in d:
            causes = "wheel brake"
        
        # Module fault
        if 'MODULE' in d:
            causes = "Module"
        
        # GCR fault
        if 'GCR' in d:
            causes = "GCR"
        
        # SCR fault
        if 'SCR' in d:
            causes = "SCR"
        
        # E-Stop
        if 'E-STOP' in d:
            causes = "E-Stop"
        
        # Overload variants
        if 'OVERLAOD' in d or 'OVER LAOD' in d:  # VBA has typo "overlaod"
            causes = "Over load"
        
        # Crane/Drive off
        if 'CRANE OFF' in d or 'DRIVE OFF' in d:
            causes = "Crane off"
        
        # TLS
        if 'TLS' in d:
            causes = "TLS"
        
        # Overcurrent
        if 'OVERCURRENT' in d or 'OVER CURRENT' in d:
            causes = "Over current"
        
        # Overvoltage
        if 'OVER VOLTAGE' in d or 'OVERVOLTAGE' in d:
            causes = "Over voltage"
        
        # Slowdown
        if 'SLOWDOWN' in d:
            causes = "slowdown"
        
        # Inverter
        if 'INVERT' in d:
            causes = f"{machine} inverter"
        
        # Encoder
        if 'ENCOD' in d:
            causes = "Encoder"
        
        # Communication
        if 'COMMUNICAT' in d:
            if machine in ["ELECTRICAL", "Other"]:
                causes = "Communication"
            else:
                causes = f"{machine} Communication"
        
        # Blink fault - spreader communication (VBA comment shows this is separate)
        if 'BLINK' in d:
            causes = "Spreader Communication"
        
        # Limit switch
        if 'LIMIT SWITCH' in d:
            if machine == "Other":
                causes = "limit switch"
            else:
                causes = f"{machine} limit switch"
        
        # UVA
        if 'UVA' in d:
            causes = "UVA"
        
        # ALM
        if 'ALM' in d:
            causes = "ALM"
        
        # Power cut off/Power off
        if any(term in d for term in ['POWER CUT OFF', 'POWER OFF', 'TRANSFO']):
            if any(term in d for term in ["L'OPÉRATEUR", "ACQUITTEMENT DEFAUT", "OPERATOR", "AUTOMATI"]):
                causes = "Power Off auto restart"
            else:
                causes = "Power Off"
        
        # Eccentric/Unbalance
        if any(term in d for term in ['ECCENTRIC', 'UNBALANCE', 'ECE', 'ECC FAULT']):
            causes = "Eccentric"
        
        # Snag (with specific conditions)
        if 'SNAG' in d and 'CUT' not in d and 'FAULT #' in d:
            causes = "snag"
        
        # Slack
        if 'SLACK' in d:
            causes = "slack"
        
        # Position (excluding telescopic)
        if 'POSITION' in d and 'TELESCOPIE' not in d:
            causes = f"{machine} position"
            if causes == "SPREADER position":
                causes = "Sensor Adjustment"
        
        # Overload (separate from over load) - VBA checks this after position
        if 'OVERLOAD' in d:
            causes = "Overload"
        
        # Stuck
        if 'COINCÉ' in d or 'STUCK' in d:
            causes = "Stuck"
        
        # Blinking
        if any(term in d for term in ['CLIGNOT', 'CLIGNOTANT', 'BLINKING', 'BLINK']):
            causes = "blinking"
        
        # Roof detected
        if 'ROOF' in d:
            causes = "Roof detected"
        
        # Brake (general)
        if 'BRAKE' in d or 'FREIN' in d:
            if machine == "Other":
                causes = "Brake"
            else:
                causes = f"{machine} Brake"
        
        # Breaker/Trip
        if 'BRAKER' in d or 'TRIP' in d:
            if machine == "Other":
                causes = "Braker"
            else:
                causes = f"{machine} Braker"
        
        # Default to machine type if no specific cause found
        if not causes or len(causes) < 2:
            causes = machine
        
        return causes
    
    def extract_crane_id(self, crane_name: str) -> str:
        """Extract crane ID from crane name (from VBA getCraneID function)."""
        if not crane_name or len(crane_name) < 5:
            return ""
        
        crane_id = crane_name[:5].upper()
        
        # Check if it's a valid STS crane ID
        if (len(crane_id) > 3 and 
            crane_id[3] in ['0', '1'] and 
            crane_id.startswith('STS')):
            
            try:
                crane_number = int(crane_id[3:5])
                if crane_number >= 1:
                    return crane_id
            except ValueError:
                pass
        
        return ""
    
    def extract_spreader_number(self, short_desc: str, machine: str) -> str:
        """Extract spreader number from description (from VBA SpreaderNumber function)."""
        if self.get_equipment_type(machine) != "SPREADER":
            return ""
        
        d = short_desc.upper().strip()
        if len(d) < 6:
            return "non"
        
        spnr = d[:6]  # First 6 characters
        
        if spnr.startswith('SPR') or spnr.startswith('SPS'):
            try:
                # Extract the 3-digit number after SPR or SPS
                number_part = spnr[3:6]
                number = int(number_part)
                if number > 200:
                    return str(number)
                else:
                    return "non"
            except (ValueError, IndexError):
                return "non"
        
        return "non"
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text data."""
        if not text:
            return ""
        
        # Convert to lowercase
        text = text.lower().strip()
        
        # Remove special characters but keep spaces and basic punctuation
        text = re.sub(r'[^\w\s\-\/\.]', ' ', text)
        
        # Remove multiple spaces
        text = re.sub(r'\s+', ' ', text)
        
        # Remove leading/trailing whitespace
        text = text.strip()
        
        return text
    
    def extract_keywords(self, text: str, min_length: int = 3) -> List[str]:
        """Extract meaningful keywords from text."""
        if not text:
            return []
        
        cleaned_text = self.clean_text(text)
        
        # Split into words
        words = cleaned_text.split()
        
        # Filter words
        keywords = []
        for word in words:
            if (len(word) >= min_length and 
                word not in self.stop_words and
                not word.isdigit()):
                keywords.append(word)
        
        return keywords
    
    def categorize_text(self, text: str) -> Dict[str, List[str]]:
        """Categorize text into equipment, actions, and problems."""
        if not text:
            return {'equipment': [], 'actions': [], 'problems': []}
        
        cleaned_text = self.clean_text(text)
        categories = {'equipment': [], 'actions': [], 'problems': []}
        
        # Check for equipment patterns
        for category, patterns in self.equipment_patterns.items():
            for pattern in patterns:
                if pattern in cleaned_text:
                    categories['equipment'].append(category)
                    break
        
        # Check for action patterns
        for category, patterns in self.action_patterns.items():
            for pattern in patterns:
                if pattern in cleaned_text:
                    categories['actions'].append(category)
                    break
        
        # Check for problem patterns
        for category, patterns in self.problem_patterns.items():
            for pattern in patterns:
                if pattern in cleaned_text:
                    categories['problems'].append(category)
                    break
        
        return categories
    
    def extract_main_data_points(self, work_orders: List[Dict[str, Any]], filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Extract main data points from work order names and descriptions."""
        filters = filters or {}
        
        analysis_results = {
            'total_work_orders': len(work_orders),
            'analysis_timestamp': datetime.now().isoformat(),
            'filters_applied': filters,
            'keyword_frequency': Counter(),
            'equipment_frequency': Counter(),
            'action_frequency': Counter(),
            'problem_frequency': Counter(),
            'job_type_distribution': Counter(),
            'supplier_distribution': Counter(),
            'equipment_distribution': Counter(),
            'common_phrases': Counter(),
            'word_combinations': Counter(),
            'timeline_distribution': Counter(),
            'monthly_distribution': Counter(),
            'yearly_distribution': Counter(),
            'sts_specific_analysis': {},
            'category_insights': {},
            'description_patterns': [],
            'name_patterns': [],
            'vba_fault_analysis': Counter(),  # New: VBA-based fault analysis
            'spreader_fault_analysis': Counter(),  # New: Spreader-specific faults
            'equipment_type_analysis': Counter(),  # New: Equipment type distribution
            'crane_id_analysis': Counter()  # New: Crane ID analysis
        }
        
        all_keywords = []
        all_phrases = []
        sts_data = defaultdict(lambda: {
            'keywords': Counter(),
            'problems': Counter(),
            'actions': Counter(),
            'job_types': Counter(),
            'vba_faults': Counter(),  # New: VBA fault analysis per equipment
            'spreader_faults': Counter()  # New: Spreader fault analysis per equipment
        })
        
        for wo in work_orders:
            # Combine wo_name and description for comprehensive analysis
            combined_text = f"{wo['wo_name']} {wo['description']}"
            
            # Add French translation for enhanced analysis
            translated_text = self.translate_french_terms(combined_text)
            analysis_text = f"{combined_text} {translated_text}"
            
            # VBA-based fault analysis
            machine_type = self.get_equipment_type(wo['equipment']) if wo['equipment'] else "Other"
            analysis_results['equipment_type_analysis'][machine_type] += 1
            
            # Analyze faults using VBA logic
            if wo['wo_name'] and wo['equipment']:
                fault_cause = self.analyze_fault_causes(wo['wo_name'], machine_type, 'CMU')
                if fault_cause:
                    analysis_results['vba_fault_analysis'][fault_cause] += 1
                
                # Spreader-specific fault analysis
                spreader_fault = self.analyze_spreader_fault(wo['wo_name'], machine_type, 'CMU')
                if spreader_fault:
                    analysis_results['spreader_fault_analysis'][spreader_fault] += 1
                
                # Extract crane ID if applicable
                crane_id = self.extract_crane_id(wo['equipment'])
                if crane_id:
                    analysis_results['crane_id_analysis'][crane_id] += 1
            
            # Extract keywords from both original and translated text
            keywords = self.extract_keywords(analysis_text)
            all_keywords.extend(keywords)
            analysis_results['keyword_frequency'].update(keywords)
            
            # Categorize content using enhanced patterns
            categories = self.categorize_text(analysis_text)
            analysis_results['equipment_frequency'].update(categories['equipment'])
            analysis_results['action_frequency'].update(categories['actions'])
            analysis_results['problem_frequency'].update(categories['problems'])
            
            # Enhanced equipment categorization
            if wo['equipment']:
                eq_category = self.get_equipment_category(wo['equipment'])
                analysis_results['equipment_distribution'][wo['equipment']] += 1
                
                # Add equipment type categorization
                if eq_category['type'] != 'unknown':
                    analysis_results['equipment_frequency'][eq_category['type']] += 1
                if eq_category['component']:
                    analysis_results['equipment_frequency'][eq_category['component']] += 1
            
            # Track job types and suppliers
            if wo['job_type']:
                analysis_results['job_type_distribution'][wo['job_type']] += 1
            if wo['supplier']:
                analysis_results['supplier_distribution'][wo['supplier']] += 1
            
            # Timeline analysis
            if wo['order_date']:
                try:
                    if isinstance(wo['order_date'], str):
                        # Parse date string
                        date_obj = datetime.fromisoformat(wo['order_date'].replace('Z', '+00:00'))
                    else:
                        date_obj = wo['order_date']
                    
                    year = date_obj.year
                    month = f"{year}-{date_obj.month:02d}"
                    quarter = f"{year}-Q{(date_obj.month-1)//3 + 1}"
                    
                    analysis_results['yearly_distribution'][str(year)] += 1
                    analysis_results['monthly_distribution'][month] += 1
                    analysis_results['timeline_distribution'][quarter] += 1
                except Exception as e:
                    logger.debug(f"Date parsing error: {e}")
            
            # STS/SPS-specific analysis (enhanced for both equipment types)
            if wo['equipment'] and ('STS' in wo['equipment'] or 'SPS' in wo['equipment']):
                equipment_unit = wo['equipment']
                
                # Extract base unit (STS01, SPS201, etc.)
                unit_match = re.search(r'(STS\d+|SPS\d+)', equipment_unit)
                if unit_match:
                    base_unit = unit_match.group(1)
                    sts_data[base_unit]['keywords'].update(keywords)
                    sts_data[base_unit]['problems'].update(categories['problems'])
                    sts_data[base_unit]['actions'].update(categories['actions'])
                    
                    # Add VBA fault analysis to equipment data
                    if fault_cause:
                        sts_data[base_unit]['vba_faults'][fault_cause] += 1
                    if spreader_fault:
                        sts_data[base_unit]['spreader_faults'][spreader_fault] += 1
                    
                    if wo['job_type']:
                        sts_data[base_unit]['job_types'][wo['job_type']] += 1
                
                # Also track full equipment code
                sts_data[equipment_unit]['keywords'].update(keywords)
                sts_data[equipment_unit]['problems'].update(categories['problems'])
                sts_data[equipment_unit]['actions'].update(categories['actions'])
                
                # Add VBA fault analysis to full equipment code
                if fault_cause:
                    sts_data[equipment_unit]['vba_faults'][fault_cause] += 1
                if spreader_fault:
                    sts_data[equipment_unit]['spreader_faults'][spreader_fault] += 1
                
                if wo['job_type']:
                    sts_data[equipment_unit]['job_types'][wo['job_type']] += 1
            
            # Extract phrases (2-3 word combinations)
            if len(keywords) >= 2:
                for i in range(len(keywords) - 1):
                    phrase = f"{keywords[i]} {keywords[i+1]}"
                    all_phrases.append(phrase)
                    analysis_results['common_phrases'][phrase] += 1
                    
                    if i < len(keywords) - 2:
                        three_word_phrase = f"{keywords[i]} {keywords[i+1]} {keywords[i+2]}"
                        analysis_results['word_combinations'][three_word_phrase] += 1
        
        # Process STS/SPS-specific data
        analysis_results['sts_specific_analysis'] = {}
        for equipment_unit, data in sts_data.items():
            if sum(data['job_types'].values()) >= 5:  # Only include units with significant data
                eq_category = self.get_equipment_category(equipment_unit)
                analysis_results['sts_specific_analysis'][equipment_unit] = {
                    'total_work_orders': sum(data['job_types'].values()),
                    'equipment_type': eq_category['type'],
                    'unit': eq_category['unit'],
                    'component': eq_category['component'],
                    'description': eq_category['description'],
                    'top_keywords': dict(data['keywords'].most_common(10)),
                    'top_problems': dict(data['problems'].most_common(5)),
                    'top_actions': dict(data['actions'].most_common(5)),
                    'job_type_breakdown': dict(data['job_types']),
                    'vba_fault_analysis': dict(data['vba_faults'].most_common(10)),  # New: VBA fault analysis
                    'spreader_fault_analysis': dict(data['spreader_faults'].most_common(5))  # New: Spreader analysis
                }
        
        # Category-specific insights
        analysis_results['category_insights'] = self._generate_category_insights(work_orders, filters)
        
        return analysis_results
    
    def identify_repeated_patterns(self, work_orders: List[Dict[str, Any]], min_frequency: int = 3) -> Dict[str, Any]:
        """Identify repeated patterns in work order data."""
        patterns = {
            'repeated_names': Counter(),
            'repeated_descriptions': Counter(),
            'repeated_name_patterns': Counter(),
            'repeated_description_patterns': Counter(),
            'template_patterns': [],
            'common_structures': Counter()
        }
        
        # Track exact matches
        for wo in work_orders:
            if wo['wo_name']:
                patterns['repeated_names'][wo['wo_name']] += 1
            if wo['description']:
                patterns['repeated_descriptions'][wo['description']] += 1
        
        # Find pattern structures (using regex for common structures)
        name_structures = []
        description_structures = []
        
        for wo in work_orders:
            # Analyze name structure
            if wo['wo_name']:
                # Replace numbers with X, specific words with patterns
                structure = re.sub(r'\d+', 'X', wo['wo_name'])
                structure = re.sub(r'SPR\d+', 'SPRX', structure)
                structure = re.sub(r'STS\d+', 'STSX', structure)
                name_structures.append(structure)
                patterns['repeated_name_patterns'][structure] += 1
            
            # Analyze description structure
            if wo['description']:
                desc = wo['description'][:100]  # First 100 chars for pattern analysis
                structure = re.sub(r'\d+', 'X', desc)
                structure = re.sub(r'[A-Z]{3,}\d+', 'CODEX', structure)
                description_structures.append(structure)
                patterns['repeated_description_patterns'][structure] += 1
        
        # Filter patterns by minimum frequency
        patterns['repeated_names'] = {k: v for k, v in patterns['repeated_names'].items() if v >= min_frequency}
        patterns['repeated_descriptions'] = {k: v for k, v in patterns['repeated_descriptions'].items() if v >= min_frequency}
        patterns['repeated_name_patterns'] = {k: v for k, v in patterns['repeated_name_patterns'].items() if v >= min_frequency}
        patterns['repeated_description_patterns'] = {k: v for k, v in patterns['repeated_description_patterns'].items() if v >= min_frequency}
        
        return patterns
    
    def generate_insights(self, analysis_results: Dict[str, Any], patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Generate insights and recommendations from the analysis."""
        insights = {
            'summary': {
                'total_work_orders_analyzed': analysis_results['total_work_orders'],
                'unique_keywords_found': len(analysis_results['keyword_frequency']),
                'most_common_equipment': dict(analysis_results['equipment_frequency'].most_common(10)),
                'most_common_actions': dict(analysis_results['action_frequency'].most_common(10)),
                'most_common_problems': dict(analysis_results['problem_frequency'].most_common(10)),
                'filters_applied': analysis_results.get('filters_applied', {}),
                'vba_fault_summary': dict(analysis_results.get('vba_fault_analysis', Counter()).most_common(10)),  # New
                'spreader_fault_summary': dict(analysis_results.get('spreader_fault_analysis', Counter()).most_common(5)),  # New
                'equipment_type_summary': dict(analysis_results.get('equipment_type_analysis', Counter()).most_common(10))  # New
            },
            'top_keywords': dict(analysis_results['keyword_frequency'].most_common(20)),
            'top_phrases': dict(analysis_results['common_phrases'].most_common(15)),
            'job_type_breakdown': dict(analysis_results['job_type_distribution']),
            'supplier_breakdown': dict(analysis_results['supplier_distribution'].most_common(10)),
            'equipment_breakdown': dict(analysis_results['equipment_distribution'].most_common(15)),
            'french_translations': self.french_translations,
            'vba_enhanced_analysis': {  # New section
                'fault_causes': dict(analysis_results.get('vba_fault_analysis', Counter()).most_common(15)),
                'spreader_specific_faults': dict(analysis_results.get('spreader_fault_analysis', Counter()).most_common(10)),
                'equipment_types': dict(analysis_results.get('equipment_type_analysis', Counter())),
                'crane_ids_found': dict(analysis_results.get('crane_id_analysis', Counter()))
            },
            'repeated_elements': {
                'repeated_names': dict(list(patterns['repeated_names'].items())[:10]),
                'repeated_name_patterns': dict(list(patterns['repeated_name_patterns'].items())[:10]),
                'repeated_description_patterns': dict(list(patterns['repeated_description_patterns'].items())[:5])
            },
            'timeline_analysis': {
                'yearly_distribution': dict(analysis_results.get('yearly_distribution', {})),
                'monthly_distribution': dict(analysis_results.get('monthly_distribution', {}))
            },
            'recommendations': self._generate_enhanced_recommendations(analysis_results, patterns)
        }
        
        return insights
    
    def _generate_enhanced_recommendations(self, analysis_results: Dict[str, Any], patterns: Dict[str, Any]) -> List[str]:
        """Generate enhanced recommendations based on the analysis with French context and VBA insights."""
        recommendations = []
        
        # VBA-based fault analysis recommendations
        vba_faults = analysis_results.get('vba_fault_analysis', Counter())
        if vba_faults:
            top_vba_fault = vba_faults.most_common(1)[0] if vba_faults else None
            if top_vba_fault:
                fault_type, count = top_vba_fault
                percentage = (count / analysis_results['total_work_orders']) * 100
                recommendations.append(f"Top fault cause identified: '{fault_type}' ({count} occurrences, {percentage:.1f}% of total work orders)")
                
                # Specific recommendations based on fault type
                if fault_type in ['Lock/Unlock', 'Spreader Communication']:
                    recommendations.append("Consider implementing preventive maintenance for spreader communication and locking systems")
                elif fault_type in ['Overload', 'Over load']:
                    recommendations.append("Review crane load management procedures and operator training")
                elif fault_type in ['Power Off', 'Power Off auto restart']:
                    recommendations.append("Investigate electrical power stability and implement power monitoring systems")
        
        # Spreader-specific fault recommendations
        spreader_faults = analysis_results.get('spreader_fault_analysis', Counter())
        if spreader_faults:
            top_spreader_faults = spreader_faults.most_common(3)
            fault_names = [fault[0] for fault in top_spreader_faults]
            if fault_names:
                recommendations.append(f"Spreader-specific issues detected: {', '.join(fault_names)} - focus on spreader maintenance protocols")
        
        # Equipment type analysis
        equipment_types = analysis_results.get('equipment_type_analysis', Counter())
        if equipment_types:
            total_equipment_work = sum(equipment_types.values())
            for eq_type, count in equipment_types.most_common(3):
                percentage = (count / total_equipment_work) * 100
                if percentage > 30:  # High maintenance equipment
                    recommendations.append(f"High maintenance volume for {eq_type}: {count} work orders ({percentage:.1f}%) - consider condition-based monitoring")
        
        # Crane ID specific recommendations
        crane_ids = analysis_results.get('crane_id_analysis', Counter())
        if crane_ids:
            high_maintenance_cranes = [(crane_id, count) for crane_id, count in crane_ids.items() if count > 50]
            if high_maintenance_cranes:
                crane_list = [f"{crane_id} ({count} WOs)" for crane_id, count in high_maintenance_cranes[:3]]
                recommendations.append(f"High-maintenance STS cranes requiring attention: {', '.join(crane_list)}")
        
        # Equipment-based recommendations
        top_equipment = analysis_results['equipment_frequency'].most_common(3)
        if top_equipment:
            equipment_names = [eq[0] for eq in top_equipment]
            recommendations.append(f"Focus maintenance planning on most frequent equipment: {', '.join(equipment_names)}")
        
        # STS vs SPS analysis
        sts_count = sum(1 for wo in analysis_results.get('equipment_distribution', {}) if 'STS' in str(wo))
        sps_count = sum(1 for wo in analysis_results.get('equipment_distribution', {}) if 'SPS' in str(wo))
        
        if sts_count > 0 and sps_count > 0:
            total_equipment = sts_count + sps_count
            sts_percentage = (sts_count / total_equipment) * 100
            recommendations.append(f"Equipment distribution: {sts_percentage:.1f}% STS Cranes vs {100-sts_percentage:.1f}% SPS Spreaders")
        
        # Action-based recommendations with French context
        top_actions = analysis_results['action_frequency'].most_common(3)
        if top_actions:
            action_names = [action[0] for action in top_actions]
            recommendations.append(f"Common maintenance activities: {', '.join(action_names)} - consider standardizing procedures")
            
            # Add French translation note
            french_actions = []
            for action, _ in top_actions:
                if action in ['repair', 'maintenance', 'inspection']:
                    french_equivalents = {'repair': 'réparation', 'maintenance': 'maintenance', 'inspection': 'inspection'}
                    if action in french_equivalents:
                        french_actions.append(f"{action} (French: {french_equivalents[action]})")
            
            if french_actions:
                recommendations.append(f"Note: Key French terms identified - {', '.join(french_actions)}")
        
        # Problem pattern recommendations with French context
        top_problems = analysis_results['problem_frequency'].most_common(3)
        if top_problems:
            problem_names = [prob[0] for prob in top_problems]
            recommendations.append(f"Recurring problems identified: {', '.join(problem_names)} - investigate root causes")
            
            # Check for French problem terms
            french_problems = analysis_results['keyword_frequency']
            critical_french_terms = ['fuite', 'panne', 'défaut', 'usure', 'corrosion']
            found_french_problems = [(term, count) for term, count in french_problems.items() if term in critical_french_terms]
            
            if found_french_problems:
                french_issues = [f"{term} ({self.french_translations.get(term, term)}): {count}" for term, count in found_french_problems]
                recommendations.append(f"French problem terms detected: {', '.join(french_issues)}")
        
        # Pattern-based recommendations
        if len(patterns['repeated_names']) > 5:
            recommendations.append(f"Found {len(patterns['repeated_names'])} repeated work order names - consider creating standardized templates")
        
        if len(patterns['repeated_name_patterns']) > 10:
            recommendations.append("Multiple naming patterns detected - standardize work order naming convention (consider bilingual French/English)")
        
        # Timeline recommendations
        if 'yearly_distribution' in analysis_results:
            yearly_data = analysis_results['yearly_distribution']
            if len(yearly_data) >= 2:
                years = sorted(yearly_data.keys())
                recent_year = years[-1] if years else None
                previous_year = years[-2] if len(years) >= 2 else None
                
                if recent_year and previous_year:
                    recent_count = yearly_data[recent_year]
                    previous_count = yearly_data[previous_year]
                    change = ((recent_count - previous_count) / previous_count) * 100
                    
                    if abs(change) > 10:
                        trend = "increase" if change > 0 else "decrease"
                        recommendations.append(f"Significant {trend} in work orders: {change:+.1f}% from {previous_year} to {recent_year}")
        
        # Category-specific recommendations
        category_insights = analysis_results.get('category_insights', {})
        if 'corrective_analysis' in category_insights:
            corrective = category_insights['corrective_analysis']
            if corrective.get('percentage_of_total', 0) > 60:
                recommendations.append("High corrective maintenance ratio (>60%) - increase preventive maintenance planning")
        
        # Keyword frequency recommendations
        if len(analysis_results['keyword_frequency']) > 500:
            recommendations.append("High keyword diversity suggests need for better categorization and bilingual tagging system (French/English)")
        
        # Equipment-specific recommendations with VBA insights
        sts_analysis = analysis_results.get('sts_specific_analysis', {})
        high_maintenance_units = []
        for unit, data in sts_analysis.items():
            if data['total_work_orders'] > 100:
                corrective_pct = data['job_type_breakdown'].get('C', 0) / data['total_work_orders'] * 100
                if corrective_pct > 50:
                    # Add VBA fault info if available
                    vba_faults_info = ""
                    if 'vba_fault_analysis' in data and data['vba_fault_analysis']:
                        top_fault = list(data['vba_fault_analysis'].items())[0]
                        vba_faults_info = f" - main issue: {top_fault[0]}"
                    high_maintenance_units.append(f"{unit} ({corrective_pct:.1f}% corrective{vba_faults_info})")
        
        if high_maintenance_units:
            recommendations.append(f"High maintenance units requiring attention: {', '.join(high_maintenance_units[:3])}")
        
        # VBA-specific insights recommendations
        if vba_faults:
            fault_diversity = len(vba_faults)
            if fault_diversity > 20:
                recommendations.append(f"High fault diversity detected ({fault_diversity} different fault types) - implement fault categorization system based on VBA analysis patterns")
        
        return recommendations
        """Generate recommendations based on the analysis."""
        recommendations = []
        
        # Equipment-based recommendations
        top_equipment = analysis_results['equipment_frequency'].most_common(3)
        if top_equipment:
            equipment_names = [eq[0] for eq in top_equipment]
            recommendations.append(f"Focus maintenance planning on most frequent equipment: {', '.join(equipment_names)}")
        
        # Action-based recommendations
        top_actions = analysis_results['action_frequency'].most_common(3)
        if top_actions:
            action_names = [action[0] for action in top_actions]
            recommendations.append(f"Common maintenance activities: {', '.join(action_names)} - consider standardizing procedures")
        
        # Problem pattern recommendations
        top_problems = analysis_results['problem_frequency'].most_common(3)
        if top_problems:
            problem_names = [prob[0] for prob in top_problems]
            recommendations.append(f"Recurring problems identified: {', '.join(problem_names)} - investigate root causes")
        
        # Pattern-based recommendations
        if len(patterns['repeated_names']) > 5:
            recommendations.append(f"Found {len(patterns['repeated_names'])} repeated work order names - consider creating templates")
        
        if len(patterns['repeated_name_patterns']) > 10:
            recommendations.append("Multiple naming patterns detected - standardize work order naming convention")
        
        # Keyword frequency recommendations
        if len(analysis_results['keyword_frequency']) > 500:
            recommendations.append("High keyword diversity suggests need for better categorization and tagging system")
        
        return recommendations
    
    def _generate_category_insights(self, work_orders: List[Dict[str, Any]], filters: Dict[str, Any]) -> Dict[str, Any]:
        """Generate category-specific insights."""
        insights = {
            'corrective_analysis': {},
            'preventive_analysis': {},
            'urgent_analysis': {},
            'sts_equipment_analysis': {},
            'timeline_trends': {}
        }
        
        # Categorize work orders by job type
        corrective_wos = [wo for wo in work_orders if wo['job_type'] == 'C']
        preventive_wos = [wo for wo in work_orders if wo['job_type'] == 'P']
        urgent_wos = [wo for wo in work_orders if wo['job_type'] == 'U']
        
        # Analyze corrective work orders
        if corrective_wos:
            corrective_problems = Counter()
            corrective_equipment = Counter()
            for wo in corrective_wos:
                combined_text = f"{wo['wo_name']} {wo['description']}"
                categories = self.categorize_text(combined_text)
                corrective_problems.update(categories['problems'])
                if wo['equipment']:
                    corrective_equipment[wo['equipment']] += 1
            
            insights['corrective_analysis'] = {
                'total_count': len(corrective_wos),
                'top_problems': dict(corrective_problems.most_common(10)),
                'top_equipment': dict(corrective_equipment.most_common(10)),
                'percentage_of_total': (len(corrective_wos) / len(work_orders)) * 100
            }
        
        # Analyze preventive work orders  
        if preventive_wos:
            preventive_actions = Counter()
            preventive_equipment = Counter()
            for wo in preventive_wos:
                combined_text = f"{wo['wo_name']} {wo['description']}"
                categories = self.categorize_text(combined_text)
                preventive_actions.update(categories['actions'])
                if wo['equipment']:
                    preventive_equipment[wo['equipment']] += 1
            
            insights['preventive_analysis'] = {
                'total_count': len(preventive_wos),
                'top_actions': dict(preventive_actions.most_common(10)),
                'top_equipment': dict(preventive_equipment.most_common(10)),
                'percentage_of_total': (len(preventive_wos) / len(work_orders)) * 100
            }
        
        # Analyze urgent work orders
        if urgent_wos:
            urgent_problems = Counter()
            urgent_equipment = Counter()
            for wo in urgent_wos:
                combined_text = f"{wo['wo_name']} {wo['description']}"
                categories = self.categorize_text(combined_text)
                urgent_problems.update(categories['problems'])
                if wo['equipment']:
                    urgent_equipment[wo['equipment']] += 1
            
            insights['urgent_analysis'] = {
                'total_count': len(urgent_wos),
                'top_problems': dict(urgent_problems.most_common(10)),
                'top_equipment': dict(urgent_equipment.most_common(10)),
                'percentage_of_total': (len(urgent_wos) / len(work_orders)) * 100
            }
        
        # STS/SPS equipment specific analysis
        equipment_map = defaultdict(list)
        for wo in work_orders:
            if wo['equipment'] and ('STS' in wo['equipment'] or 'SPS' in wo['equipment']):
                # Extract equipment number for both STS and SPS
                sts_match = re.search(r'STS(\d+)', wo['equipment'])
                sps_match = re.search(r'SPS(\d+)', wo['equipment'])
                
                if sts_match:
                    equipment_number = f"STS{sts_match.group(1)}"
                    equipment_map[equipment_number].append(wo)
                elif sps_match:
                    equipment_number = f"SPS{sps_match.group(1)}"
                    equipment_map[equipment_number].append(wo)
        
        for equipment_unit, equipment_wos in equipment_map.items():
            if len(equipment_wos) >= 10:  # Only analyze equipment units with significant data
                job_type_dist = Counter([wo['job_type'] for wo in equipment_wos if wo['job_type']])
                problems = Counter()
                actions = Counter()
                
                for wo in equipment_wos:
                    combined_text = f"{wo['wo_name']} {wo['description']}"
                    translated_text = self.translate_french_terms(combined_text)
                    analysis_text = f"{combined_text} {translated_text}"
                    categories = self.categorize_text(analysis_text)
                    problems.update(categories['problems'])
                    actions.update(categories['actions'])
                
                equipment_type = 'STS_Crane' if equipment_unit.startswith('STS') else 'SPS_Spreader'
                
                insights['sts_equipment_analysis'][equipment_unit] = {
                    'equipment_type': equipment_type,
                    'total_work_orders': len(equipment_wos),
                    'job_type_distribution': dict(job_type_dist),
                    'top_problems': dict(problems.most_common(5)),
                    'top_actions': dict(actions.most_common(5)),
                    'corrective_percentage': (job_type_dist['C'] / len(equipment_wos)) * 100 if 'C' in job_type_dist else 0,
                    'preventive_percentage': (job_type_dist['P'] / len(equipment_wos)) * 100 if 'P' in job_type_dist else 0,
                    'urgent_percentage': (job_type_dist['U'] / len(equipment_wos)) * 100 if 'U' in job_type_dist else 0
                }
        
        return insights

    def run_comprehensive_analysis(self, limit: int = 1000, filters: Dict[str, Any] = None) -> Dict[str, Any]:
        """Run comprehensive AI analysis on work order data with filtering capabilities."""
        try:
            logger.info(f"Starting comprehensive work order analysis (limit: {limit}, filters: {filters})")
            
            # Extract work order data with filters
            work_orders = self.extract_work_order_data(limit, filters)
            
            if not work_orders:
                return {
                    'error': 'No work order data found matching the specified filters',
                    'timestamp': datetime.now().isoformat(),
                    'filters_applied': filters
                }
            
            # Extract main data points
            analysis_results = self.extract_main_data_points(work_orders, filters)
            
            # Identify repeated patterns
            repeated_patterns = self.identify_repeated_patterns(work_orders)
            
            # Generate insights
            insights = self.generate_insights(analysis_results, repeated_patterns)
            
            # Combine all results
            comprehensive_results = {
                'status': 'success',
                'timestamp': datetime.now().isoformat(),
                'filters_applied': filters or {},
                'data_sources': list(set([wo['source'] for wo in work_orders])),
                'analysis': analysis_results,
                'patterns': repeated_patterns,
                'insights': insights
            }
            
            logger.info(f"Analysis completed successfully. Analyzed {len(work_orders)} work orders.")
            return comprehensive_results
            
        except Exception as e:
            logger.error(f"Error in comprehensive analysis: {str(e)}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat(),
                'filters_applied': filters
            }
    
    def export_analysis_results(self, results: Dict[str, Any], output_file: str = None) -> str:
        """Export analysis results to JSON file."""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"work_order_ai_analysis_{timestamp}.json"
        
        try:
            # Convert Counter objects to regular dicts for JSON serialization
            exportable_results = json.loads(json.dumps(results, default=str))
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(exportable_results, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Analysis results exported to: {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Error exporting results: {str(e)}")
            return None

# Example usage function
def run_ai_analysis_example():
    """Example function to run the AI analysis."""
    service = AIWorkOrderAnalysisService()
    results = service.run_comprehensive_analysis(limit=1000)
    
    if 'error' not in results:
        # Export results
        output_file = service.export_analysis_results(results)
        print(f"Analysis completed successfully!")
        print(f"Results exported to: {output_file}")
        
        # Print summary
        if 'insights' in results:
            print("\n=== ANALYSIS SUMMARY ===")
            print(f"Total Work Orders Analyzed: {results['insights']['summary']['total_work_orders_analyzed']}")
            print(f"Unique Keywords Found: {results['insights']['summary']['unique_keywords_found']}")
            
            print("\nTop Equipment Types:")
            for equipment, count in list(results['insights']['summary']['most_common_equipment'].items())[:5]:
                print(f"  {equipment}: {count}")
            
            print("\nTop Maintenance Actions:")
            for action, count in list(results['insights']['summary']['most_common_actions'].items())[:5]:
                print(f"  {action}: {count}")
            
            print("\nTop Keywords:")
            for keyword, count in list(results['insights']['top_keywords'].items())[:10]:
                print(f"  {keyword}: {count}")
                
            print("\nRecommendations:")
            for i, rec in enumerate(results['insights']['recommendations'], 1):
                print(f"  {i}. {rec}")
    else:
        print(f"Analysis failed: {results['error']}")

if __name__ == "__main__":
    run_ai_analysis_example()
