"""
AI Fault Analysis Service
Analyzes repeated faults and time-based relationships for equipment (cranes and spreaders)
"""

import sqlite3
import os
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import json
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class FaultPattern:
    equipment_id: str
    equipment_type: str  # 'crane' or 'spreader'
    fault_description: str
    frequency: int
    time_intervals: List[float]  # Hours between occurrences
    avg_interval: float
    trend: str  # 'increasing', 'decreasing', 'stable'
    criticality: str  # 'high', 'medium', 'low'
    related_faults: List[str]

@dataclass
class AIInsight:
    insight_type: str
    equipment_id: str
    title: str
    description: str
    recommendation: str
    priority: str
    confidence: float

class AIFaultAnalysisService:
    def __init__(self, instance_path):
        self.db_path = os.path.join(instance_path, 'Workorder.db')
        
    def get_database_connection(self):
        """Get connection to database"""
        if not os.path.exists(self.db_path):
            raise FileNotFoundError(f"Database not found at {self.db_path}")
        return sqlite3.connect(self.db_path)
    
    def get_equipment_faults(self, equipment_id: str, days_back: int = 365) -> List[Dict]:
        """Get all faults for a specific equipment"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Check table name
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('all_cm', 'allCM')")
        table_result = cursor.fetchone()
        if not table_result:
            conn.close()
            return []
        
        table_name = table_result[0]
        cutoff_date = datetime.now() - timedelta(days=days_back)
        
        # Get faults for specific equipment
        query = f"""
        SELECT 
            wo_name,
            description,
            order_date,
            execution_date,
            etatjob as status,
            job_type,
            cost_purpose_key,
            priority_key,
            CASE 
                WHEN POS_key = 'STS' THEN SUBSTR(MO_key, 1, 5)
                WHEN POS_key = 'SPR' THEN SUBSTR(MO_key, 1, 6)
                ELSE equipement
            END as calculated_equipment,
            POS_key
        FROM {table_name}
        WHERE (
            (POS_key = 'STS' AND SUBSTR(MO_key, 1, 5) = ?) OR
            (POS_key = 'SPR' AND SUBSTR(MO_key, 1, 6) = ?) OR
            (POS_key NOT IN ('STS', 'SPR') AND equipement = ?)
        )
        AND order_date >= ?
        AND description IS NOT NULL
        ORDER BY order_date DESC
        """
        
        cursor.execute(query, [equipment_id, equipment_id, equipment_id, cutoff_date.strftime('%Y-%m-%d')])
        columns = [desc[0] for desc in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def analyze_fault_patterns(self, equipment_id: str) -> List[FaultPattern]:
        """Analyze fault patterns for equipment"""
        faults = self.get_equipment_faults(equipment_id)
        
        if not faults:
            return []
        
        # Determine equipment type
        equipment_type = 'crane' if any(f['POS_key'] == 'STS' for f in faults) else 'spreader'
        
        # Group faults by description/type
        fault_groups = defaultdict(list)
        for fault in faults:
            # Clean and categorize fault description
            desc = self._categorize_fault(fault['description'])
            fault_groups[desc].append(fault)
        
        patterns = []
        for fault_desc, fault_list in fault_groups.items():
            if len(fault_list) < 2:  # Skip single occurrences
                continue
                
            # Calculate time intervals
            dates = [datetime.strptime(f['order_date'], '%Y-%m-%d') for f in fault_list if f['order_date']]
            dates.sort()
            
            intervals = []
            for i in range(1, len(dates)):
                interval_hours = (dates[i] - dates[i-1]).total_seconds() / 3600
                intervals.append(interval_hours)
            
            if not intervals:
                continue
                
            avg_interval = sum(intervals) / len(intervals)
            trend = self._calculate_trend(intervals)
            criticality = self._assess_criticality(len(fault_list), avg_interval, fault_desc)
            
            pattern = FaultPattern(
                equipment_id=equipment_id,
                equipment_type=equipment_type,
                fault_description=fault_desc,
                frequency=len(fault_list),
                time_intervals=intervals,
                avg_interval=avg_interval,
                trend=trend,
                criticality=criticality,
                related_faults=self._find_related_faults(fault_desc, fault_groups.keys())
            )
            patterns.append(pattern)
        
        return sorted(patterns, key=lambda x: x.frequency, reverse=True)
    
    def _categorize_fault(self, description: str) -> str:
        """Categorize fault description into common fault types"""
        if not description:
            return "Unknown"
            
        desc_lower = description.lower()
        
        # Common fault categories
        if any(word in desc_lower for word in ['hydraulic', 'oil', 'pressure']):
            return "Hydraulic System"
        elif any(word in desc_lower for word in ['electrical', 'electric', 'power', 'voltage']):
            return "Electrical System"
        elif any(word in desc_lower for word in ['mechanical', 'gear', 'bearing', 'motor']):
            return "Mechanical System"
        elif any(word in desc_lower for word in ['brake', 'braking']):
            return "Braking System"
        elif any(word in desc_lower for word in ['sensor', 'detector', 'alarm']):
            return "Sensor/Detection"
        elif any(word in desc_lower for word in ['cable', 'wire', 'connection']):
            return "Cable/Wiring"
        elif any(word in desc_lower for word in ['lubrication', 'grease', 'lubricant']):
            return "Lubrication"
        elif any(word in desc_lower for word in ['inspection', 'check', 'test']):
            return "Inspection/Testing"
        else:
            return "Other/General"
    
    def _calculate_trend(self, intervals: List[float]) -> str:
        """Calculate if fault frequency is increasing, decreasing, or stable"""
        if len(intervals) < 3:
            return "stable"
            
        # Compare first and second half of intervals
        mid = len(intervals) // 2
        first_half_avg = sum(intervals[:mid]) / mid if mid > 0 else 0
        second_half_avg = sum(intervals[mid:]) / (len(intervals) - mid)
        
        # If intervals are getting shorter, faults are increasing
        if second_half_avg < first_half_avg * 0.8:
            return "increasing"
        elif second_half_avg > first_half_avg * 1.2:
            return "decreasing"
        else:
            return "stable"
    
    def _assess_criticality(self, frequency: int, avg_interval_hours: float, fault_type: str) -> str:
        """Assess criticality of fault pattern"""
        # High frequency faults
        if frequency >= 10:
            return "high"
        elif frequency >= 5:
            return "medium"
        
        # Short intervals between faults
        if avg_interval_hours < 168:  # Less than a week
            return "high"
        elif avg_interval_hours < 720:  # Less than a month
            return "medium"
        
        # Critical system faults
        critical_systems = ["Hydraulic System", "Braking System", "Electrical System"]
        if fault_type in critical_systems:
            return "high" if frequency >= 3 else "medium"
        
        return "low"
    
    def _find_related_faults(self, fault_desc: str, all_fault_types: List[str]) -> List[str]:
        """Find related fault types that might be connected"""
        related = []
        
        # System relationships
        relationships = {
            "Hydraulic System": ["Mechanical System", "Lubrication"],
            "Electrical System": ["Sensor/Detection", "Cable/Wiring"],
            "Mechanical System": ["Hydraulic System", "Lubrication"],
            "Braking System": ["Hydraulic System", "Mechanical System"],
            "Sensor/Detection": ["Electrical System", "Cable/Wiring"],
            "Cable/Wiring": ["Electrical System", "Sensor/Detection"],
            "Lubrication": ["Mechanical System", "Hydraulic System"]
        }
        
        if fault_desc in relationships:
            for related_type in relationships[fault_desc]:
                if related_type in all_fault_types and related_type != fault_desc:
                    related.append(related_type)
        
        return related
    
    def generate_ai_insights(self, equipment_id: str) -> List[AIInsight]:
        """Generate AI-powered insights about equipment faults"""
        patterns = self.analyze_fault_patterns(equipment_id)
        insights = []
        
        if not patterns:
            return insights
        
        # High frequency fault insight
        high_freq_patterns = [p for p in patterns if p.frequency >= 5]
        if high_freq_patterns:
            pattern = high_freq_patterns[0]
            insight = AIInsight(
                insight_type="high_frequency",
                equipment_id=equipment_id,
                title=f"High Frequency {pattern.fault_description} Faults",
                description=f"Equipment {equipment_id} has experienced {pattern.frequency} {pattern.fault_description} faults with an average interval of {pattern.avg_interval:.1f} hours.",
                recommendation=f"Consider preventive maintenance schedule for {pattern.fault_description}. Investigate root cause and implement predictive maintenance.",
                priority="high" if pattern.criticality == "high" else "medium",
                confidence=0.85
            )
            insights.append(insight)
        
        # Increasing trend insight
        increasing_patterns = [p for p in patterns if p.trend == "increasing"]
        if increasing_patterns:
            pattern = increasing_patterns[0]
            insight = AIInsight(
                insight_type="increasing_trend",
                equipment_id=equipment_id,
                title=f"Escalating {pattern.fault_description} Issues",
                description=f"Fault frequency for {pattern.fault_description} is increasing over time, indicating potential deterioration.",
                recommendation="Schedule immediate inspection and consider component replacement before critical failure occurs.",
                priority="high",
                confidence=0.80
            )
            insights.append(insight)
        
        # Related faults insight
        for pattern in patterns[:2]:  # Top 2 patterns
            if pattern.related_faults:
                related_patterns = [p for p in patterns if p.fault_description in pattern.related_faults]
                if related_patterns:
                    insight = AIInsight(
                        insight_type="related_faults",
                        equipment_id=equipment_id,
                        title=f"Correlated System Failures",
                        description=f"{pattern.fault_description} faults are occurring alongside {', '.join(pattern.related_faults)} issues, suggesting system-wide problems.",
                        recommendation="Investigate interconnected systems. Consider comprehensive maintenance of related components.",
                        priority="medium",
                        confidence=0.75
                    )
                    insights.append(insight)
                    break
        
        # Critical system insight
        critical_patterns = [p for p in patterns if p.criticality == "high"]
        if critical_patterns:
            pattern = critical_patterns[0]
            insight = AIInsight(
                insight_type="critical_system",
                equipment_id=equipment_id,
                title=f"Critical {pattern.fault_description} Issues",
                description=f"Critical system failures detected in {pattern.fault_description} requiring immediate attention.",
                recommendation="Implement emergency maintenance protocol. Consider equipment downtime for comprehensive repair.",
                priority="critical",
                confidence=0.90
            )
            insights.append(insight)
        
        return insights
    
    def get_equipment_list(self) -> List[Dict[str, str]]:
        """Get list of all equipment with fault data"""
        conn = self.get_database_connection()
        cursor = conn.cursor()
        
        # Check table name
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('all_cm', 'allCM')")
        table_result = cursor.fetchone()
        if not table_result:
            conn.close()
            return []
        
        table_name = table_result[0]
        
        query = f"""
        SELECT DISTINCT
            CASE 
                WHEN POS_key = 'STS' THEN SUBSTR(MO_key, 1, 5)
                WHEN POS_key = 'SPR' THEN SUBSTR(MO_key, 1, 6)
                ELSE equipement
            END as equipment_id,
            POS_key as equipment_type,
            COUNT(*) as fault_count
        FROM {table_name}
        WHERE MO_key IS NOT NULL 
            AND description IS NOT NULL
            AND POS_key IN ('STS', 'SPR')
        GROUP BY equipment_id, equipment_type
        HAVING COUNT(*) >= 3
        ORDER BY fault_count DESC
        LIMIT 50
        """
        
        cursor.execute(query)
        results = []
        for row in cursor.fetchall():
            results.append({
                'equipment_id': row[0],
                'equipment_type': 'Crane' if row[1] == 'STS' else 'Spreader',
                'fault_count': row[2]
            })
        
        conn.close()
        return results
    
    def get_comprehensive_ai_analysis(self) -> Dict[str, Any]:
        """Get comprehensive AI analysis for all equipment"""
        equipment_list = self.get_equipment_list()
        
        analysis = {
            'total_equipment': len(equipment_list),
            'equipment_insights': {},
            'global_patterns': {
                'most_common_faults': [],
                'critical_equipment': [],
                'fault_trends': {}
            },
            'recommendations': []
        }
        
        all_patterns = []
        
        # Analyze each equipment
        for equipment in equipment_list[:10]:  # Limit to top 10 for performance
            equipment_id = equipment['equipment_id']
            patterns = self.analyze_fault_patterns(equipment_id)
            insights = self.generate_ai_insights(equipment_id)
            
            analysis['equipment_insights'][equipment_id] = {
                'equipment_type': equipment['equipment_type'],
                'fault_count': equipment['fault_count'],
                'patterns': [self._pattern_to_dict(p) for p in patterns],
                'insights': [self._insight_to_dict(i) for i in insights]
            }
            
            all_patterns.extend(patterns)
        
        # Global analysis
        if all_patterns:
            # Most common fault types
            fault_counter = Counter(p.fault_description for p in all_patterns)
            analysis['global_patterns']['most_common_faults'] = [
                {'fault_type': fault, 'frequency': count} 
                for fault, count in fault_counter.most_common(5)
            ]
            
            # Critical equipment
            critical_equipment = [
                p.equipment_id for p in all_patterns 
                if p.criticality == 'high'
            ]
            analysis['global_patterns']['critical_equipment'] = list(set(critical_equipment))
            
            # Overall recommendations
            analysis['recommendations'] = self._generate_global_recommendations(all_patterns)
        
        return analysis
    
    def _pattern_to_dict(self, pattern: FaultPattern) -> Dict:
        """Convert FaultPattern to dictionary"""
        return {
            'equipment_id': pattern.equipment_id,
            'equipment_type': pattern.equipment_type,
            'fault_description': pattern.fault_description,
            'frequency': pattern.frequency,
            'avg_interval_hours': round(pattern.avg_interval, 2),
            'trend': pattern.trend,
            'criticality': pattern.criticality,
            'related_faults': pattern.related_faults
        }
    
    def _insight_to_dict(self, insight: AIInsight) -> Dict:
        """Convert AIInsight to dictionary"""
        return {
            'type': insight.insight_type,
            'title': insight.title,
            'description': insight.description,
            'recommendation': insight.recommendation,
            'priority': insight.priority,
            'confidence': insight.confidence
        }
    
    def _generate_global_recommendations(self, patterns: List[FaultPattern]) -> List[str]:
        """Generate global maintenance recommendations"""
        recommendations = []
        
        # High frequency faults
        high_freq = [p for p in patterns if p.frequency >= 8]
        if high_freq:
            recommendations.append(
                f"Implement predictive maintenance for {len(high_freq)} equipment showing high fault frequency"
            )
        
        # Increasing trends
        increasing = [p for p in patterns if p.trend == "increasing"]
        if increasing:
            recommendations.append(
                f"Schedule immediate inspection for {len(increasing)} equipment with escalating fault patterns"
            )
        
        # Critical systems
        critical = [p for p in patterns if p.criticality == "high"]
        if critical:
            recommendations.append(
                f"Prioritize maintenance for {len(critical)} critical system failures"
            )
        
        return recommendations
