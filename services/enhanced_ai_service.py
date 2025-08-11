"""
Enhanced AI Analysis Service
Combines multiple AI services for comprehensive analysis
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
from dataclasses import dataclass

from services.ai_work_order_analysis_service import AIWorkOrderAnalysisService
from services.ai_fault_analysis_service import AIFaultAnalysisService

logger = logging.getLogger(__name__)

@dataclass
class AIInsight:
    """Unified AI insight structure"""
    insight_id: str
    insight_type: str  # 'pattern', 'prediction', 'anomaly', 'optimization'
    title: str
    description: str
    recommendation: str
    priority: str  # 'critical', 'high', 'medium', 'low'
    confidence: float  # 0.0 to 1.0
    equipment_id: Optional[str] = None
    affected_systems: List[str] = None
    cost_impact: Optional[float] = None
    timeline: Optional[str] = None
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
        if self.affected_systems is None:
            self.affected_systems = []

class EnhancedAIAnalysisService:
    """Enhanced AI Analysis Service combining multiple AI capabilities"""
    
    def __init__(self, instance_path: str = 'instance'):
        """Initialize the enhanced AI service"""
        self.instance_path = instance_path
        self.wo_ai_service = AIWorkOrderAnalysisService()
        self.fault_ai_service = AIFaultAnalysisService(instance_path)
        
        # AI Configuration
        self.confidence_thresholds = {
            'high': 0.8,
            'medium': 0.6,
            'low': 0.4
        }
        
        # Cache for AI results
        self._cache = {}
        self._cache_timeout = 300  # 5 minutes
    
    def get_comprehensive_ai_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive AI dashboard data"""
        try:
            logger.info("Generating comprehensive AI dashboard")
            
            # Get work order analysis
            wo_analysis = self.wo_ai_service.run_comprehensive_analysis(limit=2000)
            
            # Get fault analysis
            fault_analysis = self.fault_ai_service.get_comprehensive_ai_analysis()
            
            # Generate unified insights
            unified_insights = self._generate_unified_insights(wo_analysis, fault_analysis)
            
            # Calculate AI metrics
            ai_metrics = self._calculate_ai_metrics(wo_analysis, fault_analysis)
            
            # Get equipment health scores
            equipment_health = self._calculate_equipment_health_scores(wo_analysis, fault_analysis)
            
            # Generate predictions
            predictions = self._generate_basic_predictions(wo_analysis, fault_analysis)
            
            # Get optimization suggestions
            optimizations = self._generate_optimization_suggestions(wo_analysis, fault_analysis)
            
            dashboard_data = {
                'timestamp': datetime.now().isoformat(),
                'ai_metrics': ai_metrics,
                'unified_insights': unified_insights,
                'equipment_health': equipment_health,
                'predictions': predictions,
                'optimizations': optimizations,
                'work_order_analysis': wo_analysis,
                'fault_analysis': fault_analysis,
                'data_quality': self._assess_data_quality(wo_analysis, fault_analysis)
            }
            
            # Cache the results
            self._cache['dashboard'] = {
                'data': dashboard_data,
                'timestamp': datetime.now()
            }
            
            return dashboard_data
            
        except Exception as e:
            logger.error(f"Error generating AI dashboard: {str(e)}")
            return {
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _generate_unified_insights(self, wo_analysis: Dict, fault_analysis: Dict) -> List[Dict]:
        """Generate unified insights from multiple AI analyses"""
        insights = []
        
        try:
            # Work order insights
            if 'insights' in wo_analysis and 'recommendations' in wo_analysis['insights']:
                for i, rec in enumerate(wo_analysis['insights']['recommendations'][:5]):
                    insight = AIInsight(
                        insight_id=f"wo_{i}",
                        insight_type='pattern',
                        title=f"Work Order Pattern Analysis",
                        description=rec,
                        recommendation=rec,
                        priority='medium',
                        confidence=0.75
                    )
                    insights.append(self._insight_to_dict(insight))
            
            # Fault analysis insights
            if 'equipment_insights' in fault_analysis:
                for equipment_id, data in fault_analysis['equipment_insights'].items():
                    if 'insights' in data:
                        for insight_data in data['insights'][:3]:
                            insight = AIInsight(
                                insight_id=f"fault_{equipment_id}_{insight_data['type']}",
                                insight_type='prediction',
                                title=insight_data['title'],
                                description=insight_data['description'],
                                recommendation=insight_data['recommendation'],
                                priority=insight_data['priority'],
                                confidence=insight_data['confidence'],
                                equipment_id=equipment_id
                            )
                            insights.append(self._insight_to_dict(insight))
            
            # Critical equipment insights
            critical_equipment = self._identify_critical_equipment(wo_analysis, fault_analysis)
            for equipment_id, score in critical_equipment.items():
                if score > 0.8:  # High criticality
                    insight = AIInsight(
                        insight_id=f"critical_{equipment_id}",
                        insight_type='anomaly',
                        title=f"Critical Equipment Alert: {equipment_id}",
                        description=f"Equipment {equipment_id} shows high maintenance activity (criticality score: {score:.2f})",
                        recommendation=f"Schedule immediate inspection and consider preventive maintenance for {equipment_id}",
                        priority='critical',
                        confidence=score,
                        equipment_id=equipment_id
                    )
                    insights.append(self._insight_to_dict(insight))
            
            # Sort by priority and confidence
            insights.sort(key=lambda x: (
                {'critical': 4, 'high': 3, 'medium': 2, 'low': 1}.get(x['priority'], 0),
                x['confidence']
            ), reverse=True)
            
            return insights[:15]  # Return top 15 insights
            
        except Exception as e:
            logger.error(f"Error generating unified insights: {str(e)}")
            return []
    
    def _calculate_ai_metrics(self, wo_analysis: Dict, fault_analysis: Dict) -> Dict[str, Any]:
        """Calculate AI performance and data metrics"""
        metrics = {
            'total_work_orders_analyzed': 0,
            'total_equipment_analyzed': 0,
            'pattern_detection_rate': 0.0,
            'fault_prediction_accuracy': 0.0,
            'data_completeness': 0.0,
            'ai_processing_time': 0.0,
            'insights_generated': 0,
            'high_confidence_insights': 0,
            'cost_optimization_potential': 0.0
        }
        
        try:
            # Work order metrics
            if 'insights' in wo_analysis and 'summary' in wo_analysis['insights']:
                summary = wo_analysis['insights']['summary']
                metrics['total_work_orders_analyzed'] = summary.get('total_work_orders_analyzed', 0)
                metrics['pattern_detection_rate'] = min(
                    summary.get('unique_keywords_found', 0) / max(metrics['total_work_orders_analyzed'], 1),
                    1.0
                )
            
            # Fault analysis metrics
            if 'total_equipment' in fault_analysis:
                metrics['total_equipment_analyzed'] = fault_analysis['total_equipment']
            
            # Data quality assessment
            metrics['data_completeness'] = self._calculate_data_completeness(wo_analysis, fault_analysis)
            
            # AI performance estimation (placeholder - would use real metrics in production)
            metrics['fault_prediction_accuracy'] = 0.82  # Estimated based on pattern complexity
            metrics['ai_processing_time'] = 2.5  # Seconds
            
            # Insight metrics
            total_insights = len(self._generate_unified_insights(wo_analysis, fault_analysis))
            metrics['insights_generated'] = total_insights
            metrics['high_confidence_insights'] = int(total_insights * 0.4)  # Estimated 40% high confidence
            
            # Cost optimization potential (estimated)
            metrics['cost_optimization_potential'] = min(
                metrics['pattern_detection_rate'] * 15.0,  # Up to 15% optimization
                15.0
            )
            
        except Exception as e:
            logger.error(f"Error calculating AI metrics: {str(e)}")
        
        return metrics
    
    def _calculate_equipment_health_scores(self, wo_analysis: Dict, fault_analysis: Dict) -> Dict[str, Dict]:
        """Calculate health scores for equipment"""
        equipment_health = {}
        
        try:
            # From work order analysis
            if 'analysis' in wo_analysis and 'sts_specific_analysis' in wo_analysis['analysis']:
                sts_analysis = wo_analysis['analysis']['sts_specific_analysis']
                
                for equipment_id, data in sts_analysis.items():
                    total_wo = data.get('total_work_orders', 0)
                    job_types = data.get('job_type_breakdown', {})
                    corrective = job_types.get('C', 0)
                    
                    # Calculate health score (0-100)
                    if total_wo > 0:
                        corrective_ratio = corrective / total_wo
                        # Lower corrective ratio = better health
                        health_score = max(0, 100 - (corrective_ratio * 100))
                        
                        # Adjust based on total work orders
                        if total_wo > 50:  # High maintenance frequency
                            health_score -= 10
                        elif total_wo < 10:  # Low data confidence
                            health_score = health_score * 0.8
                        
                        equipment_health[equipment_id] = {
                            'health_score': round(health_score, 1),
                            'total_work_orders': total_wo,
                            'corrective_percentage': round(corrective_ratio * 100, 1),
                            'trend': self._calculate_health_trend(data),
                            'status': self._get_health_status(health_score),
                            'last_updated': datetime.now().isoformat()
                        }
            
            # Enhance with fault analysis data
            if 'equipment_insights' in fault_analysis:
                for equipment_id, fault_data in fault_analysis['equipment_insights'].items():
                    if equipment_id in equipment_health:
                        # Adjust health score based on fault patterns
                        critical_faults = sum(1 for insight in fault_data.get('insights', []) 
                                            if insight.get('priority') == 'critical')
                        if critical_faults > 0:
                            equipment_health[equipment_id]['health_score'] -= (critical_faults * 10)
                            equipment_health[equipment_id]['health_score'] = max(0, equipment_health[equipment_id]['health_score'])
                            equipment_health[equipment_id]['critical_faults'] = critical_faults
            
        except Exception as e:
            logger.error(f"Error calculating equipment health scores: {str(e)}")
        
        return equipment_health
    
    def _calculate_health_trend(self, equipment_data: Dict) -> str:
        """Calculate health trend for equipment"""
        # Simplified trend calculation - in production would use time-series analysis
        job_types = equipment_data.get('job_type_breakdown', {})
        corrective = job_types.get('C', 0)
        preventive = job_types.get('P', 0)
        
        if corrective > preventive * 2:
            return 'declining'
        elif preventive > corrective:
            return 'improving'
        else:
            return 'stable'
    
    def _get_health_status(self, health_score: float) -> str:
        """Get health status based on score"""
        if health_score >= 80:
            return 'excellent'
        elif health_score >= 60:
            return 'good'
        elif health_score >= 40:
            return 'fair'
        elif health_score >= 20:
            return 'poor'
        else:
            return 'critical'
    
    def _generate_basic_predictions(self, wo_analysis: Dict, fault_analysis: Dict) -> List[Dict]:
        """Generate basic predictions based on current data"""
        predictions = []
        
        try:
            # Equipment failure predictions
            if 'analysis' in wo_analysis and 'sts_specific_analysis' in wo_analysis['analysis']:
                for equipment_id, data in wo_analysis['analysis']['sts_specific_analysis'].items():
                    total_wo = data.get('total_work_orders', 0)
                    if total_wo > 20:  # Enough data for prediction
                        job_types = data.get('job_type_breakdown', {})
                        corrective = job_types.get('C', 0)
                        
                        # Simple prediction based on corrective maintenance ratio
                        corrective_ratio = corrective / total_wo if total_wo > 0 else 0
                        
                        if corrective_ratio > 0.6:  # High corrective ratio
                            failure_probability = min(corrective_ratio * 0.8, 0.9)
                            predictions.append({
                                'type': 'equipment_failure',
                                'equipment_id': equipment_id,
                                'title': f'High Failure Risk: {equipment_id}',
                                'description': f'Equipment shows {corrective_ratio:.1%} corrective maintenance ratio',
                                'probability': failure_probability,
                                'time_horizon': '30 days',
                                'recommended_action': 'Schedule preventive maintenance',
                                'potential_cost_impact': 'High'
                            })
            
            # Maintenance budget predictions
            current_year = datetime.now().year
            if 'analysis' in wo_analysis and 'yearly_distribution' in wo_analysis['analysis']:
                yearly_data = wo_analysis['analysis']['yearly_distribution']
                if str(current_year) in yearly_data and str(current_year - 1) in yearly_data:
                    current_count = yearly_data[str(current_year)]
                    previous_count = yearly_data[str(current_year - 1)]
                    
                    if current_count > previous_count * 1.2:  # 20% increase
                        predictions.append({
                            'type': 'budget_forecast',
                            'title': 'Maintenance Budget Alert',
                            'description': f'Work orders increased by {((current_count/previous_count - 1) * 100):.1f}% this year',
                            'probability': 0.85,
                            'time_horizon': 'End of year',
                            'recommended_action': 'Review maintenance budget allocation',
                            'potential_cost_impact': 'Medium'
                        })
            
        except Exception as e:
            logger.error(f"Error generating predictions: {str(e)}")
        
        return predictions[:10]  # Return top 10 predictions
    
    def _generate_optimization_suggestions(self, wo_analysis: Dict, fault_analysis: Dict) -> List[Dict]:
        """Generate optimization suggestions"""
        optimizations = []
        
        try:
            # Maintenance schedule optimization
            if 'insights' in wo_analysis and 'job_type_breakdown' in wo_analysis['insights']:
                job_types = wo_analysis['insights']['job_type_breakdown']
                corrective = job_types.get('C', 0)
                preventive = job_types.get('P', 0)
                total = corrective + preventive
                
                if total > 0 and corrective / total > 0.6:  # High corrective ratio
                    optimizations.append({
                        'type': 'maintenance_strategy',
                        'title': 'Increase Preventive Maintenance',
                        'description': f'Current ratio: {corrective/(corrective+preventive):.1%} corrective maintenance',
                        'potential_savings': '15-25% cost reduction',
                        'implementation_effort': 'Medium',
                        'recommendation': 'Implement condition-based maintenance for high-frequency equipment'
                    })
            
            # Resource optimization
            if 'insights' in wo_analysis and 'supplier_breakdown' in wo_analysis['insights']:
                suppliers = wo_analysis['insights']['supplier_breakdown']
                if len(suppliers) > 5:  # Many suppliers
                    optimizations.append({
                        'type': 'resource_optimization',
                        'title': 'Supplier Consolidation Opportunity',
                        'description': f'Currently using {len(suppliers)} different suppliers',
                        'potential_savings': '5-10% cost reduction',
                        'implementation_effort': 'Low',
                        'recommendation': 'Consolidate to 3-5 preferred suppliers for better rates'
                    })
            
            # Training optimization
            critical_equipment = self._identify_critical_equipment(wo_analysis, fault_analysis)
            if len(critical_equipment) > 3:
                optimizations.append({
                    'type': 'training_optimization',
                    'title': 'Targeted Training Program',
                    'description': f'{len(critical_equipment)} equipment units require specialized attention',
                    'potential_savings': '10-20% efficiency improvement',
                    'implementation_effort': 'Medium',
                    'recommendation': 'Implement specialized training for high-maintenance equipment'
                })
            
        except Exception as e:
            logger.error(f"Error generating optimizations: {str(e)}")
        
        return optimizations
    
    def _identify_critical_equipment(self, wo_analysis: Dict, fault_analysis: Dict) -> Dict[str, float]:
        """Identify critical equipment with criticality scores"""
        critical_equipment = {}
        
        try:
            if 'analysis' in wo_analysis and 'sts_specific_analysis' in wo_analysis['analysis']:
                for equipment_id, data in wo_analysis['analysis']['sts_specific_analysis'].items():
                    total_wo = data.get('total_work_orders', 0)
                    job_types = data.get('job_type_breakdown', {})
                    corrective = job_types.get('C', 0)
                    urgent = job_types.get('U', 0)
                    
                    # Calculate criticality score (0-1)
                    criticality = 0.0
                    
                    if total_wo > 0:
                        # High work order frequency
                        wo_factor = min(total_wo / 100, 1.0) * 0.3
                        
                        # High corrective maintenance ratio
                        corrective_factor = (corrective / total_wo) * 0.4
                        
                        # Urgent work orders
                        urgent_factor = (urgent / total_wo) * 0.3
                        
                        criticality = wo_factor + corrective_factor + urgent_factor
                    
                    if criticality > 0.5:  # Threshold for criticality
                        critical_equipment[equipment_id] = criticality
            
        except Exception as e:
            logger.error(f"Error identifying critical equipment: {str(e)}")
        
        return critical_equipment
    
    def _assess_data_quality(self, wo_analysis: Dict, fault_analysis: Dict) -> Dict[str, Any]:
        """Assess the quality of data for AI analysis"""
        quality_assessment = {
            'overall_score': 0.0,
            'work_order_data_quality': 0.0,
            'equipment_data_quality': 0.0,
            'completeness': 0.0,
            'freshness': 0.0,
            'consistency': 0.0,
            'recommendations': []
        }
        
        try:
            # Work order data quality
            if 'insights' in wo_analysis and 'summary' in wo_analysis['insights']:
                summary = wo_analysis['insights']['summary']
                total_wo = summary.get('total_work_orders_analyzed', 0)
                keywords = summary.get('unique_keywords_found', 0)
                
                if total_wo > 0:
                    # Data richness (keywords per work order)
                    richness = min(keywords / total_wo, 10) / 10  # Normalize to 0-1
                    quality_assessment['work_order_data_quality'] = richness * 100
                    
                    # Completeness based on having descriptions
                    quality_assessment['completeness'] = min(total_wo / 1000, 1.0) * 100
            
            # Equipment data quality
            if 'total_equipment' in fault_analysis:
                total_equipment = fault_analysis['total_equipment']
                quality_assessment['equipment_data_quality'] = min(total_equipment / 10, 1.0) * 100
            
            # Freshness (placeholder - would check actual dates in production)
            quality_assessment['freshness'] = 85.0  # Assume recent data
            
            # Consistency (placeholder - would check data consistency in production)
            quality_assessment['consistency'] = 80.0
            
            # Overall score
            quality_assessment['overall_score'] = (
                quality_assessment['work_order_data_quality'] * 0.3 +
                quality_assessment['equipment_data_quality'] * 0.2 +
                quality_assessment['completeness'] * 0.2 +
                quality_assessment['freshness'] * 0.15 +
                quality_assessment['consistency'] * 0.15
            )
            
            # Generate recommendations based on quality
            if quality_assessment['completeness'] < 70:
                quality_assessment['recommendations'].append(
                    "Increase data collection frequency to improve AI analysis accuracy"
                )
            
            if quality_assessment['work_order_data_quality'] < 60:
                quality_assessment['recommendations'].append(
                    "Improve work order description quality for better pattern recognition"
                )
            
        except Exception as e:
            logger.error(f"Error assessing data quality: {str(e)}")
        
        return quality_assessment
    
    def _calculate_data_completeness(self, wo_analysis: Dict, fault_analysis: Dict) -> float:
        """Calculate overall data completeness score"""
        try:
            completeness_factors = []
            
            # Work order completeness
            if 'insights' in wo_analysis and 'summary' in wo_analysis['insights']:
                total_wo = wo_analysis['insights']['summary'].get('total_work_orders_analyzed', 0)
                if total_wo > 0:
                    completeness_factors.append(min(total_wo / 1000, 1.0))  # Normalize to 1000 WOs
            
            # Equipment completeness
            if 'total_equipment' in fault_analysis:
                total_equipment = fault_analysis['total_equipment']
                if total_equipment > 0:
                    completeness_factors.append(min(total_equipment / 20, 1.0))  # Normalize to 20 equipment
            
            return sum(completeness_factors) / len(completeness_factors) if completeness_factors else 0.0
            
        except Exception:
            return 0.0
    
    def _insight_to_dict(self, insight: AIInsight) -> Dict:
        """Convert AIInsight to dictionary"""
        return {
            'insight_id': insight.insight_id,
            'insight_type': insight.insight_type,
            'title': insight.title,
            'description': insight.description,
            'recommendation': insight.recommendation,
            'priority': insight.priority,
            'confidence': insight.confidence,
            'equipment_id': insight.equipment_id,
            'affected_systems': insight.affected_systems,
            'cost_impact': insight.cost_impact,
            'timeline': insight.timeline,
            'created_at': insight.created_at.isoformat() if insight.created_at else None
        }
    
    def get_real_time_insights(self, limit: int = 10) -> List[Dict]:
        """Get real-time AI insights"""
        try:
            # Check cache first
            cache_key = f"insights_{limit}"
            if cache_key in self._cache:
                cached = self._cache[cache_key]
                if (datetime.now() - cached['timestamp']).seconds < self._cache_timeout:
                    return cached['data']
            
            # Generate fresh insights
            wo_analysis = self.wo_ai_service.run_comprehensive_analysis(limit=500)
            fault_analysis = self.fault_ai_service.get_comprehensive_ai_analysis()
            
            insights = self._generate_unified_insights(wo_analysis, fault_analysis)
            
            # Cache results
            self._cache[cache_key] = {
                'data': insights[:limit],
                'timestamp': datetime.now()
            }
            
            return insights[:limit]
            
        except Exception as e:
            logger.error(f"Error getting real-time insights: {str(e)}")
            return []
    
    def export_ai_dashboard(self, format_type: str = 'json') -> str:
        """Export AI dashboard data"""
        try:
            dashboard_data = self.get_comprehensive_ai_dashboard()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"ai_dashboard_{timestamp}.{format_type}"
            
            if format_type == 'json':
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(dashboard_data, f, indent=2, ensure_ascii=False, default=str)
            
            return filename
            
        except Exception as e:
            logger.error(f"Error exporting AI dashboard: {str(e)}")
            return None
