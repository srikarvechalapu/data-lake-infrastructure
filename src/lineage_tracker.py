"""
Data Lineage Tracker Module
Tracks and manages data lineage across medallion layers.
"""

import logging
from typing import Dict, List, Set, Optional
from dataclasses import dataclass, field
from datetime import datetime
import json

logger = logging.getLogger(__name__)


@dataclass
class DataAsset:
    """Represents a data asset in the data lake."""
    asset_id: str
    asset_name: str
    asset_type: str  # table, view, report, etc.
    layer: str  # bronze, silver, gold
    owner: str
    description: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')
    classification: str = 'internal'  # public, internal, confidential, restricted
    tags: List[str] = field(default_factory=list)


@dataclass
class LineageRelation:
    """Represents a lineage relationship between assets."""
    source_asset_id: str
    target_asset_id: str
    transformation_type: str  # copy, aggregate, join, etc.
    transformation_code: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')
    data_volume_mb: Optional[float] = None


class LineageTracker:
    """Tracks data lineage across the data lake."""
    
    def __init__(self):
        """Initialize lineage tracker."""
        self.assets: Dict[str, DataAsset] = {}
        self.relations: List[LineageRelation] = []
        self.asset_lineage_cache: Dict[str, Dict] = {}
    
    def register_asset(self, asset: DataAsset) -> None:
        """
        Register a data asset in the lineage system.
        
        Args:
            asset: DataAsset to register
        """
        self.assets[asset.asset_id] = asset
        logger.info(f"Registered asset: {asset.asset_name} ({asset.asset_id})")
    
    def create_lineage_relation(self, source_asset_id: str, target_asset_id: str,
                               transformation_type: str,
                               transformation_code: Optional[str] = None,
                               data_volume_mb: Optional[float] = None) -> None:
        """
        Create a lineage relationship between two assets.
        
        Args:
            source_asset_id: ID of source asset
            target_asset_id: ID of target asset
            transformation_type: Type of transformation (copy, aggregate, join, etc.)
            transformation_code: Optional SQL/code snippet
            data_volume_mb: Optional data volume transferred
        """
        if source_asset_id not in self.assets:
            logger.warning(f"Source asset not found: {source_asset_id}")
            return
        
        if target_asset_id not in self.assets:
            logger.warning(f"Target asset not found: {target_asset_id}")
            return
        
        relation = LineageRelation(
            source_asset_id=source_asset_id,
            target_asset_id=target_asset_id,
            transformation_type=transformation_type,
            transformation_code=transformation_code,
            data_volume_mb=data_volume_mb
        )
        
        self.relations.append(relation)
        self.asset_lineage_cache.clear()  # Invalidate cache
        logger.info(f"Created lineage: {source_asset_id} → {target_asset_id}")
    
    def get_upstream_lineage(self, asset_id: str, depth: int = -1) -> Dict:
        """
        Get upstream lineage (dependencies) for an asset.
        
        Args:
            asset_id: ID of the asset
            depth: Maximum depth to traverse (-1 for unlimited)
            
        Returns:
            Dictionary with upstream assets and relations
        """
        if asset_id not in self.assets:
            logger.warning(f"Asset not found: {asset_id}")
            return {'asset_id': asset_id, 'sources': [], 'relations': []}
        
        visited = set()
        upstream = self._traverse_upstream(asset_id, visited, depth)
        
        return {
            'asset_id': asset_id,
            'asset_name': self.assets[asset_id].asset_name,
            'upstream_assets': upstream['assets'],
            'upstream_relations': upstream['relations'],
            'total_upstream_assets': len(upstream['assets'])
        }
    
    def get_downstream_lineage(self, asset_id: str, depth: int = -1) -> Dict:
        """
        Get downstream lineage (consumers) for an asset.
        
        Args:
            asset_id: ID of the asset
            depth: Maximum depth to traverse (-1 for unlimited)
            
        Returns:
            Dictionary with downstream assets and relations
        """
        if asset_id not in self.assets:
            logger.warning(f"Asset not found: {asset_id}")
            return {'asset_id': asset_id, 'targets': [], 'relations': []}
        
        visited = set()
        downstream = self._traverse_downstream(asset_id, visited, depth)
        
        return {
            'asset_id': asset_id,
            'asset_name': self.assets[asset_id].asset_name,
            'downstream_assets': downstream['assets'],
            'downstream_relations': downstream['relations'],
            'total_downstream_assets': len(downstream['assets'])
        }
    
    def get_full_lineage(self, asset_id: str) -> Dict:
        """Get complete lineage (upstream + downstream) for an asset."""
        upstream = self.get_upstream_lineage(asset_id)
        downstream = self.get_downstream_lineage(asset_id)
        
        return {
            'asset_id': asset_id,
            'asset_name': self.assets[asset_id].asset_name if asset_id in self.assets else None,
            'upstream': upstream,
            'downstream': downstream,
            'total_related_assets': len(upstream['upstream_assets']) + len(downstream['downstream_assets'])
        }
    
    def get_lineage_impact(self, asset_id: str) -> Dict:
        """
        Analyze impact of changes to an asset.
        Shows all downstream consumers that would be affected.
        """
        downstream = self.get_downstream_lineage(asset_id)
        
        impact = {
            'asset_id': asset_id,
            'asset_name': self.assets[asset_id].asset_name if asset_id in self.assets else None,
            'direct_impact': [],
            'indirect_impact': [],
            'high_priority_impact': []
        }
        
        for downstream_asset_id in downstream['downstream_assets']:
            downstream_asset = self.assets.get(downstream_asset_id)
            if downstream_asset:
                asset_info = {
                    'asset_id': downstream_asset.asset_id,
                    'asset_name': downstream_asset.asset_name,
                    'layer': downstream_asset.layer,
                    'classification': downstream_asset.classification,
                    'owner': downstream_asset.owner
                }
                
                # Categorize impact
                if downstream_asset.classification in ['restricted', 'confidential']:
                    impact['high_priority_impact'].append(asset_info)
                elif downstream_asset.asset_type == 'report':
                    impact['direct_impact'].append(asset_info)
                else:
                    impact['indirect_impact'].append(asset_info)
        
        return impact
    
    def get_data_lineage_report(self, asset_id: str) -> str:
        """Generate a markdown report for data lineage."""
        lineage = self.get_full_lineage(asset_id)
        impact = self.get_lineage_impact(asset_id)
        
        report = f"""
# Data Lineage Report

**Asset:** {lineage['asset_name']} ({asset_id})
**Generated:** {datetime.utcnow().isoformat()}

## Summary
- Total Related Assets: {lineage['total_related_assets']}
- Upstream Dependencies: {len(lineage['upstream']['upstream_assets'])}
- Downstream Consumers: {len(lineage['downstream']['downstream_assets'])}

## Upstream Lineage (Dependencies)
"""
        
        if lineage['upstream']['upstream_assets']:
            for upstream_asset_id in lineage['upstream']['upstream_assets']:
                asset = self.assets.get(upstream_asset_id)
                if asset:
                    report += f"\n- **{asset.asset_name}** ({asset.asset_type}) - {asset.layer} layer"
        else:
            report += "\nNo upstream dependencies found."
        
        report += "\n\n## Downstream Lineage (Consumers)\n"
        
        if lineage['downstream']['downstream_assets']:
            for downstream_asset_id in lineage['downstream']['downstream_assets']:
                asset = self.assets.get(downstream_asset_id)
                if asset:
                    report += f"\n- **{asset.asset_name}** ({asset.asset_type}) - {asset.layer} layer"
        else:
            report += "\nNo downstream consumers found."
        
        report += "\n\n## Change Impact Analysis\n"
        report += f"- Direct Impact (Reports): {len(impact['direct_impact'])}\n"
        report += f"- Indirect Impact: {len(impact['indirect_impact'])}\n"
        report += f"- High Priority Impact: {len(impact['high_priority_impact'])}\n"
        
        if impact['high_priority_impact']:
            report += "\n### ⚠️ High Priority Assets Affected\n"
            for asset_info in impact['high_priority_impact']:
                report += f"- {asset_info['asset_name']} (Owner: {asset_info['owner']}, Classification: {asset_info['classification']})\n"
        
        return report
    
    def _traverse_upstream(self, asset_id: str, visited: Set[str], depth: int) -> Dict:
        """Traverse upstream lineage recursively."""
        if depth == 0 or asset_id in visited:
            return {'assets': [], 'relations': []}
        
        visited.add(asset_id)
        upstream_assets = []
        upstream_relations = []
        
        for relation in self.relations:
            if relation.target_asset_id == asset_id:
                upstream_assets.append(relation.source_asset_id)
                upstream_relations.append({
                    'source': relation.source_asset_id,
                    'target': relation.target_asset_id,
                    'transformation': relation.transformation_type
                })
                
                # Recursively traverse
                if depth != 1:
                    next_depth = depth - 1 if depth > 1 else -1
                    nested = self._traverse_upstream(relation.source_asset_id, visited.copy(), next_depth)
                    upstream_assets.extend(nested['assets'])
                    upstream_relations.extend(nested['relations'])
        
        return {'assets': list(set(upstream_assets)), 'relations': upstream_relations}
    
    def _traverse_downstream(self, asset_id: str, visited: Set[str], depth: int) -> Dict:
        """Traverse downstream lineage recursively."""
        if depth == 0 or asset_id in visited:
            return {'assets': [], 'relations': []}
        
        visited.add(asset_id)
        downstream_assets = []
        downstream_relations = []
        
        for relation in self.relations:
            if relation.source_asset_id == asset_id:
                downstream_assets.append(relation.target_asset_id)
                downstream_relations.append({
                    'source': relation.source_asset_id,
                    'target': relation.target_asset_id,
                    'transformation': relation.transformation_type
                })
                
                # Recursively traverse
                if depth != 1:
                    next_depth = depth - 1 if depth > 1 else -1
                    nested = self._traverse_downstream(relation.target_asset_id, visited.copy(), next_depth)
                    downstream_assets.extend(nested['assets'])
                    downstream_relations.extend(nested['relations'])
        
        return {'assets': list(set(downstream_assets)), 'relations': downstream_relations}


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    tracker = LineageTracker()
    
    # Register assets
    bronze_customers = DataAsset(
        asset_id='bronze_customers',
        asset_name='Bronze: Customers',
        asset_type='table',
        layer='bronze',
        owner='data-engineering',
        description='Raw customer data from CRM'
    )
    
    silver_customers = DataAsset(
        asset_id='silver_customers',
        asset_name='Silver: Customers (Cleaned)',
        asset_type='table',
        layer='silver',
        owner='data-engineering'
    )
    
    gold_customers = DataAsset(
        asset_id='gold_customers',
        asset_name='Gold: Customer Dimension',
        asset_type='table',
        layer='gold',
        owner='analytics'
    )
    
    tracker.register_asset(bronze_customers)
    tracker.register_asset(silver_customers)
    tracker.register_asset(gold_customers)
    
    # Create lineage relations
    tracker.create_lineage_relation(
        'bronze_customers', 'silver_customers',
        'clean_and_deduplicate'
    )
    tracker.create_lineage_relation(
        'silver_customers', 'gold_customers',
        'dimension_modeling'
    )
    
    # Get lineage
    lineage = tracker.get_full_lineage('gold_customers')
    print(json.dumps(lineage, indent=2))
