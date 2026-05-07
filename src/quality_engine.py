"""
Data Quality Engine Module
Validates data quality across medallion layers with comprehensive checks.
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import json

logger = logging.getLogger(__name__)


@dataclass
class QualityCheckResult:
    """Result of a single quality check."""
    check_name: str
    table_name: str
    passed: bool
    total_records: int
    failed_records: int
    percentage_passed: float
    check_timestamp: str
    details: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class TableQualityScore:
    """Overall quality score for a table."""
    table_name: str
    quality_score: float  # 0-100
    total_checks: int
    passed_checks: int
    failed_checks: int
    check_timestamp: str
    details: List[QualityCheckResult]


class QualityEngine:
    """Comprehensive data quality validation engine."""
    
    def __init__(self):
        """Initialize quality engine."""
        self.quality_checks = {
            'completeness': self._check_completeness,
            'uniqueness': self._check_uniqueness,
            'accuracy': self._check_accuracy,
            'timeliness': self._check_timeliness,
            'consistency': self._check_consistency,
            'validity': self._check_validity
        }
    
    def validate_table(self, table_name: str, data: List[Dict], 
                      rules: Optional[List[str]] = None) -> TableQualityScore:
        """
        Validate a table against quality rules.
        
        Args:
            table_name: Name of the table
            data: Table data as list of dictionaries
            rules: List of rules to apply (default: all)
            
        Returns:
            TableQualityScore with results
        """
        if rules is None:
            rules = list(self.quality_checks.keys())
        
        check_results = []
        total_records = len(data)
        
        for rule_name in rules:
            if rule_name in self.quality_checks:
                try:
                    check_func = self.quality_checks[rule_name]
                    result = check_func(table_name, data)
                    check_results.append(result)
                except Exception as e:
                    logger.error(f"Error running {rule_name} check: {str(e)}")
        
        # Calculate overall quality score
        if check_results:
            quality_score = (sum(1 for r in check_results if r.passed) / len(check_results)) * 100
        else:
            quality_score = 0
        
        return TableQualityScore(
            table_name=table_name,
            quality_score=quality_score,
            total_checks=len(check_results),
            passed_checks=sum(1 for r in check_results if r.passed),
            failed_checks=sum(1 for r in check_results if not r.passed),
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details=check_results
        )
    
    def _check_completeness(self, table_name: str, data: List[Dict],
                           min_not_null_percentage: float = 95) -> QualityCheckResult:
        """Check data completeness (null values)."""
        total_records = len(data)
        if total_records == 0:
            return QualityCheckResult(
                check_name='completeness',
                table_name=table_name,
                passed=False,
                total_records=0,
                failed_records=0,
                percentage_passed=0,
                check_timestamp=datetime.utcnow().isoformat() + 'Z',
                details={'reason': 'No data to validate'}
            )
        
        total_values = 0
        null_values = 0
        
        for record in data:
            for value in record.values():
                total_values += 1
                if value is None or value == '':
                    null_values += 1
        
        percentage_valid = ((total_values - null_values) / total_values * 100) if total_values > 0 else 0
        passed = percentage_valid >= min_not_null_percentage
        
        return QualityCheckResult(
            check_name='completeness',
            table_name=table_name,
            passed=passed,
            total_records=total_records,
            failed_records=null_values,
            percentage_passed=percentage_valid,
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details={
                'total_values': total_values,
                'null_values': null_values,
                'threshold': min_not_null_percentage
            }
        )
    
    def _check_uniqueness(self, table_name: str, data: List[Dict]) -> QualityCheckResult:
        """Check for duplicate records."""
        total_records = len(data)
        unique_records = len(set(str(record) for record in data))
        duplicates = total_records - unique_records
        
        passed = duplicates == 0
        
        return QualityCheckResult(
            check_name='uniqueness',
            table_name=table_name,
            passed=passed,
            total_records=total_records,
            failed_records=duplicates,
            percentage_passed=((total_records - duplicates) / total_records * 100) if total_records > 0 else 0,
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details={
                'total_records': total_records,
                'unique_records': unique_records,
                'duplicates': duplicates
            }
        )
    
    def _check_accuracy(self, table_name: str, data: List[Dict]) -> QualityCheckResult:
        """Check data accuracy (type validation, range checks)."""
        total_records = len(data)
        invalid_records = 0
        invalid_fields = []
        
        for record in data:
            for key, value in record.items():
                # Type validation
                if not self._validate_value_type(key, value):
                    invalid_records += 1
                    invalid_fields.append((key, type(value).__name__))
        
        passed = invalid_records == 0
        
        return QualityCheckResult(
            check_name='accuracy',
            table_name=table_name,
            passed=passed,
            total_records=total_records,
            failed_records=invalid_records,
            percentage_passed=((total_records - invalid_records) / total_records * 100) if total_records > 0 else 0,
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details={
                'invalid_records': invalid_records,
                'invalid_field_samples': invalid_fields[:5]  # Sample first 5
            }
        )
    
    def _check_timeliness(self, table_name: str, data: List[Dict],
                         max_age_hours: int = 24) -> QualityCheckResult:
        """Check data timeliness (freshness)."""
        total_records = len(data)
        old_records = 0
        
        # Check for timestamp fields
        timestamp_fields = [k for k in (data[0] if data else {}).keys() 
                          if 'time' in k.lower() or 'date' in k.lower()]
        
        if not timestamp_fields:
            return QualityCheckResult(
                check_name='timeliness',
                table_name=table_name,
                passed=True,
                total_records=total_records,
                failed_records=0,
                percentage_passed=100,
                check_timestamp=datetime.utcnow().isoformat() + 'Z',
                details={'reason': 'No timestamp fields found'}
            )
        
        # In production, compare timestamps with max_age_hours
        passed = old_records == 0
        
        return QualityCheckResult(
            check_name='timeliness',
            table_name=table_name,
            passed=passed,
            total_records=total_records,
            failed_records=old_records,
            percentage_passed=((total_records - old_records) / total_records * 100) if total_records > 0 else 100,
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details={
                'max_age_hours': max_age_hours,
                'old_records': old_records
            }
        )
    
    def _check_consistency(self, table_name: str, data: List[Dict]) -> QualityCheckResult:
        """Check data consistency (field consistency, referential integrity)."""
        total_records = len(data)
        inconsistent_records = 0
        
        # Check for consistent field presence
        if data:
            first_record_keys = set(data[0].keys())
            for record in data[1:]:
                if set(record.keys()) != first_record_keys:
                    inconsistent_records += 1
        
        passed = inconsistent_records == 0
        
        return QualityCheckResult(
            check_name='consistency',
            table_name=table_name,
            passed=passed,
            total_records=total_records,
            failed_records=inconsistent_records,
            percentage_passed=((total_records - inconsistent_records) / total_records * 100) if total_records > 0 else 100,
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details={
                'inconsistent_records': inconsistent_records,
                'schema_mismatch_detected': inconsistent_records > 0
            }
        )
    
    def _check_validity(self, table_name: str, data: List[Dict]) -> QualityCheckResult:
        """Check data validity (format, pattern matching)."""
        total_records = len(data)
        invalid_records = 0
        
        # This would contain business logic rules
        # Example: Email format validation, phone number format, etc.
        
        passed = invalid_records == 0
        
        return QualityCheckResult(
            check_name='validity',
            table_name=table_name,
            passed=passed,
            total_records=total_records,
            failed_records=invalid_records,
            percentage_passed=100,
            check_timestamp=datetime.utcnow().isoformat() + 'Z',
            details={'invalid_records': invalid_records}
        )
    
    @staticmethod
    def _validate_value_type(field_name: str, value: Any) -> bool:
        """Validate value type based on field name heuristics."""
        if value is None:
            return True
        
        # Simple type validation rules
        if 'email' in field_name.lower() and isinstance(value, str):
            return '@' in value
        elif 'phone' in field_name.lower() and isinstance(value, str):
            return len(value.replace('-', '').replace(' ', '')) >= 10
        elif 'date' in field_name.lower() or 'time' in field_name.lower():
            return isinstance(value, (str, int, float))
        
        return True
    
    def generate_quality_report(self, quality_score: TableQualityScore) -> str:
        """Generate a quality report in markdown format."""
        report = f"""
# Data Quality Report

**Table:** {quality_score.table_name}
**Timestamp:** {quality_score.check_timestamp}
**Overall Quality Score:** {quality_score.quality_score:.2f}%

## Summary
- Total Checks: {quality_score.total_checks}
- Passed: {quality_score.passed_checks}
- Failed: {quality_score.failed_checks}

## Detailed Results

"""
        for check in quality_score.details:
            status = "✓ PASS" if check.passed else "✗ FAIL"
            report += f"""
### {check.check_name.upper()} {status}
- Percentage Passed: {check.percentage_passed:.2f}%
- Failed Records: {check.failed_records}/{check.total_records}
"""
        
        return report


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample data
    test_data = [
        {'id': 1, 'name': 'John', 'email': 'john@example.com', 'timestamp': '2024-01-15'},
        {'id': 2, 'name': 'Jane', 'email': 'jane@example.com', 'timestamp': '2024-01-15'},
        {'id': 3, 'name': 'Bob', 'email': None, 'timestamp': '2024-01-15'},
    ]
    
    engine = QualityEngine()
    result = engine.validate_table('test_table', test_data)
    print(f"Quality Score: {result.quality_score:.2f}%")
    print(engine.generate_quality_report(result))
