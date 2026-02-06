#!/usr/bin/env python3
"""
Tarucca IoT Sensor Data Processor
Processes solar panel sensor data and generates analytical metrics.

This is the main file you need to complete for the case study.

Author: [Your Name Here]
"""

import csv
import json
import statistics
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def validate_data(record: dict) -> bool:
    """
    Validate sensor readings are within acceptable physical ranges.
    
    TODO: Implement validation logic
    
    Acceptable ranges for solar panel sensors:
    - voltage: 18V - 32V (24V nominal system with tolerance)
    - current: 0A - 12A (10A max with headroom)
    - temperature: -10°C to 80°C (operational range)
    - power: Should be >= 0 (calculated from V*I)
    
    Args:
        record: Dictionary with keys: voltage, current, temperature, power
        
    Returns:
        True if all readings are valid, False otherwise
        
    Example:
        >>> validate_data({'voltage': 24.5, 'current': 5.2, 'temperature': 35.0, 'power': 127.4})
        True
        >>> validate_data({'voltage': 50.0, 'current': 5.2, 'temperature': 35.0, 'power': 260.0})
        False
    """
    
    # TODO: Implement validation logic here
    # Hint: Check each sensor reading against the ranges above
    # Return False if ANY reading is outside acceptable range
    
    pass


def calculate_metrics(data: List[dict]) -> Dict:
    """
    Calculate comprehensive metrics from sensor data.
    
    TODO: Implement metric calculations
    
    Required metrics:
    - For voltage, current, temperature:
        - average (mean)
        - minimum
        - maximum
        - standard deviation (for voltage only)
    
    - Energy metrics:
        - total_energy_kwh: Sum of (power * time_interval) converted to kWh
          Assumption: Readings are 5 minutes apart
        - peak_power_hour: Timestamp of the hour with highest average power
    
    Args:
        data: List of dictionaries, each with validated sensor readings
        
    Returns:
        Dictionary with structure:
        {
            "voltage": {"avg": float, "min": float, "max": float, "std": float},
            "current": {"avg": float, "min": float, "max": float},
            "temperature": {"avg": float, "min": float, "max": float},
            "total_energy_kwh": float,
            "peak_power_hour": str (ISO format timestamp)
        }
        
    Hints:
    - Use statistics.mean(), min(), max(), statistics.stdev()
    - For energy: power (W) * time (hours) = energy (Wh), then convert to kWh
    - For peak hour: group readings by hour, find hour with max average power
    """
    
    # TODO: Implement metric calculations here
    
    metrics = {
        "voltage": {},
        "current": {},
        "temperature": {},
        "total_energy_kwh": 0.0,
        "peak_power_hour": None
    }
    
    return metrics


def process_sensor_data(input_file: str, output_dir: str = "data/processed") -> dict:
    """
    Main processing function - orchestrates the entire data pipeline.
    
    TODO: Complete this function
    
    Pipeline steps:
    1. Read CSV file using csv.DictReader
    2. Validate each record using validate_data()
    3. Collect valid records, count invalid ones
    4. Calculate metrics using calculate_metrics()
    5. Generate output JSON file
    6. Save to output directory
    7. Return processing results
    
    Args:
        input_file: Path to input CSV file
        output_dir: Directory where processed JSON should be saved
        
    Returns:
        Dictionary with processing results:
        {
            "input_file": str,
            "output_file": str,
            "processed_at": str (ISO timestamp),
            "status": "success" | "error",
            "records_processed": int,
            "records_invalid": int,
            "metrics": dict (from calculate_metrics),
            "error": str (optional, only if status is "error")
        }
        
    Error Handling:
    - If file doesn't exist: return status="error" with error message
    - If CSV is malformed: return status="error" with error message
    - If ALL records are invalid: return status="error"
    - Log errors but continue processing valid records
    
    Example output JSON:
    {
        "input_file": "solar_data_20250115_140523.csv",
        "output_file": "solar_data_20250115_140523_processed.json",
        "processed_at": "2025-01-15T14:07:45",
        "status": "success",
        "records_processed": 285,
        "records_invalid": 3,
        "metrics": { ... }
    }
    """
    
    # Initialize result structure
    result = {
        'input_file': Path(input_file).name,
        'output_file': None,
        'processed_at': datetime.now().isoformat(),
        'status': 'pending',
        'records_processed': 0,
        'records_invalid': 0,
        'metrics': {}
    }
    
    # TODO: Implement the processing logic
    # 
    # Suggested structure:
    # 
    # try:
    #     1. Check if input file exists
    #     2. Read CSV file
    #     3. Validate and collect valid records
    #     4. If no valid records, return error
    #     5. Calculate metrics
    #     6. Generate output filename
    #     7. Save JSON to output directory
    #     8. Update result with success status
    # except Exception as e:
    #     result['status'] = 'error'
    #     result['error'] = str(e)
    # 
    # return result
    
    # YOUR CODE HERE
    
    return result


def main():
    """
    CLI entry point - processes all CSV files in the incoming directory.
    
    This function is already complete - you don't need to modify it.
    It will use your process_sensor_data() function to process all files.
    """
    
    incoming_dir = Path("data/incoming")
    
    if not incoming_dir.exists():
        print(f"❌ Error: Directory {incoming_dir} does not exist")
        return
    
    csv_files = list(incoming_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"⚠️  No CSV files found in {incoming_dir}")
        print("   Run: python src/data_generator.py")
        return
    
    print("=" * 60)
    print("TARUCCA DATA PROCESSOR")
    print("=" * 60)
    print(f"Found {len(csv_files)} file(s) to process\n")
    
    results = []
    
    for csv_file in csv_files:
        print(f"📁 Processing: {csv_file.name}")
        result = process_sensor_data(str(csv_file))
        results.append(result)
        
        if result['status'] == 'success':
            print(f"   ✅ Success: {result['records_processed']} records processed")
            print(f"   📊 Output: {result['output_file']}")
            if result['records_invalid'] > 0:
                print(f"   ⚠️  Warning: {result['records_invalid']} invalid records skipped")
        else:
            print(f"   ❌ Error: {result.get('error', 'Unknown error')}")
        print()
    
    # Summary
    success_count = sum(1 for r in results if r['status'] == 'success')
    print("=" * 60)
    print(f"SUMMARY: {success_count}/{len(results)} files processed successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()
