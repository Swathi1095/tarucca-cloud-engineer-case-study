#!/usr/bin/env python3
"""
Tarucca IoT Sensor Data Processor
Processes solar panel sensor data and generates analytical metrics.

This is the main file you need to complete for the case study.

Author: Swathi Nadakuditi
"""

import csv
import json
import statistics
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


def validate_data(record: dict) -> bool:
    try:
        voltage = float(record["voltage"])
        if voltage < 18 or voltage > 32:
            return False

        current = float(record["current"])
        if current < 0 or current > 12:
            return False

        temperature = float(record["temperature"])
        if temperature < -10 or temperature > 80:
            return False

        power = float(record["power"])
        if power < 0:
            return False

        return True

    except:
        return False



def calculate_metrics(data: List[dict]) -> Dict:
    # Create empty lists to store values
    voltages = []
    currents = []
    temperatures = []
    powers = []

    # Fill the lists with values from each record
    for record in data:
        voltages.append(record["voltage"])
        currents.append(record["current"])
        temperatures.append(record["temperature"])
        powers.append(record["power"])

    # Basic statistics
    voltage_avg = statistics.mean(voltages)
    voltage_min = min(voltages)
    voltage_max = max(voltages)

    # Standard deviation only if more than 1 value
    if len(voltages) > 1:
        voltage_std = statistics.stdev(voltages)
    else:
        voltage_std = 0

    current_avg = statistics.mean(currents)
    current_min = min(currents)
    current_max = max(currents)

    temperature_avg = statistics.mean(temperatures)
    temperature_min = min(temperatures)
    temperature_max = max(temperatures)

    # Energy calculation (kWh)
    total_energy_kwh = 0
    interval_hours = 5 / 60  

    for p in powers:
        total_energy_kwh += (p * interval_hours) / 1000

    # Peak power hour
    hourly_power = {}

    for record in data:
        ts = datetime.fromisoformat(record["timestamp"])
        hour = ts.replace(minute=0, second=0, microsecond=0)

        if hour not in hourly_power:
            hourly_power[hour] = []

        hourly_power[hour].append(record["power"])

    # Find hour with highest average power
    peak_hour = None
    highest_avg = -1

    for hour, values in hourly_power.items():
        avg_power = statistics.mean(values)
        if avg_power > highest_avg:
            highest_avg = avg_power
            peak_hour = hour

    return {
        "voltage": {
            "avg": voltage_avg,
            "min": voltage_min,
            "max": voltage_max,
            "std": voltage_std
        },
        "current": {
            "avg": current_avg,
            "min": current_min,
            "max": current_max
        },
        "temperature": {
            "avg": temperature_avg,
            "min": temperature_min,
            "max": temperature_max
        },
        "total_energy_kwh": total_energy_kwh,
        "peak_power_hour": peak_hour.isoformat()
    }




def process_sensor_data(input_file: str, output_dir: str = "data/processed") -> dict:
    result = {
        "input_file": Path(input_file).name,
        "output_file": None,
        "processed_at": datetime.now().isoformat(),
        "status": "pending",
        "records_processed": 0,
        "records_invalid": 0,
        "metrics": {}
    }

    try:
        # 1. Check if file exists
        input_path = Path(input_file)
        if not input_path.exists():
            result["status"] = "error"
            result["error"] = "Input file does not exist"
            return result

        valid_records = []
        invalid_count = 0

        # 2. Read CSV file
        with open(input_path, "r") as f:
            reader = csv.DictReader(f)

            # 3. Loop through each row
            for row in reader:
                try:
                    # Convert values to float
                    row["voltage"] = float(row["voltage"])
                    row["current"] = float(row["current"])
                    row["temperature"] = float(row["temperature"])
                    row["power"] = float(row["power"])
                except:
                    invalid_count += 1
                    continue

                # 4. Validate row
                if validate_data(row):
                    valid_records.append(row)
                else:
                    invalid_count += 1

        # 5. If no valid records
        if len(valid_records) == 0:
            result["status"] = "error"
            result["records_invalid"] = invalid_count
            result["error"] = "All records invalid"
            return result

        # 6. Calculate metrics
        metrics = calculate_metrics(valid_records)

        # 7. Save JSON output
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        output_filename = input_path.stem + "_processed.json"
        output_path = output_dir / output_filename

        with open(output_path, "w") as f:
            json.dump({
                "input_file": result["input_file"],
                "output_file": output_filename,
                "processed_at": result["processed_at"],
                "status": "success",
                "records_processed": len(valid_records),
                "records_invalid": invalid_count,
                "metrics": metrics
            }, f, indent=4)

        # 8. Update result
        result["status"] = "success"
        result["output_file"] = output_filename
        result["records_processed"] = len(valid_records)
        result["records_invalid"] = invalid_count
        result["metrics"] = metrics

        return result

    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
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
