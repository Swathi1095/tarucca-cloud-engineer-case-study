#!/usr/bin/env python3
"""
Tarucca Solar Panel Data Generator
Generates realistic time-series sensor data for testing.

Author: Hernani Costa, Tarucca BV
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
import math
import sys


def generate_solar_data(
    output_file: str,
    hours: int = 24,
    interval_minutes: int = 5,
    add_anomalies: bool = True
):
    """
    Generate realistic solar panel sensor data with natural patterns.
    
    Simulates a full day of solar panel operation with:
    - Sunrise to sunset patterns
    - Realistic voltage/current variations
    - Temperature correlation with solar intensity
    - Optional anomalies for testing error handling
    
    Args:
        output_file: Path to output CSV file
        hours: Number of hours to simulate (default: 24)
        interval_minutes: Minutes between readings (default: 5)
        add_anomalies: Include some anomalous readings (default: True)
    
    Returns:
        Number of records generated
    """
    
    # Start at 6 AM (sunrise)
    start_time = datetime.now().replace(hour=6, minute=0, second=0, microsecond=0)
    records = []
    
    num_records = (hours * 60) // interval_minutes
    
    print(f"Generating {num_records} sensor readings...")
    print(f"Simulation period: {hours} hours with {interval_minutes}-minute intervals")
    
    for i in range(num_records):
        timestamp = start_time + timedelta(minutes=i * interval_minutes)
        hour = timestamp.hour
        
        # Calculate sun intensity using sine wave (peak at noon)
        # Maps 6 AM - 6 PM to a sine curve
        sun_intensity = max(0, math.sin((hour - 6) * math.pi / 12))
        
        # Voltage: Nominal 24V system, varies 20-28V based on sun
        base_voltage = 24.0
        voltage = base_voltage + (sun_intensity * 4.0) + random.uniform(-0.5, 0.5)
        
        # Current: 0-10A based on solar intensity
        current = sun_intensity * 10.0 + random.uniform(-0.3, 0.3)
        current = max(0, current)  # Can't be negative
        
        # Temperature: Ambient 20°C, panel heats up with sun (up to 45°C)
        ambient_temp = 20.0
        temp = ambient_temp + (sun_intensity * 25.0) + random.uniform(-2, 2)
        
        # Power calculation: P = V * I
        power = voltage * current
        
        # Add occasional anomalies to test error handling
        if add_anomalies and random.random() < 0.02:  # 2% chance
            anomaly_type = random.choice(['voltage_spike', 'sensor_disconnect', 'temp_error'])
            
            if anomaly_type == 'voltage_spike':
                voltage = random.uniform(35, 40)  # Over-voltage
            elif anomaly_type == 'sensor_disconnect':
                current = 0
                voltage = random.uniform(10, 15)  # Low voltage
            elif anomaly_type == 'temp_error':
                temp = random.uniform(-50, -20)  # Impossible reading
        
        records.append({
            'timestamp': timestamp.isoformat(),
            'voltage': round(voltage, 2),
            'current': round(current, 2),
            'temperature': round(temp, 1),
            'power': round(power, 2)
        })
    
    # Ensure output directory exists
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to CSV
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=['timestamp', 'voltage', 'current', 'temperature', 'power']
        )
        writer.writeheader()
        writer.writerows(records)
    
    print(f"✅ Successfully generated {len(records)} records")
    print(f"📁 Output file: {output_file}")
    print(f"📊 File size: {output_path.stat().st_size / 1024:.2f} KB")
    
    # Print sample statistics
    voltages = [r['voltage'] for r in records]
    currents = [r['current'] for r in records]
    temps = [r['temperature'] for r in records]
    
    print("\n📈 Sample Statistics:")
    print(f"   Voltage: {min(voltages):.2f}V - {max(voltages):.2f}V")
    print(f"   Current: {min(currents):.2f}A - {max(currents):.2f}A")
    print(f"   Temperature: {min(temps):.1f}°C - {max(temps):.1f}°C")
    
    return len(records)


if __name__ == "__main__":
    # Generate a timestamped sample file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/incoming/solar_data_{timestamp}.csv"
    
    print("=" * 60)
    print("TARUCCA SOLAR DATA GENERATOR")
    print("=" * 60)
    
    generate_solar_data(
        filename,
        hours=24,
        interval_minutes=5,
        add_anomalies=True
    )
    
    print("\n✅ Data generation complete!")
    print(f"🚀 Ready to process: python src/processor.py")
    print("=" * 60)
