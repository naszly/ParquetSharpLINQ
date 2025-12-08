#!/usr/bin/env python3
"""
Generates test Delta Lake tables for ParquetSharpLINQ integration tests.
Requires: pip install pandas pyarrow deltalake
"""

import os
import shutil
import pandas as pd
from deltalake import write_deltalake, DeltaTable

def generate_simple_delta_table(output_path):
    """Generate a simple non-partitioned Delta table."""
    print(f"Creating simple Delta table at {output_path}")
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'],
        'amount': [100.50, 200.75, 150.25, 300.00, 175.80],
        'date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19']
    }
    
    df = pd.DataFrame(data)
    write_deltalake(output_path, df, mode='overwrite')
    
    print(f"  Created {len(df)} records")

def generate_partitioned_delta_table(output_path):
    """Generate a Delta table with year/month partitions."""
    print(f"Creating partitioned Delta table at {output_path}")
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    data = []
    id_counter = 1
    
    for year in [2023, 2024]:
        for month in range(1, 13):
            for day in range(1, 6):
                data.append({
                    'id': id_counter,
                    'name': f'User{id_counter}',
                    'amount': round(50.0 + (id_counter * 10.5), 2),
                    'year': year,
                    'month': month,
                    'date': f'{year}-{month:02d}-{day:02d}'
                })
                id_counter += 1
    
    df = pd.DataFrame(data)
    write_deltalake(output_path, df, mode='overwrite', partition_by=['year', 'month'])
    
    print(f"  Created {len(df)} records across partitions")

def generate_delta_table_with_updates(output_path):
    """Generate a Delta table with add/remove actions (updates/deletes)."""
    print(f"Creating Delta table with updates at {output_path}")
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    # Initial data
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
        'quantity': [100, 200, 150, 300, 250],
        'year': [2024, 2024, 2024, 2023, 2023]
    }
    
    df = pd.DataFrame(data)
    write_deltalake(output_path, df, mode='overwrite', partition_by=['year'])
    print(f"  Initial write: {len(df)} records")
    
    # Load as Delta table for updates
    dt = DeltaTable(output_path)
    
    # Update some records (simulate update by deleting and re-adding)
    update_data = {
        'id': [2, 3],
        'name': ['Product B Updated', 'Product C Updated'],
        'quantity': [250, 175],
        'year': [2024, 2024]
    }
    update_df = pd.DataFrame(update_data)
    
    # Merge operation (this creates remove + add actions in Delta log)
    dt.merge(
        update_df,
        predicate='target.id = source.id',
        source_alias='source',
        target_alias='target'
    ).when_matched_update_all().execute()
    
    print(f"  Updated 2 records")
    
    # Delete a record
    dt.delete("id = 5")
    print(f"  Deleted 1 record")
    
    # Add new records
    new_data = {
        'id': [6, 7],
        'name': ['Product F', 'Product G'],
        'quantity': [400, 350],
        'year': [2024, 2023]
    }
    new_df = pd.DataFrame(new_data)
    write_deltalake(output_path, new_df, mode='append', partition_by=['year'])
    
    print(f"  Added 2 new records")
    
    # Show final count
    final_df = dt.to_pandas()
    print(f"  Final count: {len(final_df)} records")

def generate_delta_table_with_string_partitions(output_path):
    """Generate a Delta table with string-based partitions (region)."""
    print(f"Creating Delta table with string partitions at {output_path}")
    
    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    
    data = []
    id_counter = 1
    regions = ["us-east", "us-west", "eu-west", "eu-central", "ap-south"]
    
    for region in regions:
        for i in range(10):
            data.append({
                'id': id_counter,
                'order_name': f'Order{id_counter}',
                'total': round(100.0 + (id_counter * 5.5), 2),
                'region': region,
                'year': 2024
            })
            id_counter += 1
    
    df = pd.DataFrame(data)
    write_deltalake(output_path, df, mode='overwrite', partition_by=['year', 'region'])
    
    print(f"  Created {len(df)} records across {len(regions)} regions")

def main():
    """Generate all test Delta tables."""
    base_path = os.path.join(os.path.dirname(__file__), "delta_test_data")
    
    print("=" * 60)
    print("Delta Lake Test Data Generator")
    print("=" * 60)
    print()
    
    # Create base directory
    os.makedirs(base_path, exist_ok=True)
    
    # Generate different test scenarios
    try:
        generate_simple_delta_table(os.path.join(base_path, "simple_delta"))
        print()
        
        generate_partitioned_delta_table(os.path.join(base_path, "partitioned_delta"))
        print()
        
        generate_delta_table_with_updates(os.path.join(base_path, "delta_with_updates"))
        print()
        
        generate_delta_table_with_string_partitions(os.path.join(base_path, "delta_string_partitions"))
        print()
        
        print("=" * 60)
        print("All Delta test tables created successfully!")
        print(f"Location: {base_path}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())

