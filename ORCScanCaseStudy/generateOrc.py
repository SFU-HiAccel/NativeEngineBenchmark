import pyarrow as pa
import pyarrow.orc as orc
import numpy as np

def generate_orc_file(num_rows, output_path):
    """
    Generate an ORC file with a single column of random integers
    
    Args:
        num_rows (int): Number of rows to generate
        output_path (str): Path where the ORC file will be saved
    """
    # Generate random integers between 0 and 1000000
    data = np.random.randint(0, 1000000, size=num_rows, dtype=np.int32)
    
    # Create Arrow array and table
    arr = pa.array(data, type=pa.int32())
    table = pa.Table.from_arrays([arr], names=['numbers'])
    
    # Write to ORC file
    with pa.OSFile(output_path, 'wb') as f:
        orc.write_table(table, f, compression='zlib')
    
    print(f"Generated ORC file with {num_rows} rows at {output_path}")

def main():
    for power in range(20, 30):
        num_rows = 2 ** power
        output_path = f'data_rows_{power}.orc'
        generate_orc_file(num_rows, output_path)
        
        # Print file information
        table = orc.read_table(output_path)
        print(f"Verified file {output_path}:")
        print(f"Number of rows: {len(table)}")
        print(f"Schema: {table.schema}")
        print("-" * 50)

if __name__ == "__main__":
    main()