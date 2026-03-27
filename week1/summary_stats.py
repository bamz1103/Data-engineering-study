import pandas as pd
import os

def get_stats(file_path):
    """
    Reads a CSV file and prints summary statistics.
    Includes error handling to prevent crashes if the file is missing.
    """
    try:
        # Check if the file exists before trying to read it
        if not os.path.exists(file_path):
            print(f"Error: The file '{file_path}' was not found.")
            return

        df = pd.read_csv(file_path)
        
        print("--- Data Summary (Numerical) ---")
        print(df.describe())
        
        print("\n--- Column Information ---")
        print(df.info())
        
        print("\n--- First 5 Rows ---")
        print(df.head())

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # 1. Create a dummy dataset so the script has something to process
    # In a real PwC project, this would be a large database file.
    data = {
        'Employee_ID': [101, 102, 103, 104, 105],
        'Monthly_Sales': [2500, 3100, 2800, 4200, 3900],
        'Project_Hours': [40, 45, 38, 50, 42]
    }
    
    df_test = pd.DataFrame(data)
    
    # Ensure the file is saved in the week1 folder
    csv_name = 'week1/sample_data.csv'
    df_test.to_csv(csv_name, index=False)
    
    # 2. Run the function
    get_stats(csv_name)