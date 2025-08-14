import re
import statistics

def extract_execution_times(log_file_path):
    """
    Extract execution times from a log file.
    
    Args:
        log_file_path (str): Path to the log file
    
    Returns:
        dict: A dictionary containing execution times and their statistics
    """
    execution_times = []
    
    # Regular expression to match execution time logs
    time_pattern = re.compile(r'Execution time: (\d+\.\d+) seconds')
    
    try:
        with open(log_file_path, 'r') as file:
            for line in file:
                match = time_pattern.search(line)
                if match:
                    execution_times.append(float(match.group(1)))
    
    except FileNotFoundError:
        print(f"Error: File {log_file_path} not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
    # If no execution times found
    if not execution_times:
        print("No execution times found in the log file.")
        return None
    
    # Calculate statistics
    result = {
        'execution_times': execution_times,
        'count': len(execution_times),
        'min': min(execution_times),
        'max': max(execution_times),
        'average': statistics.mean(execution_times),
        'median': statistics.median(execution_times)
    }
    
    return result

# Example usage
def main():
    log_file_path = 'logs.txt'
    results = extract_execution_times(log_file_path)
    
    if results:
        print("\nAll Execution Times:")
        for time in results['execution_times']:
            if time > 10:
                print(f"{time:.4f} seconds")


        
        print("Execution Time Analysis:")
        print(f"Total executions: {results['count']}")
        print(f"Minimum time: {results['min']:.4f} seconds")
        print(f"Maximum time: {results['max']:.4f} seconds")
        print(f"Average time: {results['average']:.4f} seconds")
        print(f"Median time: {results['median']:.4f} seconds")

if __name__ == '__main__':
    main()