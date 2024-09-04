import subprocess
import time

# List of Python scripts to be executed in the desired sequence
scripts = [
    "pharma-data-warehouse-edna\PHM_ADMIN_TOOLS\GENERATORS\MODULES\gen_git_dag_pdw.py"
    ]


# Iterate over the list of scripts and execute each one
for script in scripts:
    # Ask the user if they want to execute this script
    execute = input(f"Do you want to execute {script}? (y/n) ")

    # Only execute the script if the user entered 'y'
    if execute.lower() == 'y':
        # Get the start time
        start = time.time()
        print(f"Running script {script} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start))}")

        # Run the script and wait for it to complete
        subprocess.run(["python", script], check=True)

        # Get the end time
        end = time.time()
        elapsed_time = end - start
        print(f"Finished running script {script} at {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(end))}")
        print(f"Elapsed time: {elapsed_time} seconds")

print("PDW EDnA DDLs Generated, Please Commit Changes")