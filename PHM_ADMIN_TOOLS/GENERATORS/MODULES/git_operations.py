# git_operations.py

import os
import subprocess
import sys

def switch_to_branch(repo_path, branch_name):
	original_dir = os.getcwd()  # Save the current directory
	os.chdir(repo_path)  # Change to your repository's directory

	try:
		current_branch = subprocess.check_output(['git', 'rev-parse', '--abbrev-ref', 'HEAD']).decode('utf-8').strip()
		if current_branch == branch_name:
			print(f"Already on '{branch_name}'")
		else:
			# Discard all changes in the working directory and index
			subprocess.check_call(['git', 'reset', '--hard'])
			print("Discarded all changes in the working directory and index.")
			
			# Fetch latest branches
			subprocess.check_call(['git', 'fetch'])
			# Switch to the desired branch
			subprocess.check_call(['git', 'checkout', branch_name])
			print(f"Switched to branch '{branch_name}'")
	except subprocess.CalledProcessError as e:
		print(f"Failed to switch to branch '{branch_name}'. Error: {e}", file=sys.stderr)
		sys.exit(1)
	finally:
		os.chdir(original_dir)  # Change back to the original directory