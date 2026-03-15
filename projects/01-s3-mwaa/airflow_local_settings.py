"""
Airflow local settings for MWAA
This file is automatically loaded by Airflow on startup
"""
import sys
import os

# Add lib/ folder to Python path for hot-reloadable custom libraries
lib_path = '/usr/local/airflow/lib'
if lib_path not in sys.path:
    sys.path.insert(0, lib_path)

print(f"Added {lib_path} to Python path")
