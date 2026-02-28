# Databricks notebook

# Add repo src to path
import sys
sys.path.append("/Workspace/Repos/YOUR_USERNAME/europe3-transactions/src")

from jobs.run_pipeline import run

run()