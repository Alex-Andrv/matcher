#!/bin/bash

# Define the name of the Conda environment and Python app script
conda_env_name="matcher"
python_app_module="matcher"

# Check if the Conda environment exists
if ! conda env list | grep -q "$conda_env_name"; then
  environment_file="environment.yml"

  # Create and activate the Conda environment using the environment.yml file
  conda env create -f "$environment_file"
fi

# Activate the Conda environment
conda activate "$conda_env_name"

# Run your Python app
python -m "$python_app_module"