#!/bin/bash

# Setup Virtual Python Environment
python3 -m venv .venv
source .venv/bin/activate

# Install Python Packages
cd airflow

pip3 install -r requirements.txt