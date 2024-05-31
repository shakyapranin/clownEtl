## CLOWN ETL

# This repository contains ETL Scripts to load files from a watched directory into raw tables, Transform then stage (to test scripts against) and once verified load into a production database.

# Prerequisites

- python3
- sqlite3

# Installation instructions

- Create python virtual environment `python3 -m venv myenv`
- Activate python virtual environment `source myenv/bin/activate`
- Install requirements `pip install -r requirements`
- Create a docs directory
- Run script `python3 main.py`
- Move csv files into the docs directory