import os
import yaml
from dune_client.client import DuneClient
from dotenv import load_dotenv
import sys

dotenv_path = os.path.join(os.path.dirname(__file__), '.', '.env')
load_dotenv(dotenv_path)

dune = DuneClient.from_env()

# Read the queries.yml file
queries_yml = os.path.join(os.path.dirname(__file__), '.', 'queries.yml')
with open(queries_yml, 'r', encoding='utf-8') as file:
    data = yaml.safe_load(file)

# Extract the query_ids from the data
query_ids = [id for id in data['query_ids']]

for id in query_ids:
    # print(id)
    results = dune.get_latest_result(id, max_age_hours=24)
    print(results.result.rows)