import requests
import json
import time
from datetime import datetime
# Disable warnings for insecure requests
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Configuration
INDEX_PREFIX = 'my-index'
OPENSEARCH_URL = 'https://localhost:9200'
USERNAME = 'admin'
PASSWORD = 'admin'
INDEX_TEMPLATE = 'my-index-template'
LIFECYCLE_POLICY = 'my-lifecycle-policy'

# create opensearch index mapping template
def create_index_template():
    with open('index_template.json') as f:
        template = json.load(f)
    response = requests.put(f'{OPENSEARCH_URL}/_index_template/{INDEX_TEMPLATE}', json=template, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print("Index template created")

# create opensearch index
def create_index(index):
    response = requests.put(f'{OPENSEARCH_URL}/{index}', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print(f"Index {index} created")

# apply create_index 3 times to create 3 indices
def create_indices():
    for i in range(3):
        create_index(f'{INDEX_PREFIX}-00{i}')
    print("Indices created")

# create opensearch index lifecycle policy
def create_lifecycle_policy():
    with open('lifecycle_policy.json') as f:
        policy = json.load(f)
    response = requests.put(f'{OPENSEARCH_URL}/_plugins/_ism/policies/{LIFECYCLE_POLICY}', json=policy, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print("Lifecycle policy created")

# function to add one docuument to an index
def add_document(index):
    doc = {
        'timestamp': datetime.now().isoformat(),
        'message': 'Hello, world!'
    }
    response = requests.post(f'{OPENSEARCH_URL}/{index}/_doc', json=doc, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print(f"Document added to {index}")

if __name__ == '__main__':
    print("Creating index template")
    create_index_template()
    print("Creating indices")
    create_indices()
    print("Creating lifecycle policy")
    create_lifecycle_policy()