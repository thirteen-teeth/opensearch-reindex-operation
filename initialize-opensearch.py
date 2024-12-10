import requests
import json
import sys
from datetime import datetime
import random
import string
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
def create_index_template(template_file_name):
    with open(template_file_name) as f:
        template = json.load(f)
    response = requests.put(f'{OPENSEARCH_URL}/_index_template/{INDEX_TEMPLATE}', json=template, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print("Index template created")

# delete opensearch index mapping template
def delete_index_template():
    response = requests.delete(f'{OPENSEARCH_URL}/_index_template/{INDEX_TEMPLATE}', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print("Index template deleted")

# create opensearch index
def create_index(index):
    response = requests.put(f'{OPENSEARCH_URL}/{index}', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print(f"Index {index} created")

# delete opensearch index
def delete_index(index):
    response = requests.delete(f'{OPENSEARCH_URL}/{index}', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print(f"Index {index} deleted")

# apply create_index 3 times to create 3 indices
# add a 4th index with a different mapping
def create_indices():
    for i in range(3):
        create_index(f'{INDEX_PREFIX}-00{i}')
        # add 5 documents to the index
        for _ in range(5):
            add_document(f'{INDEX_PREFIX}-00{i}')
    print("Indices created")
    # create a 4th index with a different mapping
    with open('different_mapping.json') as f:
        different_mapping = json.load(f)
    # change index mapping template to different mapping
    create_index_template('different_mapping.json')
    create_index(f'{INDEX_PREFIX}-003')
    # add 10 documents to the 4th index
    for _ in range(10):
        add_document(f'{INDEX_PREFIX}-003')
# delete indices
def delete_indices():
    # apply cluster setting to allow wildcard delete
    response = requests.put(f'{OPENSEARCH_URL}/_cluster/settings', json={'persistent': {'action.destructive_requires_name': 'false'}}, auth=(USERNAME, PASSWORD), verify=False)
    delete_index(f'{INDEX_PREFIX}-*')

# create opensearch index lifecycle policy
def create_lifecycle_policy():
    with open('lifecycle_policy.json') as f:
        policy = json.load(f)
    response = requests.put(f'{OPENSEARCH_URL}/_plugins/_ism/policies/{LIFECYCLE_POLICY}', json=policy, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print("Lifecycle policy created")

# delete opensearch index lifecycle policy
def delete_lifecycle_policy():
    response = requests.delete(f'{OPENSEARCH_URL}/_plugins/_ism/policies/{LIFECYCLE_POLICY}', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print("Lifecycle policy deleted")

# function to add one document to an index
# ensure the message has some random content
def add_document(index):
    rand_str = ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))
    doc = {
        'timestamp': datetime.now().isoformat(),
        'message': rand_str
    }
    response = requests.post(f'{OPENSEARCH_URL}/{index}/_doc', json=doc, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    print(f"Document added to {index}")

if __name__ == '__main__':
    if len(sys.argv) != 2 or sys.argv[1] not in ['--create', '--delete']:
        print("Usage: python initialize-opensearch.py [--create | --delete]")
        sys.exit(1)

    action = sys.argv[1]

    if action == '--create':
        print("Creating index template")
        create_index_template('index_template.json')
        print("Creating indices")
        create_indices()
        print("Creating lifecycle policy")
        create_lifecycle_policy()
    elif action == '--delete':
        print("Deleting index template")
        delete_index_template()
        print("Deleting indices")
        delete_indices()
        print("Deleting lifecycle policy")
        delete_lifecycle_policy()