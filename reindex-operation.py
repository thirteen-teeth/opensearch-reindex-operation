import argparse
import requests
import json
import time
from datetime import datetime
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# Configuration
OPENSEARCH_URL = 'https://localhost:9200'
INDEX_PATTERN = 'my-index-*'
REINDEX_SUFFIX = 'v2'
STATE_FILE = 'reindex_state.json'
# ISM_POLICY_ID = 'my-ism-policy'
USERNAME = 'admin'
PASSWORD = 'admin'

# check if the state file exists, create an empty state file if it does not exist
def load_state():
    try:
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        with open(STATE_FILE, 'w') as f:
            json.dump({}, f)
        return {}

# based on the index pattern, find a list of indices that match the pattern
# get the mapping of the most recently created index and check the index mapping of the older indices
# return a list of index names that do not match the mapping of the most recent index
def create_state():
    print("Creating state...")
    indices = get_indices(INDEX_PATTERN)
    # print(f"Indices: {indices}")
    # print(f"Indices type: {type(indices)}")
    if not indices:
        print("No indices found.")
        return
    indices = json.loads(indices[0])
    indices = sorted(indices, key=lambda x: x['creation.date'])
    
    latest_index = indices[-1]
    print(f"Latest index: {latest_index['index']}")

    latest_mapping = get_mapping(latest_index['index'])
    latest_mapping = latest_mapping[latest_index['index']]['mappings']
    print(f"Latest mapping: {latest_mapping}")

    # loop through the older indices and compare the mapping
    state = {}
    for index in indices[:-1]:
        # print(f"Checking mapping for {index['index']}")
        mapping = get_mapping(index['index'])
        # print(f"Mapping: {mapping}")
        # print(f"mapping[index['index']]['mappings']: {mapping[index['index']]['mappings']}")
        # print(f"latest_mapping['mappings']: {latest_mapping}")
        if mapping[index['index']]['mappings'] != latest_mapping:
            print(f"Mapping for {index['index']} does not match the latest index {latest_index['index']}.")
            state[index['index']] = {'mapping': mapping}
        else:
            print(f"Mapping for {index['index']} matches the latest index {latest_index['index']}.")

    return state

# save the state to the state file
def save_state(state):
    with open(STATE_FILE, 'w') as f:
        # pretty print the state
        json.dump(state, f, indent=4)

# check the status of the reindex task
def check_reindex_status(task_id):
    response = requests.get(f'{OPENSEARCH_URL}/_tasks/{task_id}', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    # pretty print response
    print(json.dumps(response.json(), indent=4))
    return response.json()

# start the reindex operation
def start_reindex(source_index, target_index):
    payload = {
        "source": {"index": source_index},
        "dest": {"index": target_index}
    }
    response = requests.post(f'{OPENSEARCH_URL}/_reindex?wait_for_completion=false', json=payload, auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    return response.json()['task']

# get mapping of an index
def get_mapping(index):
    response = requests.get(f'{OPENSEARCH_URL}/{index}/_mapping', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    return response.json()

def get_indices(pattern):
    # get a list of indices that match the pattern, in JSON format with index name and creation date
    response = requests.get(f'{OPENSEARCH_URL}/_cat/indices/{pattern}?format=json&h=index,creation.date', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    return [line.strip() for line in response.text.splitlines()]

def get_doc_count(index):
    # perform a _refresh to ensure the document count is accurate
    response = requests.post(f'{OPENSEARCH_URL}/{index}/_refresh', auth=(USERNAME, PASSWORD), verify=False)
    response = requests.get(f'{OPENSEARCH_URL}/{index}/_count', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    return response.json()['count']

def main(dry_run):
    state = load_state()
    print(f"State: {state}")
    if not state:
        state = create_state()
    else:
        print("State already exists")
    # pretty print the state
    # print(json.dumps(state, indent=4))
    print(f"State: {state}")
    if not dry_run:
        save_state(state)

    for index, mapping in state.items():
        print(f"Reindexing {index} due to mapping changes.")
        if dry_run:
            print(f"Would start reindex task for {index} to {index}-{REINDEX_SUFFIX}")
        else:
            task = start_reindex(index, f"{index}-{REINDEX_SUFFIX}")
            print(f"Reindex task started: {task}")
            state[index] = {'task': task}            
            save_state(state)

    if not dry_run:
        # check the status of the reindex task every 5 seconds
        while state:
            for index in list(state.keys()):
                task_id = state[index]['task']
                status = check_reindex_status(task_id)
                if status['completed']:
                    print(f"Reindex task {task_id} completed.")
                    del state[index]
                else:
                    print(f"Reindex task {task_id} still running.")
            save_state(state)
            time.sleep(5)
        print("All reindex tasks completed.")
    else:
        print("Dry run completed. No changes were made.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reindex operation script.')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making any changes.')
    args = parser.parse_args()
    main(args.dry_run)