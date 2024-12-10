import requests
import json
import time
from datetime import datetime

# Configuration
OPENSEARCH_URL = 'http://localhost:9200'
INDEX_PATTERN = 'my-index-*'
NEW_INDEX = 'my-index-latest'
STATE_FILE = 'reindex_state.json'
# ISM_POLICY_ID = 'my-ism-policy'
USERNAME = 'admin'
PASSWORD = 'admin'

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
    indices = get_indices(INDEX_PATTERN)
    if not indices:
        print("No indices found.")
        return

    latest_index = max(indices)
    print(f"Latest index: {latest_index}")

    state = {}
    for index in indices:
        if index != latest_index:
            mapping_latest = get_mapping(latest_index)
            mapping_index = get_mapping(index)
            if mapping_latest != mapping_index:
                state[index] = mapping_index

    return state

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def check_reindex_status(task_id):
    response = requests.get(f'{OPENSEARCH_URL}/_tasks/{task_id}', auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return response.json()

def start_reindex(source_index, target_index):
    payload = {
        "source": {"index": source_index},
        "dest": {"index": target_index}
    }
    response = requests.post(f'{OPENSEARCH_URL}/_reindex?wait_for_completion=false', json=payload, auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return response.json()['task']

def get_indices(pattern):
    response = requests.get(f'{OPENSEARCH_URL}/_cat/indices/{pattern}?h=index', auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return [line.strip() for line in response.text.splitlines()]

def get_doc_count(index):
    response = requests.get(f'{OPENSEARCH_URL}/{index}/_count', auth=(USERNAME, PASSWORD))
    response.raise_for_status()
    return response.json()['count']

# def apply_ism_policy(index, policy_id):
#     payload = {"policy_id": policy_id}
#     response = requests.post(f'{OPENSEARCH_URL}/{index}/_opendistro/_ism/add', json=payload, auth=(USERNAME, PASSWORD))
#     response.raise_for_status()

def main():
    state = load_state()

    # add a check if the state is empty, 

    if 'task_id' in state:
        task_id = state['task_id']
        status = check_reindex_status(task_id)
        if status['completed']:
            print(f"Reindex task {task_id} completed.")
            del state['task_id']
            save_state(state)
        else:
            print(f"Reindex task {task_id} is still running.")
            return

    indices = get_indices(INDEX_PATTERN)
    if not indices:
        print("No indices found.")
        return

    latest_index = max(indices)
    doc_count = get_doc_count(latest_index)
    print(f"Latest index: {latest_index}, Document count: {doc_count}")

    task_id = start_reindex(latest_index, NEW_INDEX)
    state['task_id'] = task_id
    save_state(state)
    print(f"Started reindexing from {latest_index} to {NEW_INDEX}, task ID: {task_id}")

    # apply_ism_policy(NEW_INDEX, ISM_POLICY_ID)
    # print(f"Applied ISM policy {ISM_POLICY_ID} to {NEW_INDEX}")

if __name__ == "__main__":
    main()