import argparse
import requests
import json
import time
from datetime import datetime
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

parser = argparse.ArgumentParser(description='Reindex operation script.')
parser.add_argument('--opensearch-url', required=True, help='The URL of the Opensearch cluster.')
parser.add_argument('--index-pattern', required=True, help='The pattern of the indices to reindex.')
parser.add_argument('--reindex-suffix', required=True, help='The suffix to append to the reindexed indices.')
parser.add_argument('--state-file', required=True, help='The file to save the state of the reindex operation.')
parser.add_argument('--username', required=True, help='The username to use for authentication.')
parser.add_argument('--password', required=True, help='The password to use for authentication.')
parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without making any changes.')
args = parser.parse_args()

OPENSEARCH_URL = args.opensearch_url
INDEX_PATTERN = args.index_pattern
REINDEX_SUFFIX = args.reindex_suffix
STATE_FILE = args.state_file
USERNAME = args.username
PASSWORD = args.password
DRY_RUN = args.dry_run

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
    print(f'{OPENSEARCH_URL}/_tasks/{task_id}')
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
    # return an empty object if no indices are found
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    return [line.strip() for line in response.text.splitlines()]

def get_doc_count(index):
    # perform a _refresh to ensure the document count is accurate
    response = requests.post(f'{OPENSEARCH_URL}/{index}/_refresh', auth=(USERNAME, PASSWORD), verify=False)
    response = requests.get(f'{OPENSEARCH_URL}/{index}/_count', auth=(USERNAME, PASSWORD), verify=False)
    response.raise_for_status()
    return response.json()['count']

def main():
    state = load_state()
    if not state:
        state = create_state()
    else:
        print("State already exists")

    if not DRY_RUN:
        save_state(state)

    # Find the first index that needs reindexing
    while state:
        index_to_reindex = next(iter(state), None)

        if index_to_reindex:
            # check if the new target index already exists
            target_index = f"{index_to_reindex}-{REINDEX_SUFFIX}"
            if get_indices(target_index):
                print(f"Target index: {target_index} already exists.")
                print("Please check or possibly delete the target index and rerun the script.")
                break
            print(f"Reindexing {index_to_reindex} due to mapping changes.")
            if DRY_RUN:
                print(f"Would start reindex task for {index_to_reindex} to {target_index}")
                del state[index_to_reindex]
            else:
                task = start_reindex(index_to_reindex, f"{target_index}")
                print(f"Reindex task started: {task}")
                state[index_to_reindex] = {'task': task}
                save_state(state)

                # Check the status of the reindex task every 5 seconds
                while True:
                    status = check_reindex_status(task)
                    print(f"Status: {status}")
                    print(type(status))
                    print(dir(status))
                    if status['completed'] == True:
                        print(f"Reindex task {task} completed.")
                        del state[index_to_reindex]
                        save_state(state)
                        break
                    # check if task has failures
                    elif 'failures' in status['response']:
                        print(f"Reindex task {task} failed.")
                        del state[index_to_reindex]
                        save_state(state)
                        break
                    else:
                        print(f"Reindex task {task} still running.")
                    time.sleep(5)

        if DRY_RUN:
            print("Dry run completed. No changes were made.")
            break

    if not state:
        print("All reindex tasks completed.")
    else:
        print("Some reindex tasks are still pending. Please check the state and rerun the script if necessary.")

if __name__ == "__main__":
    main()