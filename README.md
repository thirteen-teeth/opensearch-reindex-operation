```
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

```bash
python3 initialize-opensearch.py --create
python3 initialize-opensearch.py --delete


export OS_USERNAME=admin
export OS_PASSWORD=admin

python3 reindex-operation.py --opensearch-url https://localhost:9200 --index-pattern 'my-index-*' --reindex-suffix v2 --state-file reindex_state.json --username $OS_USERNAME --password $OS_PASSWORD --dry-run
python3 reindex-operation.py --opensearch-url https://localhost:9200 --index-pattern 'my-index-*' --reindex-suffix v2 --state-file reindex_state.json --username $OS_USERNAME --password $OS_PASSWORD
```