"""Module for inserting documents into Elasticsearch."""
import pandas as pd
import urllib3
from dateutil import parser
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

urllib3.disable_warnings()

df = pd.read_csv("csv/collection.csv")

df["date"] = df["date"].apply(parser.parse)

df = df.astype(
    {
        "id": "int",
        "body": "string",
        "title": "string",
        "date": "datetime64[ns, UTC]",
        "court": "string",
        "click_context": "string",
        "copy_context": "string",
        "expanded_copy_context": "string",
    }
)

es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "changeme"),
    verify_certs=False,
    ssl_show_warn=False,
)

documents = df.to_dict(orient="records")


def doc_generator(data):
    """Generator function for creating documents."""
    for _, document in enumerate(data):
        yield {"_index": "documents", "_id": document["id"], "_source": document}


print("Inserting documents into Elasticsearch...")

success, failed = bulk(es, doc_generator(documents))

print(f"Successfully inserted: {success} documents")
print(f"Failed to insert: {failed} documents")
