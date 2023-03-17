from typing import List, Generator

import meilisearch

from archivebox.util import enforce_types
from archivebox.config import (
    SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD,
    MEILISEARCH_INDEX, MEILISEARCH_URI_SCHEME
)

MEILISEARCH_HOST_URI = '{}://{}:{}'.format(
    MEILISEARCH_URI_SCHEME,
    SEARCH_BACKEND_HOST_NAME,
    SEARCH_BACKEND_PORT
)


@enforce_types
def _connect_index() -> meilisearch.index.Index:
    client = meilisearch.Client(MEILISEARCH_HOST_URI, SEARCH_BACKEND_PASSWORD)
    return client.index(MEILISEARCH_INDEX)


@enforce_types
def index(snapshot_id: str, texts: List[str]):
    index = _connect_index()
    index.add_documents(
        [{
            'snapshot_id': snapshot_id,
            'texts': texts,
        }],
        primary_key='snapshot_id'
    )


@enforce_types
def search(text: str) -> List[str]:
    index = _connect_index()
    result = index.search(
        text,
        {
            'attributesToRetrieve': ['snapshot_id'],
        }
    )
    snapshot_ids = [hit['snapshot_id'] for hit in result.get('hits', [])]
    return snapshot_ids


@enforce_types
def flush(snapshot_ids: Generator[str, None, None]):
    index = _connect_index()
    index.delete_documents(snapshot_ids)
