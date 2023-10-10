from pathlib import Path
from typing import List, Generator

import meilisearch
from meilisearch.errors import MeiliSearchError, MeiliSearchApiError, MeiliSearchTimeoutError

from archivebox.util import enforce_types
from archivebox.config import (
    SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD,
    MEILISEARCH_INDEX, MEILISEARCH_URI_SCHEME,
    OUTPUT_DIR, TIMEOUT, stderr, write_config_file
)

MEILISEARCH_HOST_URI = '{}://{}:{}'.format(
    MEILISEARCH_URI_SCHEME,
    SEARCH_BACKEND_HOST_NAME,
    SEARCH_BACKEND_PORT
)

MEILISEARCH_RESULTS_LIMIT = 1000


@enforce_types
def _connect_index() -> meilisearch.index.Index:
    client = meilisearch.Client(MEILISEARCH_HOST_URI, SEARCH_BACKEND_PASSWORD)
    return client.index(MEILISEARCH_INDEX)


@enforce_types
def setup(out_dir: Path=OUTPUT_DIR):
    client = meilisearch.Client(MEILISEARCH_HOST_URI, SEARCH_BACKEND_PASSWORD)
    try:
        # If get_keys succeeds, we are using the master key
        client.get_keys()
        is_master_key = True
    except MeiliSearchApiError:
        # get_keys failed, so see if we are using a properly-scoped API key
        is_master_key = False

    if is_master_key:
        stderr(
            '\n    [!] SEARCH_BACKEND_PASSWORD is set to the MeiliSearch master key;'
            '\n    [!] do you want ArchiveBox to create a safer, scoped API key for itself?',
            color='lightyellow'
        )
        try:
            assert input('    y/[n]: ').lower() == 'y'
        except (KeyboardInterrupt, EOFError, AssertionError):
            return

        try:
            create_result = client.create_key({
                'name': 'archivebox',
                'description': 'ArchiveBox key for adding, deleting, and searching documents',
                'indexes': [MEILISEARCH_INDEX],
                'actions': ['search', 'indexes.create', 'documents.add', 'documents.delete', 'tasks.get'],
                'expiresAt': None,
            })
        except MeiliSearchError as e:
            stderr(f'[X] Failed to create a new API key for ArchiveBox: {e}', color='red')
            raise SystemExit(1)

        api_key = create_result.key
        stderr(
            '\n    Created the new, scoped "archivebox" API key. Setting'
            '\n    SEARCH_BACKEND_PASSWORD to the new key...'
        )
        write_config_file({'SEARCH_BACKEND_PASSWORD': api_key}, out_dir)

    else:
        try:
            failed_actions = ['documents.add', 'indexes.create', 'tasks.get']
            index = client.index(MEILISEARCH_INDEX)
            create_task = index.add_documents(
                [{
                    'snapshot_id': 'SETUP',
                    'texts': ['SETUP'],
                }],
                primary_key='snapshot_id'
            )
            client.wait_for_task(create_task.task_uid, timeout_in_ms=TIMEOUT*1000)

            failed_actions = ['search']
            search('SETUP')

            failed_actions = ['documents.delete']
            flush(['SETUP'])
        except MeiliSearchApiError as e:
            stderr(f'[X] MeiliSearch API key is missing permitted actions: '
                   f'"{failed_actions!r}" ({e})', color='red')
            raise SystemExit(1)
        except MeiliSearchTimeoutError as e:
            stderr(f'[X] MeiliSearch timed out during the following actions: '
                   f'"{failed_actions!r}" ({e})', color='red')
            raise SystemExit(1)


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
            'limit': MEILISEARCH_RESULTS_LIMIT,
            'attributesToRetrieve': ['snapshot_id'],
        }
    )
    snapshot_ids = [hit['snapshot_id'] for hit in result.get('hits', [])]
    return snapshot_ids


@enforce_types
def flush(snapshot_ids: Generator[str, None, None]):
    index = _connect_index()
    index.delete_documents(snapshot_ids)
