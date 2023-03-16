from math import ceil
from typing import List, Generator

from sonic import IngestClient, SearchClient

from archivebox.logging_util import ProgressBar
from archivebox.util import enforce_types
from archivebox.config import SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD, SONIC_BUCKET, SONIC_COLLECTION

MAX_SONIC_TEXT_TOTAL_LENGTH = 1000000     # dont index more than 1 million characters per text
MAX_SONIC_ERRORS_BEFORE_ABORT = 5

@enforce_types
def index(snapshot_id: str, texts: List[str]):
    error_count = 0
    with IngestClient(SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD) as ingestcl:
        for text in texts:
            text_len = min(len(text), MAX_SONIC_TEXT_TOTAL_LENGTH)
            try:
                ingestcl.push_chunked(SONIC_COLLECTION, SONIC_BUCKET, snapshot_id, text[:text_len])
            except Exception as err:
                print(f'\n[!] Sonic search backend threw an error while indexing: {err.__class__.__name__} {err}')
                error_count += 1
                if error_count > MAX_SONIC_ERRORS_BEFORE_ABORT:
                    raise

@enforce_types
def search(text: str) -> List[str]:
    with SearchClient(SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD) as querycl:
        snap_ids = querycl.query(SONIC_COLLECTION, SONIC_BUCKET, text)
    return snap_ids

@enforce_types
def flush(snapshot_ids: Generator[str, None, None]):
    with IngestClient(SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD) as ingestcl:
        for id in snapshot_ids:
            ingestcl.flush_object(SONIC_COLLECTION, SONIC_BUCKET, str(id))
