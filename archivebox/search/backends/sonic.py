from math import ceil
from typing import List, Generator

from sonic import IngestClient, SearchClient

from archivebox.logging_util import ProgressBar
from archivebox.util import enforce_types
from archivebox.config import SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD, SONIC_BUCKET, SONIC_COLLECTION

MAX_SONIC_TEXT_TOTAL_LENGTH = 100000000     # dont index more than 100 million characters per text
# Overhead: 'PUSH ' + SONIC_BUCKET + ' ' + SONIC_COLLECTION + ' ' + Snapshot.id + ' "' + ...data... + '"\n'
SONIC_PUSH_PROTOCOL_OVERHEAD = 5 + len(SONIC_BUCKET) + 1 + len(SONIC_COLLECTION) + 1 + 36 + 1 + 3
MAX_SONIC_ERRORS_BEFORE_ABORT = 5

@enforce_types
def index(snapshot_id: str, texts: List[str]):
    error_count = 0
    with IngestClient(SEARCH_BACKEND_HOST_NAME, SEARCH_BACKEND_PORT, SEARCH_BACKEND_PASSWORD) as ingestcl:
        sonic_bufsize = ingestcl.bufsize
        max_sonic_chunk_length = round((sonic_bufsize - SONIC_PUSH_PROTOCOL_OVERHEAD) * 0.9)
        sonic_bufsize_remaining = sonic_bufsize - max_sonic_chunk_length
        for text in texts:
            text_len = min(len(text), MAX_SONIC_TEXT_TOTAL_LENGTH)
            chunks = (
                text[i:i+max_sonic_chunk_length]
                for i in range(
                    0,
                    text_len,
                    max_sonic_chunk_length,
                )
            )
            num_chunks = ceil(text_len / max_sonic_chunk_length)
            progress = ProgressBar(num_chunks, prefix='Chunks ')
            try:
                for idx, chunk in enumerate(chunks):
                    progress.update(idx + 1)
                    # Sonic protocol escapes quotes with backslashes, doubling the number
                    # of bytes required. If this exceeds the overhead available in the buffer,
                    # then split the chunk and submit it as two chunks.
                    num_quotes = chunk.count('"')
                    chunk_remainder = None
                    if num_quotes > sonic_bufsize_remaining:
                        chunk_remainder = chunk[-num_quotes:]
                        chunk = chunk[:-num_quotes]
                    ingestcl.push(SONIC_COLLECTION, SONIC_BUCKET, snapshot_id, str(chunk))
                    if chunk_remainder is not None:
                        ingestcl.push(SONIC_COLLECTION, SONIC_BUCKET, snapshot_id, str(chunk_remainder))
                progress.end()
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
