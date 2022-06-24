# %%
from typing import Iterator
from itertools import islice
import pandas as pd
import json
from tqdm import tqdm
import time

from src.connection import Neo4jConnection

def batched(iterable, batch_size):
    iterator = iter(iterable)
    while batch := list(islice(iterator, batch_size)):
        yield batch

def insert_data(conn: Neo4jConnection, query: str, record_iterator: Iterator[dict], batch_size=100, total=None) -> dict:
    """Run query on batched records from an iterator."""
    total_in = 0
    total_out = 0
    batch = 0
    res = None
    start = time.time()
    
    num_batches = total // batch_size + 1 if total is not None else None
    print("Start query...")
    for i, batch in tqdm(enumerate(batched(record_iterator, batch_size)), total=num_batches):
        total_in += len(batch)
        res = conn.query(query,
                         parameters = {'rows': batch})
        try:
            total_out += res[0]['total'] # TODO: use or remove
        except Exception:
            total_out -=1
        
    print("Total_time:              ", time.time()-start)
    print("Total input:             ", total_in)
    print("Total out (if supported): ", total_out)
    
    return res

# %% [markdown]
# # Load stackexchange XML


# %%
import re
import xml.etree.ElementTree as ET
from typing import Generator, Iterator, List


def iter_parse_stackexchange(board: str, table: str, max_num=None) -> Generator[dict, None, None]:
    """_summary_

    Args:
        board (str): Stackexchange board to load. Expected file structure is
            ./data/`board`/*.xml

    Returns:
        list[pd.DataFrame]: List of pandas dataframes with Comments and
            PostHistory data with extracted DOIs and markdown links.
    """
    # Regex patterns
    DOI_PATTERN = "10\.\d{4,9}/[-._;\(\)/:A-Z0-9]+[/A-Z0-9]"
    MD_PATTERN = "\[([\w\s\d]+)\](https?:\/\/[\w\d./?=#]+)"
    print(f" --- {board}/{table} loaded:")
    # Load xml file
    path = f"data/{board}/{table}.xml"
    root = ET.iterparse(path)
    for i, (event, child) in enumerate(root):
        if max_num is not None and i >= max_num:
            return
        if child.tag =='row':
            record = child.attrib
            if "Body" in record: # for posts
                record["DOIs"] =  re.findall(DOI_PATTERN,record["Body"])
            elif "Text" in record:  # for comments
                record["DOIs"] = re.findall(DOI_PATTERN,record["Text"])
                if len(record["DOIs"]) == 0:
                    continue
            yield record

        # Extract all DOIs using pattern defined before to list per row
            # df = df.loc[df["DOIs"].str.len() > 2]


# %% INTEGRATE TODO:
# process_set("Comments"), process_set("PostHistory"), process_set("PostLinks")
    #     if verbose > 0:
    #         print(f" --- {board}/{set} loaded:")
    #         print(df.head())
    #         print(df.shape)
    #     return df

    # return process_set("Comments"), process_set("Posts"), process_set("PostLinks")
