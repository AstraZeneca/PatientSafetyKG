#
#   Manage custom MedDRA query data
#
#   Custom queries build sets of specific PTs for use in reporting and visualizations.  The MeddraCq node type
#   holds meta data (such as a name and description), and edges capture links between MeddraCq nodes and specific
#   PTs.  Thes utility functions read custom query definitions from an auxillary BitBucket project and create
#   files suitable for loading into the graph
#

import logging
import pandas as pd
from pathlib import Path

from pandas.core.algorithms import isin

## In-line definitions (meta-data and data)

SOURCE_FILE_COLUMN = "FileName"

## Meta-data columns (used to create MeddraCq nodes)
META_COLUMNS = [
    "Name",
    "Abbreviation",
    "Description",
    "Authors",
    "CreatedDate",
    "Source",
]

## Data columns (used to link MeddraCq nodes to PTs)
DATA_COLUMNS = ["Name", "PT"]

logger = logging.getLogger("pskg_loader.meddracq")


def read_raw_meta_data(input_folder):
    """
    Build a unified meta dataframe and from
    given local file sources, and return as a data frame

    Parameters
    ----------
    input_folder: str
        Path to data files, files with a ".tsv" suffix will be loaded

    Returns
    -------
    pd.Dataframe
        Returns a dataframe of custom query definitions
    """

    if isinstance(input_folder, str):
        input_folder = Path(input_folder)

    logger.info(f"Scanning {input_folder.resolve()}")

    meta_dfs = []

    for f in input_folder.rglob("*.tsv"):
        if f.name.endswith("_meta.tsv"):
            logger.info(f"Reading meta file {f.name}")
            meta_dfs.append(
                pd.read_csv(f, sep="\t").assign(**{SOURCE_FILE_COLUMN: f.resolve()})
            )

    # Save meta data file
    meta_df = pd.concat(meta_dfs)
    if len(meta_df[meta_df.duplicated(subset=["Name"])]):
        raise ValueError(
            "Duplicate meta data detected:"
            + str(
                meta_df[meta_df.duplicated(subset=["Name"])][
                    [SOURCE_FILE_COLUMN, "Name"]
                ]
            )
        )

    # Return concise data frames
    return meta_df[META_COLUMNS]


def read_raw_links(input_folder):
    """
    Build a dataframe with links from Meddra custom queries to PTS from
    given local file sources and return it

    Parameters
    ----------
    input_folder: str
        Path to data files, files with a ".tsv" suffix will be loaded

    Returns
    -------
    pd.Dataframe
        Returns dataframe linking custom query names to PTs
    """

    if isinstance(input_folder, str):
        input_folder = Path(input_folder)

    logger.info(f"Scanning {input_folder}")

    data_dfs = []

    for f in input_folder.rglob("*.tsv"):
        if f.name.endswith("_meta.tsv"):
            pass
        else:
            logger.info(f"Reading data file {f.name}")
            data_dfs.append(pd.read_csv(f, sep="\t"))

    # Return concise data frames
    return pd.concat(data_dfs)[DATA_COLUMNS]
