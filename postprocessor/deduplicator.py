"""
Deduplicator Module

This module provides the Deduplicator class, an entity responsible for
checking dataset content - duplications and length.

Classes:
- Deduplicator: Got functions for finding duplicates and length of dataset.

Dependencies:
- pandas: Provides dataframe and algorithms for finding duplicated documents.
- tqdm: Provides formatted progress bar.
- postprocessor.utils: Provides 'log' function (based on 'rich' library) for formatted logs.
"""
import pandas as pd
from tqdm import tqdm
from postprocessor.utils import log

class Deduplicator:
    """
    Represents the Deduplicator class, an entity responsible for
    checking dataset content - duplications and length.
    """

    @staticmethod
    def get_duplicates(dataset_obj, dedup_out_flag: bool = False, duplicates_file: str = 'duplicates.csv') -> tuple[set, int]:
        """
        Generates a list (set) of documents indexes that are duplicates and qualify for deletion.
        The function compares texts based on their hash representation in SHA256 space.

        :param dataset_obj: SpeakleashDataset object (dataset).

        :return: A tuple containing a set of documents indexes that are duplicates,
                and total number of documents in dataset (int).
        """
        log("Gathering documents data...", "INFO")

        import hashlib

        frame = pd.DataFrame(
            [
                {
                    "text": hashlib.sha256(txt.encode("utf-8")).hexdigest(),
                    "characters": len(txt),
                    "url": meta.get("url", meta.get("name", "-")),
                }
                for txt, meta in tqdm(dataset_obj.ext_data)
            ]
        )

        log("Getting duplicated documents...", "INFO")
        frame["is_duplicated"] = frame.duplicated(subset=["text"])

        if dedup_out_flag:
            frame["non_unique"] = frame.duplicated(subset=["text"], keep=False)
            kappa = (frame[frame["non_unique"] == True].groupby("characters").apply(pd.DataFrame))
            kappa.to_csv(
                duplicates_file,
                sep="\t",
                header=True,
                index=True,
                encoding="utf-8",
            )

        dup_list = set(frame[frame["is_duplicated"] == True].index)
        index_max = frame.index[-1] + 1
        log(f"Duplicated docs: {len(dup_list)}", "INFO")

        return dup_list, index_max

    @staticmethod
    def get_length(dataset_obj) -> int:
        """
        Retrieves total number of documents in dataset (int).

        :param dataset_obj: SpeakleashDataset object (dataset).

        :return: Total number of documents in dataset (int).
        """
        log("Gathering documents data...", "INFO")
        index_max = 0

        for x in tqdm(dataset_obj.ext_data):
            index_max += 1

        log(f"Documents in [{dataset_obj.name}] dataset: {index_max}", "INFO")
        return index_max
