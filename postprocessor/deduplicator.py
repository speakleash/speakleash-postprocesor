# This class takes dataset and returns indices of documents to be
# removed (duplicated copies) using clustering upon text length and comparing text to find exact duplicates
import pandas as pd
import progressbar
from common.functions import log

class Deduplicator:
    def __init__(self, ds):
        self.df = self._get_data(ds)
        mask = (
            self.df['characters'].duplicated()  # most probably 1:1 duplicates have the same number of characters
        )
        self.df = self.df[mask]  
        self.chunks = self._get_chunks(self.df)  # grouping text content into chunks of the same length
        self.dup_list = (self._get_duplicates(self.chunks))  # final result

    @staticmethod
    def _get_data(ds):
        log("Gathering documents data...", "INFO")      
        
        # Using a list comprehension to create the data for dataframe
        data = [{'text': txt, 'characters': meta.get('characters', meta.get('length', len(txt)))} for txt, meta in ds]
        
        # Directly creating the dataframe from the data
        frame = pd.DataFrame(data)
        return frame

    @staticmethod
    def _get_chunks(df):  # function grouping text content into chunks of the same length
        log("Chunking documents...", "INFO") 
        chunks = [group for _, group in df.groupby('characters')]
        return chunks

    @staticmethod
    def _get_duplicates(chunks):
        dup_list = []
        log("Getting duplicated documents...", "INFO") 
        with progressbar.ProgressBar(max_value=len(chunks)) as bar:  # monitoring the progress
           for i, chunk in zip(range(len(chunks)), chunks):  # iterating through each chunk
               # sorting - to eliminate sets of the same values easily
                # removing the first entry, because we need to keep one entry for each (multi-)duplicated value
                dup_list.extend(sorted(chunk[chunk.duplicated(subset='text')].index)[1:])
                bar.update(i)
        return set(dup_list)
    
