import os
from lm_dataformat import Archive, Reader
import shutil
import json
import glob
import spacy
from multiprocessing import set_start_method

from multiprocessing.pool import Pool
import time


INPUT_FOLDER = './input'
OUTPUT_FOLDER = './output'
EXPECTED_MANIFEST_VERSION = '2.0'


def update_meta(txt, meta):
    if not txt:
        return meta

    nouns = 0
    verbs = 0
    symbols = 0
    stopwords = 0
    punctations = 0
    words = 0
    oovs = 0
    
    doc = nlp(txt)

    for token in doc:
        if not token.is_punct and not token.is_stop and not token.is_space:
            if token.is_oov and not token.pos_ == "SYM":
                oovs +=1
            if token.pos_ == "NOUN":
                nouns += 1
            elif token.pos_ == "VERB":
                verbs += 1
        if token.pos_ == "SYM":
            symbols += 1
        if token.is_stop:
            stopwords += 1
        if token.is_punct:
            punctations += 1
        elif not token.is_space and not token.pos_ == "SYM":
            words += 1
            
    meta['words'] = words
    meta['nouns'] = nouns
    meta['verbs'] = verbs
    meta['punctuations'] = punctations
    meta['symbols'] = symbols
    meta['stopwords'] = stopwords
    meta['oovs'] = oovs 

    return meta
    
   
def process_item(document):

    text = document[0]
    meta = document[1]
  
    l = len(text.strip())
    if l > 100000:
        nlp.max_length = len(text) + 100
    
    meta = update_meta(text, meta)

    return meta, text


def initialize_worker():

    print('Initializing worker...')   

    #Each worker node needs to have its own resources.
    global nlp

    #Disabling some unused model features speeds things up to 20%
    nlp = spacy.load("pl_core_news_md", disable=('ner','lemmatizer','textcat','entity_linker'))
      


if __name__ == '__main__':
    
    
    
    start_time = time.time()

    set_start_method("spawn")


    zst_files= glob.glob(INPUT_FOLDER+'/*.zst')

    if len(zst_files) == 0:
        print("No zst files found in {0}".format(INPUT_FOLDER))


    for file in zst_files:
        
        
        
        print("Processing dataset: "+file)

        ar = Archive('./data')

        total_words = 0
        total_verbs = 0
        total_nouns = 0
        total_punctuations = 0
        total_symbols = 0
        total_stopwords = 0
        total_oovs = 0

        if os.path.exists(file.replace('jsonl.zst','manifest')):
        
            with open(file.replace('jsonl.zst','manifest')) as manifest_file:
                file_manifest = manifest_file.read()


            manifest = json.loads(file_manifest)

            if not 'version' in manifest.keys() or manifest['version'] != EXPECTED_MANIFEST_VERSION:

                rdr = Reader(file)

                with Pool(initializer=initialize_worker, processes=os.cpu_count(), maxtasksperchild=1000) as pool:
                
                        for meta, doc in pool.imap(func=process_item, iterable=rdr.stream_data(get_meta=True), chunksize=os.cpu_count()):
                        
                            total_words += meta['words']
                            total_verbs += meta['verbs']
                            total_nouns += meta['nouns']
                            total_punctuations += meta['punctuations']
                            total_symbols += meta['symbols']
                            total_stopwords += meta['stopwords']
                            total_oovs += meta['oovs']
                            
                            

                            ar.add_data(doc, meta = meta)

                        # Close the process pool
                        pool.close()
                        # Wait for all tasks to complete
                        pool.join()
                        
                ar.commit()
            
                #update manifest
                manifest['version'] = EXPECTED_MANIFEST_VERSION
                manifest['stats']['words'] = total_words
                manifest['stats']['verbs'] = total_verbs
                manifest['stats']['nouns'] = total_nouns
                manifest['stats']['punctuations'] = total_punctuations
                manifest['stats']['symbols'] = total_symbols
                manifest['stats']['stopwords'] = total_stopwords
                manifest['stats']['oovs'] = total_oovs

                data_files= glob.glob('./data/*')
                
                file_size = 0
                
                ar = None

                if not os.path.exists(OUTPUT_FOLDER):
                    os.makedirs(OUTPUT_FOLDER)

                for f in data_files:
                    if f.endswith('.zst'):
                        shutil.copy(f, os.path.join(OUTPUT_FOLDER,os.path.basename(file)))
                        file_size = os.path.getsize(f)
                    os.remove(f)

                manifest["file_size"] = file_size

                json_manifest = json.dumps(manifest, indent = 4) 

                with open(os.path.join(OUTPUT_FOLDER,os.path.basename(file.replace('jsonl.zst','manifest'))), 'w') as mf:
                    mf.write(json_manifest)
            else:
                print('Dataset {0} manifest version match. Skipping.'.format(file))
        else:
            print('No matching manifest for dataset: {0}'.format(file))

    print(time.time()-start_time)