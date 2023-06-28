import argparse
import pyfiglet
from speakleash import Speakleash
import os
import json
from postprocessor.analyzer import Analyzer
from lm_dataformat import Archive
import glob
import shutil
import spacy
from rich import print as rich_print
from common.functions import log
from multiprocessing import Pool, set_start_method
from functools import partial

#Define helper class to parse --metrics argument.
#If argument is not specified return empty list, if argument is specified without values use default values (all metrics)

class MetricsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if values:
            setattr(namespace, self.dest, values)
        else:
            setattr(namespace, self.dest, ['stats', 'quality'])


def process_doc(doc, metrics, quality):
    counter = doc[0]
    txt, meta = doc[1]
    analyzer = Analyzer(txt, meta, nlp, counter, metrics, quality)
    meta = analyzer.go()
    return txt, meta

def initialize_worker():

    print('Initializing worker...')   

    global nlp
    nlp = spacy.load("pl_core_news_md", disable=('ner','textcat','entity_linker'))



if __name__ == '__main__':

    set_start_method("spawn")

    VERSION = "0.1.3"

    base_dir = os.path.join(os.path.dirname(__file__))
    output_dir = os.path.join(base_dir, "output")
    manifest_dir = os.path.join(base_dir, "manifests")
    replicate_to = os.path.join(base_dir, "datasets")
    sample_dir = os.path.join(base_dir, "samples")

    parser = argparse.ArgumentParser(
        prog="SpeakLeash post-proceessor",
        description="Application performing post-processing (determining metrics, generating samples, etc.) on SpeakLeash datasets",
    )

    parser.add_argument("--sample", action="store_true", help="Generate sample of dataset")
    parser.add_argument("--metrics", nargs='*', action=MetricsAction, help="Calculate metrics for dataset [stats, quality]")
    parser.add_argument("--name", type=str, nargs='+', help="Name(s) of dataset")
    parser.add_argument("--processes", type=int, help="Number of prcocesses used for metrics counting. Default = os.cpu_count()")

    args = parser.parse_args()

    all = not args.name

    if not args.processes:
        args.processes=os.cpu_count()
    
    if args.metrics:
        if not os.path.exists(manifest_dir):
            os.makedirs(manifest_dir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        get_metrics = 'stats' in args.metrics
        get_quality = 'quality' in args.metrics
        process_doc_partial = partial(process_doc, metrics=get_metrics, quality=get_quality)
        if get_metrics:
            maxtasksperchild=2000
        else:
            maxtasksperchild=100000

    if args.sample:
        if not os.path.exists(sample_dir):
            os.makedirs(sample_dir)

    figlet = pyfiglet.Figlet(font="slant")
    ascii_banner = figlet.renderText("SpeakLeash")
    print(ascii_banner)
    rich_print("POST-PROCESSOR [blue]v" + VERSION + "[/blue]\n") 

    rich_print("Generating sample: [green]" + str(args.sample) + "[/green]")
    rich_print("Calculating metrics: [green]" + str(args.metrics) + "[/green]")

    if args.name:
        rich_print("Dataset name: [green]" + str(args.name) + "[/green]")
        all = False
    else:
        rich_print("All datasets: [green]" + str(all) + "[/green]")

    print("")
    log("Starting post-processing", "INFO")
    
    manifest = {}
    sl = Speakleash(replicate_to)
    for d in sl.datasets:
        if all or d.name in args.name:
            log("Processing dataset: " + d.name, "INFO")

            stats = {'documents': 0}

            manifest = d.manifest

            
            file_name_zst = os.path.join(base_dir, output_dir,  d.name + '.jsonl.zst')
            file_name_manifest = os.path.join(base_dir, output_dir, d.name + '.manifest') 

            counter = 0
            sample = []
            ds = d.ext_data

            if args.metrics:

                if get_quality:
                    quality_low_count = 0
                    quality_med_count = 0
                    quality_high_count = 0

                ar = Archive(os.path.join(base_dir, "data"))
                with Pool(initializer=initialize_worker, processes=args.processes, maxtasksperchild=maxtasksperchild) as pool:
                    for txt, meta in pool.imap(func=process_doc_partial, iterable=enumerate(ds), chunksize=1):                         
                        
                        #Handling empty document removal                       
                        if txt and len(txt)>200 and meta['words']>0:
                            stats['documents'] += 1                        
                            
                            
                            for key in meta.keys():
                                if isinstance(meta[key], (int, float)):
                                    stats[key] = stats.setdefault(key, 0) + meta[key]
                            ar.add_data(txt, meta = meta)                

                            

                            if args.sample:
                                if counter < 5:
                                    sample.append({"text": txt, "meta": meta})
                    
                                if counter == 4:
                                    with open(os.path.join(base_dir, sample_dir, d.name + ".sample"), "w", encoding = "utf-8") as f:
                                        f.write(json.dumps(sample, ensure_ascii = False ,  indent=4))

                            counter += 1

                            if get_quality:
                                if meta['quality'] == "LOW":
                                    quality_low_count += 1
                                elif meta['quality'] == "HIGH":
                                    quality_high_count += 1
                                else:
                                    quality_med_count += 1

                        else:
                            name = meta.get("name", "")
                            if name == "":
                                name = meta.get("url", "")[:80]
                            log("Removed empty document : " + name, "WARNING")



                    pool.close()
                    pool.join()
                    ar.commit()

                
                for key in stats.keys():
                    if key in Analyzer.AVG_METRICS_DEF:
                        stats[key] = round(stats[key]/stats['documents'],4)
                
                #Remove obsolete keys from manifest stats if present
                for key in Analyzer.OBSOLETE_KEYS:
                    stats.pop(key,None)
                    
                if get_quality:
                    stats['quality'] = {'HIGH' : round(quality_high_count/stats['documents'],2), 'MEDIUM': round(quality_med_count/stats['documents'],2), 'LOW': round(quality_low_count/stats['documents'],2)}
                else:
                    log("Required metrics for quality check not found in manifest", "WARNING")           

                    
    
                
                ar = None
                data_files= glob.glob(os.path.join(base_dir,'data','*'))
                file_size = 0

                for f in data_files:
                    if f.endswith('.zst'):
                        shutil.copy(f, file_name_zst)
                        file_size = os.path.getsize(file_name_zst)
                        os.remove(f)

                manifest['stats'] = stats    
                manifest['file_size'] = file_size
            
                json_manifest = json.dumps(manifest, indent = 4) 
                with open(file_name_manifest, 'w') as mf:
                    mf.write(json_manifest)

            if not args.metrics and args.sample:
                
            
                for i in range(5):                    
                    txt, meta = next(ds)
                    sample.append({"text": txt, "meta": meta})
                                        
                    
                with open(os.path.join(base_dir, sample_dir, d.name + ".sample"), "w", encoding="utf-8") as f:
                    f.write(json.dumps(sample, ensure_ascii = False,  indent=4))
                
                #Release dataset object to allow delete
                ds = None

            
        if os.path.exists(os.path.join(replicate_to, d.name + '.jsonl.zst')):
            os.remove(os.path.join(replicate_to, d.name + '.jsonl.zst'))

        if os.path.exists(os.path.join(replicate_to, d.name + '.manifest')):
            os.remove(os.path.join(replicate_to, d.name + '.manifest'))

        if os.path.exists(os.path.join('data')):
            shutil.rmtree(os.path.join('data'))

    log("Finished post-processing", "INFO")
    print("")
