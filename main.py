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

if __name__ == '__main__':

    VERSION = "0.1.0"

    base_dir = os.path.join(os.path.dirname(__file__))
    manifest_dir = os.path.join(base_dir, "manifests")
    replicate_to = os.path.join(base_dir, "datasets")
    sample_dir = os.path.join(base_dir, "samples")

    parser = argparse.ArgumentParser(
        prog="SpeakLeash post-proceessor",
        description="Application performing post-processing (determining metrics, generating samples, etc.) on SpeakLeash datasets",
    )

    parser.add_argument("--sample", action="store_true", help="Generate sample of dataset")
    parser.add_argument("--metrics", action="store_true", help="Calculate metrics for dataset")
    parser.add_argument("--name", type=str, help="Name of dataset")

    args = parser.parse_args()

    all = True

    #Set defaults
    #args.sample = True
    #args.metrics = False
    #args.name = ""

    if args.metrics:
        if not os.path.exists(manifest_dir):
            os.makedirs(manifest_dir)

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
    nlp = spacy.load("pl_core_news_md", disable=('ner','lemmatizer','textcat','entity_linker', 'attribute_ruler'))
    manifest = {}
    sl = Speakleash(replicate_to)
    for d in sl.datasets:
        if all or d.name == args.name:
            log("Processing dataset: " + d.name, "INFO")

            total_len = 0
            total_docs = 0
            total_sentences = 0
            total_words = 0
            total_verbs = 0
            total_nouns = 0
            total_punctuations = 0
            total_symbols = 0
            total_stopwords = 0
            total_oovs = 0
            total_adjecives = 0
            total_adverbs = 0

            manifest = d.manifest

            ar = Archive(os.path.join(base_dir, "data"))
            file_name_zst = os.path.join(base_dir, d.name + '.zst')
            file_name_manifest = os.path.join(base_dir, d.name + '.manifest') 

            counter = 0
            sample = []
            ds =d.ext_data

            for doc in ds:
                txt, meta = doc
                
                if args.metrics:
                    analyzer = Analyzer(txt, meta, nlp, counter)
                    meta = analyzer.go()

                    total_words += meta['words']
                    total_verbs += meta['verbs']
                    total_nouns += meta['nouns']
                    total_len += meta['length']
                    total_docs += 1
                    total_sentences += meta['sentences']
                    total_punctuations += meta['punctuations']
                    total_symbols += meta['symbols']
                    total_stopwords += meta['stopwords']
                    total_oovs += meta['oovs']   
                    total_adjecives += meta['adjecives']
                    total_adverbs += meta['adverbs'] 

                    ar.add_data(txt, meta = meta)                

                counter += 1

                if args.metrics:
                    ar.commit()
                    
                if args.sample:
                    if counter <= 5:
                        sample.append({"text": txt, "meta": meta})
                
                    if counter == 5:
                        with open(os.path.join(base_dir, sample_dir, d.name + ".sample"), "w") as f:
                            f.write(json.dumps(sample, ensure_ascii = False ,  indent=4))
    
            if args.metrics:
                ar = None
                data_files= glob.glob(os.path.join('data','/*'))
                file_size = 0
                
                for f in data_files:
                    if f.endswith('.zst'):
                        shutil.copy(f, os.path.join(file_name_zst))
                        file_size = os.path.getsize(file_name_zst)
                        os.remove(f)

                stats = {"documents": total_docs, "sentences": total_sentences, "words" : total_words, "nouns" : total_nouns, "verbs" : total_verbs, "characters": total_len, "punctuations" : total_punctuations, "symbols" : total_symbols, 'stopwords': total_stopwords,  'oovs': total_oovs, 'adjecives': total_adjecives, 'adverbs': total_adverbs}
                manifest['stats'] = stats
                manifest['size'] = file_size
            
                json_manifest = json.dumps(manifest, indent = 4) 
                with open(os. file_name_manifest, 'w') as mf:
                    mf.write(json_manifest)
            
            if os.path.exists(os.path.join(replicate_to, d.name + '.jsonl.zst')):
                os.remove(os.path.join(replicate_to, d.name + '.jsonl.zst'))

            if os.path.exists(os.path.join(replicate_to, d.name + '.manifest')):
                os.remove(os.path.join(replicate_to, d.name + '.manifest'))
 
            if os.path.exists(os.path.join('data')):
                shutil.rmtree(os.path.join('data'))

    log("Finished post-processing", "INFO")
    print("")
