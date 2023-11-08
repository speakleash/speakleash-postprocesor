import os
import glob
import json
import shutil
import logging
import argparse
from datetime import datetime
from functools import partial
from multiprocessing import Pool, set_start_method

import spacy
import pyfiglet
from tqdm import tqdm
from rich import print as rich_print
from speakleash import Speakleash
from lm_dataformat import Archive
from postprocessor.utils import log
from postprocessor.deduplicator import Deduplicator
from postprocessor.analyzer import Analyzer


class MetricsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values if values else ['stats', 'quality', 'lang', 'dedup'])


def process_doc(doc, metrics, quality, lang):
    id_doc, (doc_txt, doc_meta) = doc
    analyzer = Analyzer(doc_txt, doc_meta, nlp, id_doc, metrics, quality, lang)
    doc_meta = analyzer.go()
    return doc_txt, doc_meta, id_doc


def initialize_worker():
    global nlp
    nlp = spacy.load("pl_core_news_md", disable=('ner', 'textcat', 'entity_linker'))


def generate_sample(dataset, sample_dir, samples = None):
    if not samples:
        samples = [{"text": txt, "meta": meta} for (txt, meta) in dataset.ext_data][:5]
    with open(os.path.join(sample_dir, dataset.name + ".sample"), "w", encoding="utf-8") as f:
        json.dump(samples, f, ensure_ascii=False, indent=4)


if __name__ == '__main__':
    set_start_method("spawn")

    VERSION = "0.1.6"

    base_dir = os.path.dirname(__file__)
    output_dir = os.path.join(base_dir, "processing_output")
    replicate_to = os.path.join(base_dir, "processing_datasets")
    sample_dir = os.path.join(base_dir, "processing_samples")
    logs_dir = os.path.join(base_dir, "processing_logs")
    dedup_dir = os.path.join(base_dir, "processing_duplicates")
    TEMP_DATA = "temp_data"

    parser = argparse.ArgumentParser(
        prog="SpeakLeash post-processor",
        description="Application performing post-processing \
                    (determining metrics, generating samples, etc.) on SpeakLeash datasets"
    )

    parser.add_argument("--sample", action="store_true",
                        help="Generate a sample of the dataset")
    parser.add_argument("--metrics", nargs='*', action=MetricsAction,
                        help="Calculate metrics for the dataset [stats, quality, lang, dedup]")
    parser.add_argument("--name", type=str, nargs='+',
                        help="Name(s) of the dataset")
    parser.add_argument("--processes", type=int,
                        help="Number of processes used for metrics counting. Default = os.cpu_count() - 1")
    parser.add_argument("--update", action="store_true",
                        help="If dataset is updated (new files) - create 'update_date' in manifest")
    parser.add_argument("--dedup_out", action="store_true",
                        help="Create folder with CSV files where all duplicated documents are listed")
    parser.add_argument("--min_txt_len", type=int, default=200,
                        help="Minimum Text Length (default 200)")

    args = parser.parse_args()
    all_datasets = not args.name
    MIN_TXT_LENGTH = args.min_txt_len

    if not args.processes:
        args.processes = 1 if (os.cpu_count() - 1) < 2 else os.cpu_count() - 1

    if args.metrics:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        get_metrics = 'stats' in args.metrics
        get_quality = 'quality' in args.metrics
        get_lang = 'lang' in args.metrics
        get_duplicates = 'dedup' in args.metrics
        process_doc_partial = partial(process_doc, metrics=get_metrics, 
                                      quality=get_quality, lang=get_lang)
        maxtasksperchild = 2500 if get_metrics else 100000

    if args.sample:
        if not os.path.exists(sample_dir):
            os.makedirs(sample_dir)

    if args.dedup_out:
        if not os.path.exists(dedup_dir):
            os.makedirs(dedup_dir)

    if not os.path.exists(logs_dir):
        os.makedirs(logs_dir)

    figlet = pyfiglet.Figlet(font="slant")
    ascii_banner = figlet.renderText("SpeakLeash")
    print(ascii_banner)
    rich_print("POST-PROCESSOR [blue]v" + VERSION + "[/blue]\n")

    rich_print("Generating sample: [green]" + str(args.sample) + "[/green]")
    rich_print("Calculating metrics: [green]" + str(args.metrics) + "[/green]")
    rich_print("Update dataset -> update date in manifest: [green]" + str(args.update) + "[/green]")
    rich_print("Postprocesor will create: [green]" + str(args.processes) + " processes" + "[/green]")
    rich_print("Minimum text length: [green]" + str(MIN_TXT_LENGTH) + "[/green]")

    if args.name:
        rich_print("Dataset name: [green]" + str(args.name) + "[/green]")
        all_datasets = False
    else:
        rich_print("All datasets: [green]" + str(all_datasets) + "[/green]")

    print("")
    log("Starting post-processing", "INFO")

    sl = Speakleash(replicate_to)
    manifest = {}

    for dataset in sl.datasets:
        if all_datasets or dataset.name in args.name:
            time_now = datetime.now()
            logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s',
                                filename=os.path.join(logs_dir, dataset.name + '_' + time_now.strftime('%Y-%m-%d--%H-%M-%S') + '.log'),
                                encoding='utf-8', level=logging.DEBUG)

            logging.info("------------------------------------------------")
            logging.info(f"Starting postprocesor on dataset: {dataset.name}")

            log("Processing dataset: " + dataset.name, "INFO")

            stats = {'documents': 0}
            manifest = dataset.manifest

            file_name_zst = os.path.join(output_dir, dataset.name + '.jsonl.zst')
            file_name_manifest = os.path.join(output_dir, dataset.name + '.manifest')

            counter = 0
            samples = []

            # Start checking what need to be done
            if args.metrics:

                duplicate_indices = []
                dataset_index_max = 0

                # Get duplicates or only dataset length
                if get_duplicates:
                    duplicate_indices, dataset_index_max = Deduplicator.get_duplicates(dataset, args.dedup_out,
                                                            duplicates_file = os.path.join(dedup_dir, dataset.name + '_Non-Unique_Texts.csv'))
                else:
                    dataset_index_max = Deduplicator.get_length(dataset)

                # Get Quality dictionary
                if get_quality or manifest.get('stats',{}).get('quality',None):
                    quality_count = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0}

                # Init Archive for final dataset file
                ar = Archive(os.path.join(base_dir, TEMP_DATA))

                ds_extdata = dataset.ext_data

                with Pool(initializer = initialize_worker, processes = args.processes,
                          maxtasksperchild = maxtasksperchild) as pool:

                    for txt, meta, index in tqdm(pool.imap(func = process_doc_partial,
                                                      iterable = enumerate(ds_extdata),
                                                      chunksize = 1),
                                                total = dataset_index_max,
                                                smoothing=0.01):

                        name = meta.get("name", meta.get("url", ""))

                        # Check if document is a duplicate
                        if get_duplicates and index in duplicate_indices:
                            logging.warning(f"Removed duplicate : {name}")
                            continue

                        # Check if document has minimum length and words
                        if txt and len(txt) > MIN_TXT_LENGTH and meta['words'] > 0:

                            # Check for document language
                            if get_lang and meta['language']['lang'] != 'pl':
                                logging.warning(f"Removed non 'pl' document : {name} | meta: {meta['language']}")
                                continue

                            # Add document to final dataset
                            stats['documents'] += 1
                            for key, value in meta.items():
                                if isinstance(value, (int, float)):
                                    stats[key] = stats.get(key, 0) + value
                            ar.add_data(txt, meta=meta)

                            # Add document to corresponding quality counter
                            if get_quality or manifest.get('stats',{}).get('quality',None):
                                quality_count[meta['quality']] += 1

                            # Create samples
                            if args.sample and counter < 5:
                                samples.append({"text": txt, "meta": meta})

                            counter += 1
                        else:
                            logging.warning(f"Removed empty document : {name}")

                pool.close()
                pool.join()
                ar.commit()

                if get_duplicates:
                    log("Found and removed " + str(len(duplicate_indices))+" duplicates", "WARNING")
                    logging.warning(f"Found and removed {len(duplicate_indices)} duplicates")

                log(f"Logs can be found in the 'logs' folder", "INFO")

                log(f"Dataset before: {dataset_index_max} docs -> now: {stats['documents']} docs", "INFO")
                logging.info(f"Dataset before: {dataset_index_max} docs -> now: {stats['documents']} docs")

                for key, value in stats.items():
                    if key in Analyzer.AVG_METRICS_DEF:
                        stats[key] = round(value / stats['documents'], 4)

                for key in Analyzer.OBSOLETE_KEYS:
                    stats.pop(key, None)

                if get_quality or manifest.get('stats',{}).get('quality',None):
                    stats['quality'] = {
                        'HIGH': round(quality_count['HIGH'] / stats['documents'], 2),
                        'MEDIUM': round(quality_count['MEDIUM'] / stats['documents'], 2),
                        'LOW': round(quality_count['LOW'] / stats['documents'], 2)
                    }

                log(f"Adding last details in the manifest and clearing cache files...", "INFO")
                logging.info("Adding last details in the manifest and clearing cache files...")

                ar = None
                data_files = glob.glob(os.path.join(base_dir, TEMP_DATA, '*'))
                file_size = 0

                for f in data_files:
                    if f.endswith('.zst'):
                        shutil.copy(f, file_name_zst)
                        file_size = os.path.getsize(file_name_zst)
                        os.remove(f)

                current_timestamp = time_now.strftime('%Y-%m-%d %H:%M:%S')
                if 'creation_date' not in manifest:
                    manifest['creation_date'] = current_timestamp
                elif args.update:
                    manifest['updated_date'] = current_timestamp

                manifest['stats'] = stats
                manifest['file_size'] = file_size

                with open(file_name_manifest, 'w', encoding='utf-8') as mf:
                    json.dump(manifest, mf, indent=4)

            if args.sample:
                generate_sample(dataset, sample_dir, samples)

            if os.path.exists(os.path.join(replicate_to, dataset.name + '.jsonl.zst')):
                os.remove(os.path.join(replicate_to, dataset.name + '.jsonl.zst'))

            if os.path.exists(os.path.join(replicate_to, dataset.name + '.manifest')):
                os.remove(os.path.join(replicate_to, dataset.name + '.manifest'))

            if os.path.exists(TEMP_DATA):
                shutil.rmtree(TEMP_DATA)

            log(f"Finished processing dataset: {dataset.name}", "INFO")
            logging.info(f"Finished processing dataset: {dataset.name}")
            logging.info("++++++++++++++++++++++++++++++++++++++++++++++++")

    log("Finished post-processing\n", "INFO")
