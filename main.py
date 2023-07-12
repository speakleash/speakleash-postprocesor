import argparse
import json
import os
import shutil
import spacy
import glob
import pyfiglet
from multiprocessing import Pool, set_start_method
from functools import partial
from rich import print as rich_print
from speakleash import Speakleash
from postprocessor.analyzer import Analyzer
from lm_dataformat import Archive
from common.functions import log


class MetricsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, values if values else ['stats', 'quality', 'lang'])


def process_doc(doc, metrics, quality, lang):
    counter, (txt, meta) = doc
    analyzer = Analyzer(txt, meta, nlp, counter, metrics, quality, lang)
    meta = analyzer.go()
    return txt, meta


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

    VERSION = "0.1.4"

    base_dir = os.path.dirname(__file__)
    output_dir = os.path.join(base_dir, "output")
    manifest_dir = os.path.join(base_dir, "manifests")
    replicate_to = os.path.join(base_dir, "datasets")
    sample_dir = os.path.join(base_dir, "samples")

    parser = argparse.ArgumentParser(
        prog="SpeakLeash post-processor",
        description="Application performing post-processing (determining metrics, generating samples, etc.) on SpeakLeash datasets",
    )

    parser.add_argument("--sample", action="store_true", help="Generate a sample of the dataset")
    parser.add_argument("--metrics", nargs='*', action=MetricsAction,
                        help="Calculate metrics for the dataset [stats, quality, lang]")
    parser.add_argument("--name", type=str, nargs='+', help="Name(s) of the dataset")
    parser.add_argument("--processes", type=int,
                        help="Number of processes used for metrics counting. Default = os.cpu_count()")

    args = parser.parse_args()
    all_datasets = not args.name

    if not args.processes:
        args.processes = os.cpu_count()

    if args.metrics:
        if not os.path.exists(manifest_dir):
            os.makedirs(manifest_dir)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        get_metrics = 'stats' in args.metrics
        get_quality = 'quality' in args.metrics
        get_lang = 'lang' in args.metrics
        process_doc_partial = partial(process_doc, metrics=get_metrics, quality=get_quality, lang=get_lang)
        maxtasksperchild = 2000 if get_metrics else 100000

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
        all_datasets = False
    else:
        rich_print("All datasets: [green]" + str(all_datasets) + "[/green]")

    print("")
    log("Starting post-processing", "INFO")

    manifest = {}
    sl = Speakleash(replicate_to)
    for dataset in sl.datasets:
        if all_datasets or dataset.name in args.name:
            log("Processing dataset: " + dataset.name, "INFO")

            stats = {'documents': 0}
            manifest = dataset.manifest

            file_name_zst = os.path.join(output_dir, dataset.name + '.jsonl.zst')
            file_name_manifest = os.path.join(output_dir, dataset.name + '.manifest')

            counter = 0
            samples = []
            ds = dataset.ext_data

            if args.metrics:
                if get_quality or manifest.get('stats',{}).get('quality',None):
                    quality_count = {'LOW': 0, 'MEDIUM': 0, 'HIGH': 0}

                ar = Archive(os.path.join(base_dir, "data"))
                with Pool(initializer=initialize_worker, processes=args.processes,
                          maxtasksperchild=maxtasksperchild) as pool:
                    for txt, meta in pool.imap(func=process_doc_partial, iterable=enumerate(ds),
                                                         chunksize=1):
                        if txt and len(txt) > 200 and meta['words'] > 0:
                                                        
                            if get_lang and not meta['language']['lang'] == 'pl':
                                try:
                                    name = meta.get("name", "") or meta.get("url", "")[:80]
                                    log("Removed non 'pl' document : " + name, "WARNING")
                                except:
                                    pass
                                continue

                            stats['documents'] += 1
                            for key, value in meta.items():
                                if isinstance(value, (int, float)):
                                    stats[key] = stats.get(key, 0) + value
                            ar.add_data(txt, meta=meta)

                            if get_quality or manifest.get('stats',{}).get('quality',None):
                                quality_count[meta['quality']] += 1

                            if args.sample and counter < 5:
                                samples.append({"text": txt, "meta": meta})

                            counter += 1
                        else:
                            name = meta.get("name", "") or meta.get("url", "")[:80]
                            log("Removed empty document : " + name, "WARNING")

                pool.close()
                pool.join()
                ar.commit()

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

                ar = None
                data_files = glob.glob(os.path.join(base_dir, 'data', '*'))
                file_size = 0

                for f in data_files:
                    if f.endswith('.zst'):
                        shutil.copy(f, file_name_zst)
                        file_size = os.path.getsize(file_name_zst)
                        os.remove(f)

                manifest['stats'] = stats
                manifest['file_size'] = file_size

                with open(file_name_manifest, 'w') as mf:
                    json.dump(manifest, mf, indent=4)

            if args.sample:
                generate_sample(dataset, sample_dir, samples)

            if os.path.exists(os.path.join(replicate_to, dataset.name + '.jsonl.zst')):
                os.remove(os.path.join(replicate_to, dataset.name + '.jsonl.zst'))

            if os.path.exists(os.path.join(replicate_to, dataset.name + '.manifest')):
                os.remove(os.path.join(replicate_to, dataset.name + '.manifest'))

            if os.path.exists('data'):
                shutil.rmtree('data')

    log("Finished post-processing", "INFO")
    print("")
