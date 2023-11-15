# SpeakLeash Post-Processor

SpeakLeash post-processor is a Python script that provides various functionalities for processing datasets. The script can be executed using the `main.py` file and supports several command-line arguments to customize its behavior.

## Installation

To install the required packages for the SpeakLeash post-processor script, you can use the following command:

```console
$ python -m pip install -r requirements.txt
```

This command installs the packages listed in the `requirements.txt` file.

### Using Virtual Environments

It is highly recommended to use virtual environments when working with Python projects. Virtual environments allow you to manage dependencies for each project separately, ensuring that different projects do not interfere with each other.

```console
$ python -m venv .env
```
Remember to activate virtual environment ;)


## Usage

To run the SpeakLeash post-processor script, use the following command:

```console
$ python main.py [arguments]
```

> **Note**
> The first run of the post-processor may result in an error related to the "lid.176.bin" language model associated with the ftlangdetect / fasttext library. After restarting the postprocessor, the error should not show up.

The script accepts the following command-line arguments:

### `--name`

- Use this argument to specify the name of the dataset - available datasets are listed on the dashboard: https://speakleash.org/dashboard/
- This argument requires a string value representing the name of the dataset or list of strings separated by space
- If this argument is omitted, the script will generate samples or calculate metrics for all available datasets.
- Example usage: `python main.py --name my_dataset1 my_dataset2`

### `--sample`

- Use this argument to generate a sample of the dataset.
- This argument does not require a value.
- Example usage: `python main.py --sample`

### `--metrics`

- Use this argument to calculate all available metrics for the dataset.
- This argument accepts an additional values [stats quality lang dedup]. If additional values are specified only selected metrics are counted.
- Example usage: `python main.py --metrics stats`
- Example usage: `python main.py --metrics stats quality lang dedup` (the same as `python main.py --metrics` )

### `--processes`

- Use this argument to specify the number of parallel processes used for calculating metric.
- This argument requires an integer value representing the number of required processes to spawn.
- If this argument is omitted, the script will use the value returned by: os.cpu_count() - 1
- Example usage: `python main.py --processes 8`

> **Warning**
> Each process can use even 2 GBs of RAM. Don't set too many processes at once, or they might use more RAM than you have - computer can freeze. Remember, the number of processes should be less than the number of logical CPU cores.

### `--update`

- Use this argument when new documents are added to old package, e.g., wikipedia page has been re-scraped.
- This will add a new field in the manifest called 'updated_date' - date of last dataset update.
- This argument does not require a value.
- Example usage: `python main.py --name my_dataset1 --metrics --update`

### `--dedup_out`

- Argument only for debug - create folder with CSV files where all duplicated documents are listed.
- Duplicates are listed with: length of text, hash representation of text and URLs or names.
- This argument does not require a value.
- Example usage: `python main.py --name my_dataset1 --dedup_out`

## Examples

For new datasets, it is worth doing processing for every aspect - through logs or duplicates files, we can get a lot of information about the dataset and discover possible issues:

```console
$ python main.py --name example_dataset --sample --metrics --dedup_out
```
> **Warning**
> Remember about `--processes` argument -> 2 GBs per process, e.g. you got 16GB of RAM it is better to use maximum `--processes 6` to keep some memory for operating system and other processes.

To generate a sample of a dataset named "example_dataset", use the following command:

```console
$ python main.py --sample --name example_dataset
```

To calculate metrics for a dataset named "example_dataset", use the following command:

```console
$ python main.py --name example_dataset --metrics
```

## Additional Information

For more information about the SpeakLeash post-processor script and its functionalities, please refer to the source code and comments within the `main.py` file.
