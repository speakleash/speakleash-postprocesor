# SpeakLeash Post-Processor

SpeakLeash post-processor is a Python script that provides various functionalities for processing datasets. The script can be executed using the `main.py` file and supports several command-line arguments to customize its behavior.

## Installation

To install the required packages for the SpeakLeash post-processor script, you can use the following command:

```
python -m pip install -r requirements.txt
```

This command installs the packages listed in the `requirements.txt` file.

### Using Virtual Environments

It is highly recommended to use virtual environments when working with Python projects. Virtual environments allow you to manage dependencies for each project separately, ensuring that different projects do not interfere with each other.


## Usage

To run the SpeakLeash post-processor script, use the following command:

```
python main.py [arguments]
```

The script accepts the following command-line arguments:

### `--sample`

- Use this argument to generate a sample of the dataset.
- This argument does not require a value.
- Example usage: `python main.py --sample`

### `--metrics`

- Use this argument to calculate metrics for the dataset.
- This argument accepts an additional values [metrics quality]. If additional values are specified only selected metrics are counted.
- This argument does not require a value. In this case all available metrics are counted.
- Example usage: `python main.py --metrics metrics quality`

### `--name`

- Use this argument to specify the name of the dataset.
- This argument requires a string value representing the name of the dataset or list of strings separated by space
- If this argument is omitted, the script will generate samples or calculate metrics for all available datasets.
- Example usage: `python main.py --name my_dataset1 my_dataset2`

### `--processes`

- Use this argument to specify the number of parallel processes used for calculating metric.
- This argument requires an integer value representing the number of required processes to spawn.
- If this argument is omitted, the script will use the value returned by os.cpu_count()
- Please not: the total system memory in GB / expected processes number should exceed 2GB. Otherwise it may cause system to run out of RAM while processing multiple large documents.
- Example usage: `python main.py --processes 8`

## Examples

To generate a sample of a dataset named "example_dataset", use the following command:

```
python main.py --sample --name example_dataset
```

To calculate metrics for a dataset named "example_dataset", use the following command:

```
python main.py --metrics --name example_dataset
```

## Additional Information

For more information about the SpeakLeash post-processor script and its functionalities, please refer to the source code and comments within the `main.py` file.
