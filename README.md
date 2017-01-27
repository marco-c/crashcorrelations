# crash-correlations
> Tool to find deviations between crash groups.

## Example usage

Install required Python packages:
```sh
sudo pip install -r requirements.txt
```

Create a config.ini file with the following contents:
```ini
[Socorro]
token=YOUR_TOKEN_HERE
```

To run the CLI tool locally (useful to find correlations in crashes with a particular signature):
```sh
$PATH_TO_SPARK/bin/spark-submit --master local[*] cli-signature.py [SIGNATURES] (-c [CHANNEL] or -v [VERSIONS]) -p [PRODUCT]
```

To run notebooks:
```sh
PYSPARK_DRIVER_PYTHON=ipython PYSPARK_DRIVER_PYTHON_OPTS="notebook" $PATH_TO_SPARK/bin/pyspark
```
