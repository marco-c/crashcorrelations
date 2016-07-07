# crash-correlations
> Tool to find deviations between crash groups.

## Example usage

Create a config.ini file with the following contents:
```ini
[Socorro]
token=YOUR_TOKEN_HERE
```

To run the CLI tool locally (useful to find correlations in crashes with a particular signature):
```bash
$PATH_TO_SPARK/bin/spark-submit --master local[*] cli.py [SIGNATURE]
```
