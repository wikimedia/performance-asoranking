# ASORanking

Generates a ranking of ASOs (Autonomous Systems Organizations), based on normalized RUM (Real user measurement) metrics from web browsers during sampled on Wikipedia page views.

For more information, refer to [AS Report](https://wikitech.wikimedia.org/wiki/Performance/AS_Report) documentation on wikitech.wikimedia.org.

## Usage

This script is meant to be run on a stat machine on cron on the 1st of every month, using the following syntax:

`python asoranking.py --publish`

Which will generate the ranking for the previous calendar month in the form
of a tsv file published to `/srv/published-datasets/performance/autonomoussystems/`

## Output

* https://performance.wikimedia.org/asreport/
* https://analytics.wikimedia.org/published/datasets/performance/autonomoussystems/
