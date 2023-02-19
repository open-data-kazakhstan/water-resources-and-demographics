# Water Resources and Demographics Republic of Kazakhstan

## Data

Data is in xlsx format and synced with upstream source yearly. It is sourced from https://stat.gov.kz/.

**Note:** Since data.egov.kz requested for a signed key for authorization, we just downloaded and put in directory *archive*. It is temporary solution

We have processed the source data to make it normalized and derived from it several aggregated datasets:

* `data/water-basins-kz.csv` - water basins description.
* `data/water-basins-lakes.csv` - lakes description.
  * `lakes size` in Square, km²
  * *Note* `Itemman, Tontyger` lakes size in average value
* `data/water-basins-rivers.csv` - rivers description.
  * `river_length` in kilometers
* `data/water-basins-water-comsumpsion.csv` - water basins consumption 
  * Average annual water consumption in m3/s
  * Rivers length in km
  * River fall in m
  * Water and energy resources power in thousand kW
  * Water and energy resources energy in million kWh/year

We have also added some metadata such as column descriptions and [data packaged it][dp].

[dp]: https://frictionlessdata.io/data-package/

## Preparation

[![Python 3.6](https://img.shields.io/badge/python-3.6-blue.svg)](https://www.python.org/downloads/release/python-360/)
![.github/workflows/actions.yml](https://github.com/open-data-kazakhstan/decent_work_indicators/actions/workflows/actions.yml/badge.svg?branch=master)

This repository uses [dataflows](https://github.com/datahq/dataflows) to process and normalize the data.

You first need to install the dependencies:

```
pip install -r scripts/requirements.txt
```

Then run the script

```
python scripts/process.py
```

## License

This dataset is licensed under the Open Data Commons [Public Domain and Dedication License][pddl].

[pddl]: https://www.opendatacommons.org/licenses/pddl/1-0/