# Tiny ETL


Tiny ETL is a python program that extracts data from local files, transforms it to unified form and loads it to datawarehouse (local sqlite3 database). 

New data for product wwc or hb can be added to directory `data` in a directory tree based on product name and extract dates. Refer e.g. `data/wwc/2018/06/28/hb.json` and `data/hb/2018/06/28/wwc.csv` for formats. Extract supports csv and (lines of) json. 

In transform phase, analytically interesting fields are selected and transformed to unified forms. IP address to country name transformation for source `hb` uses web service https://ipapi.co. As free version of the service limits the number of requests to 1000/day fetched IP address-country mappings are stored to a local sqlite database `db/ip_cache.db` that is queried before requesting the web service. Cache contains mapping for provided example data. 

Nationality to country name transformation uses a dataset available at https://datahub.io/core/country-list. Data set is stored locally in `res/iso2country.json` and loaded in memory when the script is run.

To scale the ETL for larger volume and velocity, one would need to source the data required in IP address to country transformation and store it to local database and apply an efficient indexing and caching of IP address ranges.

# Instuctions

Requirements:
- Python 3 (3.6.5 or newer is recommended)
- Internet connection

To run the ETL for both data sources:
```
cd tiny-etl
python etl.py hb 2018-06-28
python etl.py wwc 2018-06-28
```

When etl.py has been run for both data sources, analytical test queries 
can be run: 
```
python test/test_queries.py
```

# Unit tests

Unit tests are written with the standard unittest module.

In directory tiny-etl, command:
```
python -m unittest discover -v
```
runs the tests located in directory test

Code coverage can be analyzed with e.g. coverage utility.
To install coverage from python package index:
```
pip install coverage
```
Run the coverage analysis:
```
coverage run -m unittest discover
```
Print the report:
```
coverage report -m
```