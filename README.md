# Tiny ETL


Tiny ETL is a python script that extracts source data, transforms it to unified form and loads it to analytical database. Source data sets are located in: data/wwc/2018/06/28/*.json and data/hb/2018/06/28/*.csv 

New data for game wwc or hb can be added to the directory tree based on extract dates.

In addition, web service https://ipapi.co is used for IP address to country name transformation. Used free version of service limits the number of requests to 1000/day. Tiny ETL stores fetched IP addresses to local sqlite database (db/ip_cache.db) that is queried before requesting the web service. Dabase contains mapping for provided source 
data. In general, running the script (and unit tests) requires Internet 
connection.

For nationality to country name transformation, a dataset available at https://datahub.io/core/country-list is used. Data set is stored locally in res/iso2country.json and loaded in memory when the script is run.

To scale the ETL for larger volume and velocity, one would need to source the data required in IP address to country transformation and store it to local database and apply an efficient indexing and caching of IP address ranges. Computation could be parallized with e.g. Apache Spark that provides abstraction layer on top of physical computation instances.

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