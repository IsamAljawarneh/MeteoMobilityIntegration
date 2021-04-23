# MeteoMobilityIntegration

Description of the project
--------------------------

This is the repo for a cloud-based QoS aware integration architecture for integrating meteorological and mobility data at scale\
The elements in here are:
* an automation script `download_data.sh` for downloading data from the [CAMS global reanalysis (EAC4) download web form][cams_download_page] to the local repository in **GRIB format**, extract as a csv the essential data and save the csv file.
* a [Jupyter Notebook][jp_homepage] file to interactively analyze data. \
This notebook has already pre-built functions for:
    1. ingesting the data downloaded from CAMS into a [DataFrame][df_docs]
    2. importing a [GeoJSON][geojson] polygon representing the area of interest for the analysis into a DataFrame
    3. geohashing both DataFrames with a 30-characters precision
    4. extracting the data points pertaining to the polygon with an efficient *filter-and-refine* approach

The contents of the automation script expand the [CAMS documentation page][grib2csv_convert_docs] on how to convert GRIB files to CSV by adding some cleaning steps, together with automation and customization of the API download request.

## How to choose the data to be downloaded

The main dataset chosen for this project is the [CAMS global reanalysis (EAC4)][CAMS dataset], which contains the 
> reanalysis data set of atmospheric composition (AC) produced by the Copernicus Atmosphere Monitoring Service, consisting of 3-dimensional time-consistent AC fields, including aerosols, chemical species and greenhouse gases.
The data is stored on the [Atmosphere Data Store (ADS)][ads_docs], the ECMWF infrastructure that sustains the CAMS.

For debugging purposes and as a way to explore data, the [web form][cams_download_page] can be used to choose among many variables, like dataset, time interval, geographic area.
There all the parameters can be chosen to customize the query to the user's need.
Then the API request can be shown by clicking `Show API request` or the data downloaded by clicking `Submit Form`.

This will redirect the user to the [requests page][your_requests], where the data can be downloaded by clicking hte green `Download` button to the right of the request record.

Preparation of the project
==========================
How to prepare an environment for this project
----------------------------------------------

This project needs:
* a python environment (created with [conda][conda_home] or [venv][venv_home] or other tools) with the package `cdsapi` ([ecmwf guide][cdsapi_guide] on the topic)
* the `eccodes` tool package installed ([ecmwf_guide][eccodes_guide] on the topic), since `grib_get_data` is used at some point.
The tool package can be installed in an easy way on macOS with `brew`
    > brew install eccodes
    
    or on Ubuntu following [this guide](https://gist.github.com/MHBalsmeier/a01ad4e07ecf467c90fad2ac7719844a)

Then a file in your `$HOME` folder has to be created, named `.cdsapirc` with contents inside
> url: https://ads.atmosphere.copernicus.eu/api/v2 \
> key: {uid}:{api-key}

Substitute `uid` and `api-key` with the info found in the [user page][user_page] in the "API key" section below `Terms & Conditions`

(For more in-depth guide please follow the [CDS API installation guide][cds_install_guide])


# How to run the download data automation

The only step needed is executing the `download data.sh` script with or without arguments.

> ./download_data.sh

The arguments that the script accepts are:
* variable: variable name for the dataset on ADS
* start_date: start date for the period of data downloaded from ADS
* end_date: end date for the period of data downloaded from ADS
* minimum_longitude: minimum longitude for the area of data downloaded from ADS
* maximum_longitude: maximum longitude for the area of data downloaded from ADS
* minimum_latitude: minimum latitude for the area of data downloaded from ADS
* maximum_latitude: maximum latitude for the area of data downloaded from ADS
* download_name: name of the file containing data downloaded from ADS. This will be the final file containing the data in CSV

**variable** is the dataset part of CAMS global reanalysis (EAC4) that will be downloaded. Default is `particulate_matter_10um`, which represent the reanalysis of Particulate matter d < 10 µm (PM10)\

**start_date** and **end_date** are the extremes of the period for which the data will be downloaded. Start date must be smaller (earlier) or equal than the end date. The format is `yyyy-mm-dd`. Default values used are 2020-01-01 for start date and 2020-01-31 for end date.

**minimum_longitude maximum_longitude minimum_latitude maximum_latitude** are the 4 parameters for the area for which the data will be downloaded. The longitude oscillates between (-180;180) and the latitude between (-90;90). The default values are `(minimum_longitude, maximum_longitude, minimum_latitude, maximum_latitude) = (10, 12, 43, 45)` , which is an area that encloses the city of Bologna

**download_name** is the name of the file we want to create with the final data in CSV format. The default value is `structured_data.csv`.


## Usage


From the help function of the main script
> $ ./download_data.sh -h
>
> Usage: ./download_data.sh [-v <variable>] [-s <start_date>] [-e <end_date>] [-a <min_lon>] [-b <max_lon>] [-c <min_lat>] [-d <max_lat>] [-f <downoad_file_name>] [-x]
> 
> -v <variable>: variable name for the dataset on ADS\
> -s <start_date>: start date for the period of data downloaded from ADS\
> -e <end_date>: end date for the period of data downloaded from ADS\
> -a <min_lon>: minimum longitude for the area of data downloaded from ADS\
> -b <max_lon>: maximum longitude for the area of data downloaded from ADS\
> -c <min_lat>: minimum latitude for the area of data downloaded from ADS\
> -d <max_lat>: maximum latitude for the area of data downloaded from ADS\
> -f <downoad_file_name>: name of the file containing data downloaded from ADS\
> -x <debug_mode>: if present, skips deletion of temporary files, logs the API request logs and shows the variables recognized by the script

__________________________________________________________________________________

**Author**: Denis Pereira
**Email**: denis.pereira@studio.unibo.it

[cams_download_page]: https://ads.atmosphere.copernicus.eu/cdsapp#!/dataset/cams-global-reanalysis-eac4?tab=form "Download web form"
[jp_homepage]: https://jupyter.org/ "Jupyter main page"
[df_docs]: https://spark.apache.org/docs/2.2.0/sql-programming-guide.html#datasets-and-dataframes "Spark 2.2.0 docs entry for DataFrame"
[geojson]: https://geojson.org/ "GeoJSON documentation main page"
[CAMS dataset]: https://www.ecmwf.int/en/forecasts/dataset/cams-global-reanalysis
[ads_docs]: https://confluence.ecmwf.int/display/CKB/Atmosphere+Data+Store+%28ADS%29+documentation
[your_requests]: https://ads.atmosphere.copernicus.eu/cdsapp#!/yourrequests?tab=form
[grib2csv_convert_docs]: https://confluence.ecmwf.int/display/CKB/How+to+convert+GRIB+to+CSV
[conda_home]: https://docs.conda.io/en/latest/miniconda.html
[venv_home]: https://docs.python.org/3/library/venv.html
[eccodes_guide]: https://confluence.ecmwf.int/display/ECC/ecCodes+installation
[cdsapi_guide]: https://confluence.ecmwf.int/display/CKB/How+to+download+ERA5#HowtodownloadERA5-First:InstallCDSAPIonyourmachine
[cds_install_guide]: https://ads.atmosphere.copernicus.eu/api-how-to
[user_page]: https://ads.atmosphere.copernicus.eu/user/
