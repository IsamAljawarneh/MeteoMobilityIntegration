#!/usr/bin/env bash

PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-v <variable>] [-s <start_date>] [-e <end_date>] [-a <min_lon>] [-b <max_lon>] [-c <min_lat>] [-d <max_lat>] [-f <downoad_file_name>]

-v <variable>: variable name for the dataset on ADS
-s <start_date>: start date for the period of data downloaded from ADS
-e <end_date>: end date for the period of data downloaded from ADS
-a <min_lon>: minimum longitude for the area of data downloaded from ADS
-b <max_lon>: maximum longitude for the area of data downloaded from ADS
-c <min_lat>: minimum latitude for the area of data downloaded from ADS
-d <max_lat>: maximum latitude for the area of data downloaded from ADS
-f <downoad_file_name>: name of the file containing data downloaded from ADS

EOF
  exit 1
}
variable='particulate_matter_10um'
start_date='2020-01-01'
end_date='2020-01-31'
min_lon='-180'
max_lon='180'
min_lat='-90'
max_lat='90'
downoad_file_name='download.grib'

dir=default_dir file=default_file verbose_level=0
while getopts v:s:e:a:b:c:d:f: o; do
  case $o in
    (v) variable=$OPTARG;;
    (s) start_date=$OPTARG;;
    (e) end_date=$OPTARG;;
    (a) min_lon=$OPTARG;;
    (b) max_lon=$OPTARG;;
    (c) min_lat=$OPTARG;;
    (d) max_lat=$OPTARG;;
    (f) downoad_file_name=$OPTARG;;
    (*) usage
  esac
done
shift "$((OPTIND - 1))"

echo "variable: $variable"
echo "start_date: $start_date"
echo "end_date: $end_date"
echo "min_lon: $min_lon"
echo "max_lon: $max_lon"
echo "min_lat: $min_lat"
echo "max_lat: $max_lat"
echo "downoad_file_name: $downoad_file_name"



echo "python3 api_request_template.py $variable $start_date $end_date $min_lon $max_lon $min_lat $max_lat $downoad_file_name"
