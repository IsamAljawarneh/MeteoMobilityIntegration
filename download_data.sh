#!/usr/bin/env bash

PROGNAME=$0

usage() {
  cat << EOF >&2
Usage: $PROGNAME [-v <variable>] [-s <start_date>] [-e <end_date>] [-a <min_lon>] [-b <max_lon>] [-c <min_lat>] [-d <max_lat>] [-f <downoad_file_name>] [-x]

-v <variable>: variable name for the dataset on ADS
-s <start_date>: start date for the period of data downloaded from ADS
-e <end_date>: end date for the period of data downloaded from ADS
-a <min_lon>: minimum longitude for the area of data downloaded from ADS
-b <max_lon>: maximum longitude for the area of data downloaded from ADS
-c <min_lat>: minimum latitude for the area of data downloaded from ADS
-d <max_lat>: maximum latitude for the area of data downloaded from ADS
-f <downoad_file_name>: name of the final file containing data downloaded from ADS in CSV format
-x <debug_mode>: if present, skips deletion of temporary files, logs the API request logs and shows the variables recognized by the script

EOF
  exit 1
}

REQUEST_LOG_FILENAME='request.log'
GRIB_DATA_FILENAME='download.grib'
GRIB_STRUCTURED_FILENAME='csv_grib.csv'
STRUCTURED_CSV_FILENAME='structured_data.csv'
GRIB_COLUMNS='dataDate,time,shortName'

variable='particulate_matter_10um'
start_date='2020-01-01'
end_date='2020-01-31'
min_lon='10'
max_lon='12'
min_lat='43'
max_lat='45'
downoad_file_name='cams_data.csv'
debug_mode=false

dir=default_dir file=default_file verbose_level=0
while getopts v:s:e:a:b:c:d:f:x o; do
  case $o in
    (v) variable=$OPTARG;;
    (s) start_date=$OPTARG;;
    (e) end_date=$OPTARG;;
    (a) min_lon=$OPTARG;;
    (b) max_lon=$OPTARG;;
    (c) min_lat=$OPTARG;;
    (d) max_lat=$OPTARG;;
    (f) downoad_file_name=$OPTARG;;
    (x) debug_mode=true;;
    (*) usage
  esac
done

shift "$((OPTIND - 1))"

if [ "$debug_mode" == true ];
then
	echo "variable: $variable"
	echo "start_date: $start_date"
	echo "end_date: $end_date"
	echo "min_lon: $min_lon"
	echo "max_lon: $max_lon"
	echo "min_lat: $min_lat"
	echo "max_lat: $max_lat"
	echo "downoad_file_name: $downoad_file_name"
fi




if [ "$debug_mode" == true ];
then
	echo "=============================================================="
	echo "API REQUEST LOGS:"
	python3 utils/api_request_template.py "$variable" "$start_date" "$end_date" "$min_lon" "$max_lon" "$min_lat" "$max_lat" "$GRIB_DATA_FILENAME" "$debug_mode" &> "$REQUEST_LOG_FILENAME"
	cat "$REQUEST_LOG_FILENAME"
	echo "=============================================================="
else
	python3 utils/api_request_template.py "$variable" "$start_date" "$end_date" "$min_lon" "$max_lon" "$min_lat" "$max_lat" "$GRIB_DATA_FILENAME" "$debug_mode" &> /dev/null
fi

grib_get_data -p "$GRIB_COLUMNS" "$GRIB_DATA_FILENAME" | sed -e 's/,//g' > "$GRIB_STRUCTURED_FILENAME"

head -n 1 "$GRIB_STRUCTURED_FILENAME" > "$STRUCTURED_CSV_FILENAME"
sed -e "s/$(cat "$STRUCTURED_CSV_FILENAME")//" "$GRIB_STRUCTURED_FILENAME" | sed -e '/^$/d' >> "$STRUCTURED_CSV_FILENAME"

python3 utils/grib2csv.py -i "$STRUCTURED_CSV_FILENAME" -o data/"$downoad_file_name" -d "$debug_mode"

echo "The data has been saved at data/$downoad_file_name"
if [ "$debug_mode" == false ]; 
then
	rm "$GRIB_DATA_FILENAME" "$GRIB_STRUCTURED_FILENAME" "$STRUCTURED_CSV_FILENAME"
	rm "$REQUEST_LOG_FILENAME" || true
fi
