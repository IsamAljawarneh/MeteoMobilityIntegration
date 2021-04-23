import cdsapi
import sys
"""
Usage:
$ python3 api_request.py <variable> <start_date> <end_date> <min_lon> <max_lon> <min_lat> <max_lat> <downoad_file_name> <debug_mode>
"""

_, variable, start_date, end_date, min_lon, max_lon, min_lat, max_lat, download_file_name, debug_mode = sys.argv

def main(argv):

    c = cdsapi.Client()

    c.retrieve(
        'cams-global-reanalysis-eac4',
        {
            'date':  "{}/{}".format(start_date, end_date),
            'format': 'grib',
            'variable': variable,
            'time': [
                '12:00', '15:00',
            ],
            'area': [
                int(min_lon), int(min_lat), int(max_lon),
                int(max_lat),
            ],
        },
         download_file_name)
    
    if debug_mode == 'true':
        print("GRIB data has been saved to ", download_file_name)

if __name__ == "__main__":
   main(sys.argv)
