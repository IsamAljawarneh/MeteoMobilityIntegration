import csv

with open('pm10lat.csv', 'r') as l:
    latitudes = [el.split(',') for el in l.read().split('\n')[:-1]] * 5

with open('pm10lon.csv', 'r') as l:
    longitudes = [el.split(',') for el in l.read().split('\n')[:-1]] * 5

with open('pm10variable.csv', 'r', encoding='utf-8-sig') as l:
    values = [el.split(',') for el in l.read().split('\n')]


assert (len(latitudes) == len(longitudes))

lat_lon_val_tuples_matrix = []

for n in range(len(latitudes)):
	lat_lon_val_tuples_matrix.append(list(zip(latitudes[n], longitudes[n], values[n])))


lat_lon_val_tuples = [item for sublist in lat_lon_val_tuples_matrix for item in sublist]

with open('pm_10_python.csv','w') as out:
	csv_output = csv.writer(out)
	csv_output.writerow(['lat', 'lon', 'pm10_value'])
	for row in lat_lon_val_tuples:
		csv_output.writerow(row)
