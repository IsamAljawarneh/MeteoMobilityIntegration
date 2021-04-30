#!/usr/bin/env python3

import cdsapi
import netCDF4
from netCDF4 import num2date
import numpy as np
import os
import pandas as pd

# Open netCDF4 file
pm10_location = "../data/bologna_110_historic_cEUPM10daily.nc"
o3_location = "../data/bologna_110_historic_cEUO3daily.nc"

pm10 = netCDF4.Dataset(pm10_location, mode='r')
o3 = netCDF4.Dataset(o3_location, mode='r')


pm10_lat = pm10.variables['lat'][:]
pm10_lon = pm10.variables['lon'][:]

o3_lat = o3.variables['lat'][:]
o3_lon = o3.variables['lon'][:]


# Continue testing from here

o3_time_var = o3.variables['time']
o3_dtime = netCDF4.num2date(o3_time_var[:], o3_time_var.units)

pm10_time_var = pm10.variables['time']
pm10_dtime = netCDF4.num2date(pm10_time_var[:], pm10_time_var.units)

o3_values = nc.variables['cEUO3daily'][:]
pm10_values = nc.variables['cEUPM10daily'][:]

o3_ts = pd.Series(o3_values, index=o3_dtime)
pm10_ts = pd.Series(pm10_values, index=pm10_dtime)

o3_ts.to_csv(output_dir + 'precip.csv', header=True)
pm10_ts.to_csv(output_dir + 'precip.csv', header=True)


# o3_ts.to_csv(output_dir + 'precip.csv',index=True, header=True)
# pm10_ts.to_csv(output_dir + 'precip.csv',index=True, header=True)