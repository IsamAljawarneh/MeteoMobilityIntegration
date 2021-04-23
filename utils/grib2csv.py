#!/usr/bin/env python

import sys, getopt

def main(argv):

   input_file = ''
   output_file = ''

   try:
      opts, args = getopt.getopt(argv,"hi:o:d:",["ifile=","ofile=","debug_mode="])

   except getopt.GetoptError:
      print('grib2csv.py -i <input_file> -o <output_file>')
      sys.exit(2)

   for opt, arg in opts:
      if opt == '-h':
         print('grib2csv.py -i <input_file> -o <output_file>')
         sys.exit()
      elif opt in ("-i", "--input"):
         input_file = arg
      elif opt in ("-o", "--output"):
         output_file = arg
      elif opt in ("-d", "--debug"):
         debug_mode = arg
   if debug_mode == 'true':
      print('Input file is ', input_file)
      print('Output file is data/', output_file)

   with open(input_file, 'r') as f_in, open(output_file, 'w') as f_out:
      [f_out.write(','.join(line.split()) + '\n') for line in f_in]

if __name__ == "__main__":
   main(sys.argv[1:])
