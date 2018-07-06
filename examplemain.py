import csv
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('-x', type=float)
parser.add_argument('-y', type=float)
parser.add_argument('-n', type=float)
parser.add_argument('-fileout', type=str)
args = parser.parse_args()

with open(args.fileout, "w") as resfile:
		wr =csv.writer(resfile, delimiter='\t')
		wr.writerows([[(args.x/args.y)**args.n]])


