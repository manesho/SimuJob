import xarray as xr
import numpy as np
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('-x', type=float)
parser.add_argument('-y', type=float)
parser.add_argument('-n', type=float)
parser.add_argument('-fileout', type=str)
args = parser.parse_args()



ics = list(range(12))
jcs = list(range(10))

obs1 = [(args.x/args.y)**(args.n*i) for i in ics]
obs2 = [(args.y/args.x)**(args.n*j) for j in jcs]

xa1 = xr.DataArray(data=np.array(obs1), dims = ('i'), coords = {'i':ics}, name='obs1')
xa2 = xr.DataArray(data=np.array(obs2), dims = ('j'), coords = {'j':jcs}, name= 'obs2' )
fd = xr.Dataset({'obs1':xa1, 'obs2':xa2})
fd.to_netcdf(args.fileout)


