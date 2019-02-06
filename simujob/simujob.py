"""
SimuJob Module

A tool to create and launch Matrix/array jobs and retrieve data on a SGE cluster.

The class MatrixJob can be used to generate job launch files, submite them to the 
cluster and retrieve the resulting data as a convenient xarray DataArray.

The launchfile is generated from a template specified in the launchfiletemplate variable.
Adjust it to your needs. 
As an example, for arrayargs={'a':[1,2], 'b':[3,4]} and constargs={'c':1, 'd':2} the
launchfile template

''
#$-t 1-{nmax}
{argdefstring}
foo {argstring}
''

will be expanded to
''
#$-t 1-5
a=(0 1 2 1 2)
b=(0 3 3 4 4)
rfname = (0 "results/a-1-b-3" "results/a-2-b-3" "results/a-1-b-4" "results/a-2-b-4")
foo -a ${a[${SGE_TASK_ID}]} -b ${b[${SGE_TASK_ID}]} -rfname ${rfname[${SGE_TASK_ID}]} -c 1 -d 2
''

note the additional argument rfname, that has not been specified. In order for this script together with
the retrieve data method to work properly, it is assumed that each of your simulations instances stores
its results to a file whose name can be specified with an argument.
The name of that argument can be specified in the variable fileargname.
This file can either be a plain csv file (one that can be read by numpy.loadtxt) or a netcdf file.
In the case of csv files, the inner dimension neames need to be specified by the variable innerdims.
For more complicated output data, with an arbitrary number of dimensions, use netcdf files.


"""

import itertools as it
import os
import paramiko
import xarray as xr
import numpy as np
from pandas import MultiIndex

#####################################
# System dependent default settings
####################################
validclusternames = ['itpwilson', 'neumann']
# used for the ssh connection:
defaultusername = 'hornung'
defaultclustername = 'itpwilson'
# all files the job depends on 
defaultdependencies = ['/scratch1/hornung/soworm/worm.so',
                '/home/hornung/projects/soworm/pytools/simulator.py',
                '/home/hornung/projects/soworm/pytools/wormwrap.py']

# Templates - simulation program dependent 
fileargname = "rfname"
# adjust this to you're need...
# {argdefstring}, {argstring} and {nmax} will be replaced
launchfiletemplate = """#!/bin/bash
#$-S /bin/sh
#$-cwd
#$-j yes
#$-t 1-{nmax}
{argdefstring}
python3 simulator.py {argstring} 
"""
# The name of the data axes as stored in the files by the simulation.
# only necessary if you wish to use the retrieve_data method to load the data
# make sure that this matches! 
innerdims = ('i', 'observable_index')

class Cluster(object):
        """     
        Manages the communication to the cluster via ssh

        Args:
            username (str):         The username to log in via ssh
            clustername(str):   The name of the cluster to log in via ssh
        
        Attributes:
            username (str):         The username to log in via ssh
            clustername(str):   The name of the cluster to log in via ssh

        """
        def __init__(self, username=defaultusername, clustername=defaultclustername):
                self.username = username
                if clustername not in validclusternames:
                    print('Warning, invalid cluster name')
                self.clustername = clustername

        
        def submit(self, jobscriptname):
            """ Submits a job to the cluster, the job is executed from the
                location where the jobscrit is.

            Args:
                jobscriptname (str): The full path to the jobscript that is submitted to 
                                        the cluster via qsub
            """
            with paramiko.SSHClient() as client:
                    client.load_system_host_keys()
                    client.set_missing_host_key_policy(paramiko.WarningPolicy)
                # Establish SSH connection  
                    client.connect(self.clustername, username=self.username)
                    (path, fname) = os.path.split(jobscriptname)
                    stdin, stdout, stderror =client.exec_command("cd {};qsub {}".format(path,fname))
                    print(stderror)
            return



class MatrixJob(object):
        """
        The main Class in this module. 

        Args:
            name (str):             The Job name on the SGE

            localpath (str):        The local path to the folder the job files are generated in, assumed to
                                be on a device mounted on both, the cluster and the 
                                current machine.

            remotepath (str):       The path on the Cluster to the folder the job files are generated in.

            arrayargs (dict):   A dictionary of the form {'parname': [parvalue1,parvalue2, ...], ...}
                                The job will start a simulation for all combinations of parameters
                                specified here.

            constargs (dict):   A dictionary of the form {'parname':parvalue ,...} 
                                All parameters specified here will be used by all simulations launched
                                by this job.

            workingcluster (Cluster):  A cluster object, to manage communication with the cluster 

            dependencies (list): A list of full paths to all files required by the simulation job,
                                including the main executable.

        Attributes:
            jobscriptname (str): The full path to the jobscript (submitted via qsub)

            resultfilenames(list): a list of strings containing the file names of all simulation results                                
            
        """
        def __init__(self, 
                   folder='',
                   localpath ='',
                   remotepath ='',
                   name ='run',
                   arrayargs={},
                   constargs={},
                   workingcluster=Cluster(),
                   dependencies = defaultdependencies):
            self.workingcluster = workingcluster
            self.localpath =localpath 
            self.remotepath =remotepath 
            self.name = name
            self.arrayargs = arrayargs
            self.constargs = constargs
            self.dependencies = dependencies
            #if folder is specified instead of local and remote path, set both to folder. (for backwards compatibility)
            if not folder == '':
                            self.localpath=folder
                            self.remotepath=folder
            # create flat lists over all combinations of arrayargs:
            flatlists = list(zip(*it.product(*self.arrayargs.values())))
            self.ta=flatlists
            #recombine the lists with their name to a dictionary
            self.arrayargsflat = { parname:parvalues for parname, parvalues
                                                        in zip(arrayargs.keys(), flatlists) } 
            # to create the resultfilenamelist: tranpose the flat arrayargs dict:
            self.arrayargsflattr = [dict(zip(self.arrayargsflat.keys(), partuple))
                                        for partuple in zip(*self.arrayargsflat.values())]
            # concatenate to filenames and sort alphabetically to be reproducible
            rfnames = ['"results/'+'-'.join(
                                [parname+'-'+str(parvalue)
                                        for parname, parvalue in sorted(pardict.items())]
                                                        )   + '.dat"' 
                                                                    for pardict in self.arrayargsflattr]

            self.localjobscriptname = self.localpath + self.name + '.sh'
            self.remotejobscriptname = self.remotepath + self.name + '.sh'
            self.arrayargsflat[fileargname]=rfnames         
            return
                
            
        def run(self):
            """ Launches the job via ssh """
            self.workingcluster.submit(self.remotejobscriptname)
            return

        def create_all_files(self):
            """ Creates the jobfile, jobdirectory and subdirectories if necessary and copys all other
                files, that a job debends on.
            """
            for f in [self.localpath, self.localpath+"err/", self.localpath+"out/", self.localpath+"results/"]:
                os.makedirs(f, exist_ok = True)
    
            for dep in self.dependencies:
                os.system("cp "+dep+" "+self.localpath)

            self.create_launch_file()
            
            return

        def create_launch_file(self):
            """ Creates the launch file to be submitted to SGE
                uses the global string launchfiletemplate as a basis
            """
            # create a string of the form
            #"""
            # arg1 = (0 a1v1 a1v2 a1v3 )
            # arg2 = (0 a2v1 a2v2)
            # ...
            # """ 
            # out of the arrayargsflat dict {'arg1':[a1v1,a1v2, a1v3], 'arg2':[a2v1,a2v2], ...}
            argdefstring = "\n".join(
                        [("{}=(0 "+ ("{} "*len(parvalues)) +")").format(parname,*parvalues)  
                                    for parname,parvalues in self.arrayargsflat.items()])
            
            # create the string
            #  -arg1 ${arg1[${SGE_TASK_ID}] }
            arrayargstring = " ".join([" -{} ${{{}[${{SGE_TASK_ID}}]}}".format(key, key)
                                                        for key in self.arrayargsflat.keys()])
            constargstring = " ".join( ["-{} {} ".format(name,value) 
                                                    for name, value in self.constargs.items() ])

            launchfilecontent = launchfiletemplate.format(
                                    # the plus one is needed because bash array indexing starts with
                                    # 0 and the SGE_TASk_ID always starts with 1
                                    nmax = len(next(iter(self.arrayargsflat.values())))+1,
                                    argdefstring =  argdefstring,
                                    argstring = constargstring + arrayargstring
                                        )
            with open(self.localjobscriptname, "w") as f:
                    f.write(launchfilecontent)
                    f.close()
            return 

                
        def retrieve_data(self):
            """ returns the data generated by the matrix job, combined into an xarray

            
                Returns:
                    data (DataArray):   an xarray.DataArray containing one dataarray
                                        with coordinates specified by arrayargs and attributes 
                                        specified by  constargs. The inner dimensions are named 
                                        with the names specifyed in the variable innerdims (for txt data). 
            """
                    
            try:
                data = [np.loadtxt(self.localpath+fname.strip('"')) 
                                for fname in self.arrayargsflat[fileargname] ]
                xrdata = xr.DataArray(np.array(data), dims=('pars', *innerdims), attrs=self.constargs)
            except:
                data = [xr.open_dataset(self.localpath+fname.strip('"')) 
                                for fname in self.arrayargsflat[fileargname] ]
                xrdata = xr.concat(data, dim='pars')

            #create a multiindex coordinate for the pars dimension:
            parvaluesarray = [value for key, value in sorted(self.arrayargsflat.items())]
            names = ([key for key in sorted(self.arrayargsflat.keys())])
            #remove the rfname as name and value
            parvaluesarray.pop(names.index(fileargname))
            names.remove(fileargname)
            mi = MultiIndex.from_arrays(parvaluesarray,     names=names)
            xrdata.coords['pars']=mi
            return xrdata.unstack('pars')


        def retrieve_xrdata_ignore_missing(self):
            def try_load_data_xr(name):
                    try:
                         data = xr.open_dataset(name)
                         return data
                    except:
                         print('Ignoreing {} (was not found)'.format(name))
                         return None
            #loop over
            datalist=[]
            for i in range(len(self.arrayargsflat['rfname'])):
                fname = self.arrayargsflat['rfname'][i]
                ds = try_load_data_xr(self.localpath+fname.strip('"'))
                if not ds is None:
                #add dimensions for all arrayarg parameters:
                    parnames = ([key for key in sorted(self.arrayargsflat.keys())])
                    parnames.remove(fileargname)
                    dsexpanded=ds.expand_dims(parnames)
                    # add the coordinates
                    for parname in parnames:
                            dsexpanded.coords[parname] = [ self.arrayargsflat[parname][i] ]
                    datalist.append(dsexpanded)
            return xr.merge(datalist)
            