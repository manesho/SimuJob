"""
SimuJob Module

A tool to create and launch Matrix/array jobs and retrieve data on a SGE eluster.

The class MatrixJob can be used to generate job launch files, submite them to the 
cluster and retrieve the resulting data as a convenient xarray DataArray.

The launchfile is generated from a template specified in the launchfiletemplate variable.
Adjust it to your needs. 
As an example, for arrayargs={'a':[1,2], 'b':[3,4]} and constargs={'c':1, 'd':2} the
launchfile template

''
#!/bin/bash
#SBATCH --mail-type=NONE
#SBATCH --time=24:00:00     
#SBATCH --array=1-{nmax} 
#SBATCH -e err/%A_%a.err
#SBATCH -o out/%A_%a.out

{argdefstring}
runmyprogram {argstring}
''

will be expanded to
''
#!/bin/bash
#SBATCH --mail-type=NONE
#SBATCH --array=1-5
#SBATCH -e err/%A_%a.err
#SBATCH -o out/%A_%a.out

a=(0 1 2 1 2)
b=(0 3 3 4 4)
rfname = (0 "results/a-1-b-3" "results/a-2-b-3" "results/a-1-b-4" "results/a-2-b-4")
runmyprogram -a ${a[${SGE_TASK_ID}]} -b ${b[${SGE_TASK_ID}]} -rfname ${rfname[${SGE_TASK_ID}]} -c 1 -d 2
''

note the additional argument rfname, that has not been specified. In order for this script together with
the retrieve data method to work properly, it is assumed that each of your simulations instances stores
its results to a file whose name can be specified with an argument.
The name of that argument can be specified in the variable fileargname.
This file can either be a plain csv file (one that can be read by numpy.loadtxt) or a netcdf file.
In the case of csv files, the inner dimension neames need to be specified by the variable innerdims.
For more complicated output data, with an arbitrary number of dimensions, use netcdf files, that can be opened with xarray.open_dataset

"""

import itertools as it
import os
from os import path
import subprocess
import xarray as xr
import numpy as np
from pandas import MultiIndex

#####################################
# System dependent default settings
####################################
# all files the job depends on 

# {argdefstring}, {argstring} and {nmax} will be replaced
defaultlaunchfiletemplate = """#!/bin/bash
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



class MatrixJob(object):
        """
        The main Class in this module. 

        Args:
            name (str):             The Job name on the SGE


            sshremote(str):    The user and host address of the cluster, e.g. user@cluster.ch such that one can login to the cluster with ssh sshremote.

            localpath (str):        The local path to the folder the job files are generated in, assumed to
                                be on a device mounted on both, the cluster and the 
                                current machine.

            remotepath (str):       The path on the Cluster to the folder the job files are generated in.

            arrayargs (dict):   A dictionary of the form {'parname': [parvalue1,parvalue2, ...], ...}
                                The job will start a simulation for all combinations of parameters
                                specified here.

            zipargs (dict):      A dictionary of the form {'parnameA':parvaluesA, 'parnameB':parvaluesB ...}
                                Where len(parvaluesA) = len(parvaluesB). These parameters will be treated as one parameter in arrayargs.

            constargs (dict):   A dictionary of the form {'parname':parvalue ,...} 
                                All parameters specified here will be used by all simulations launched
                                by this job.


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
                     sshremote='user@cluster.ch',
                     name ='a_short_but_descriptive_jobname',
                     arrayargs={},
                     zipargs={},
                     constargs={},
                     task_id_str='SLURM_ARRAY_TASK_ID',
                     dependencies = [],
                     launchfiletemplate = """
#!/bin/bash
#SBATCH --mail-type=NONE
#SBATCH --array=1-{nmax}
#SBATCH -e err/%A_%a.err
#SBATCH -o out/%A_%a.out

{argdefstring}
runmyprogram {argstring}
""",
                     fileargname = 'rfname' 
        ):
        
            self.sshremote=sshremote
            self.task_id_str= task_id_str
            self.localpath =localpath 
            self.remotepath =remotepath 
            self.name = name
            self.arrayargs = arrayargs
            self.zipargs = zipargs 
            self.constargs = constargs
            self.dependencies = dependencies
            #if folder is specified instead of local and remote path, set both to folder. (for backwards compatibility)
            if not folder == '':
                            self.localpath=folder
                            self.remotepath=folder
            # create flat lists over all combinations of arrayargs:
            self.launchfiletemplate = launchfiletemplate
            self.fileargname = fileargname

            flatlists = list(zip(*it.product(*self.arrayargs.values())))
            self.parvalues=flatlists

            if not zipargs == {}:
                    l=list(self.arrayargs.values()) +[list(zip(*self.zipargs.values()))]
                    l2 =list(it.product(*l))
                    l3=[ l[:-1]+l[-1] for l in l2 ]
                    self.parvalues = list(zip ( * l3))

            parnames = list(self.arrayargs.keys())+list(self.zipargs.keys())
        

            #recombine the lists with their name to a dictionary
            self.arrayargsflat = { parname:parval for parname, parval in zip(parnames, self.parvalues) } 
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
            self.arrayargsflat[self.fileargname]=rfnames         
            return
                
            

        def rsync_here2there(self):
                subprocess.call(['ssh', self.sshremote, 'mkdir -p {}'.format(self.remotepath)])
                subprocess.call(['rsync', '-av',self.localpath ,self.sshremote+':'+self.remotepath])
 
        def rsync_there2here(self):
                subprocess.call(['rsync', '-av', self.sshremote+':'+self.remotepath,self.localpath])

        def submit(self, submissioncmd='sbatch', extracmds=''):
                path, fname = os.path.split(self.remotejobscriptname)
                stdout = subprocess.check_output(
                        ['ssh',self.sshremote, 'source ~/.bash_profile; cd {}; {} {} {}'.format(
                                path, extracmds,submissioncmd, fname
                        )])

                self.jobid=int(stdout.split()[-1])
                print(stdout)

        def get_status(self, byname= False):
                jobidentifier = self.name if byname else self.jobid
                stdout = subprocess.check_output(
                        ['ssh',self.sshremote, 'squeue | grep {} | wc -l'.format(
                                jobidentifier
                        )])
                print("{} out of {} jobs still running".format(
                        int(stdout),
                        len(self.parvalues[0])))

        def delete_errors(self):
                subprocess.check_output(['ssh', self.sshremote, 'rm {}err/*.err'.format(self.remotepath)])
                

        def print_errors(self):
                stdout = subprocess.check_output(['ssh', self.sshremote, 'cat {}err/*.err'.format(self.remotepath)])
                print(str(stdout).replace('\\n', '\n'))
        def delete_stdout(self):
                subprocess.check_output(['ssh', self.sshremote, 'rm {}out/*.out'.format(self.remotepath)])

        def print_stdout(self):
                stdout = subprocess.check_output(['ssh', self.sshremote, 'cat {}out/*.out'.format(self.remotepath)])
                print(str(stdout).replace('\\n', '\n'))


        

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

        def create_launch_file_content(self):
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
            # the zero after the paranthesis is a hack: bash array indexing starts with zero, but sge_task_id with one
            argdefstring = "\n".join(
                        [("{}=(0 "+ ("{} "*len(parvalues)) +")").format(parname,*parvalues)  
                                    for parname,parvalues in self.arrayargsflat.items()])
            
            # create the string
            #  -arg1 ${arg1[${SGE_TASK_ID}] }
            arrayargstring = " ".join([" -{} ${{{}[${{{}}}]}}".format(key, key, self.task_id_str)
                                                        for key in self.arrayargsflat.keys()])
            constargstring = " ".join( ["-{} {} ".format(name,value) 
                                                    for name, value in self.constargs.items() ])

            launchfilecontent = self.launchfiletemplate.format(
                                    nmax = len(next(iter(self.arrayargsflat.values()))),
                                    argdefstring =  argdefstring,
                                    argstring = constargstring + arrayargstring
                                        )

            return launchfilecontent

        def create_launch_file(self):
            launchfilecontent = self.create_launch_file_content()
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
                                for fname in self.arrayargsflat[self.fileargname] ]
                xrdata = xr.DataArray(np.array(data), dims=('pars', *innerdims), attrs=self.constargs)
            except:
                data = [xr.open_dataset(self.localpath+fname.strip('"')) 
                                for fname in self.arrayargsflat[self.fileargname] ]
                xrdata = xr.concat(data, dim='pars')

            #create a multiindex coordinate for the pars dimension:
            parvaluesarray = [value for key, value in sorted(self.arrayargsflat.items())]
            names = ([key for key in sorted(self.arrayargsflat.keys())])
            #remove the rfname as name and value
            parvaluesarray.pop(names.index(self.fileargname))
            names.remove(self.fileargname)
            mi = MultiIndex.from_arrays(parvaluesarray, names=names)
            xrdata.coords['pars']=mi
            return xrdata.unstack('pars')


        def retrieve_xrdata_ignore_missing(self, verbose=False):
            allfiles = [(self.localpath+fname.strip('"'))
                        for fname in self.arrayargsflat[self.fileargname] ]
            if verbose:
                    for f in allfiles:
                            if not  path.exists(f):
                                    print('ignoreing '+f)


            data = [xr.open_dataset(f) for f in allfiles if path.exists(f) ]

            parvaluesarray = [[v for v,f in zip(value, allfiles) if path.exists(f) ] for key, value in sorted(self.arrayargsflat.items())]
            names = ([key for key in sorted(self.arrayargsflat.keys())])
            #remove the rfname as name and value
            parvaluesarray.pop(names.index(self.fileargname))
            names.remove(self.fileargname)
            mi = MultiIndex.from_arrays(parvaluesarray, names=names)
            
#            try:
#                    xrdata = xr.concat(data, dim='pars')
#                    
#            except:
#                    return data,mi
#            xrdata.coords['pars']=mi
            return data, mi 



