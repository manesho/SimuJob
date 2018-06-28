"""
SimuJob Module

A tool to create and launch Matrix/array jobs and retrieve data on a SGE cluster.

"""

import itertools as it
import os
import paramiko
import xarray as xr

# System dependent default settings
validclusternames = ['itpwilson', 'neumann']
defaultusername = 'hornung'
defaultclustername = 'itpwilson'
defaultdependencies = ['/scratch1/hornung/soworm/worm.so',
				'/home/hornung/projects/sowoworm/pytools/simulator.py',
				'/home/hornung/projects/soworm/pytools/wormwrap.py',
				'/home/hornung/projects/soworm/pytools/run.py']

# Templates
launchfiletemplate = """#!/bin/bash
#$-S /bin/sh
#$-cwd
#$-j yes
#$-t 1-{nmax}
{argdefstring}
python simulator.py {argstring} -resultfile
"""


class Cluster(object):
		"""	
		Manages the communication to the cluster via ssh

		Args:
			username (str):		The username to log in via ssh
			clustername(str):	The name of the cluster to log in via ssh
		
		Attributes:
			username (str):		The username to log in via ssh
			clustername(str):	The name of the cluster to log in via ssh

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
			name (str):			The Job name on the SGE
			folder (str): 		The folder the job files are generated in, assumed to
								be on a device mounted on both, the cluster and the 
								current machine.

			arrayargs (dict):	A dictionary of the form {'parname': [parvalue1,parvalue2, ...], ...}
								The job will start a simulation for all combinations of parameters
								specified here.

			constargs (dict):   A dictionary of the form {'parname':parvalue ,...} 
								All parameters specified here will be used by all simulations launched
								by this job.

			workingcluster (Cluster):  A cluster object, to manage communication with the cluster 

			dependencies (list): A list of full paths to all files required by the simulation job

		Attributes:
			jobscriptname (str): The full path to the jobscript (submitted via qsub)
			
		"""
		def __init__(self, 
						folder ='',
						name ='run',
						arrayargs={},
						constargs={},
						workingcluster=Cluster(),
						dependencies = defaultdependencies):
			
			self.workingcluster = workingcluster
			self.folder = folder
			self.name = name
			self.arrayargs = arrayargs
			self.constargs = constargs

			# create flat lists over all combinations of arrayargs:
			flatlists = zip(*it.product(*self.arrayargs.values()))
			#recombine the lists with their name to a dictionary
			self.arrayargsflat = { parname:parvalues for parname, parvalues
														in zip(arrayargs.keys(), flatlists) } 
			self.jobscriptname = self.folder + self.name + '.sh'
		
			return
				
			
		def run(self):
			""" Launches the job via ssh """
			self.workingcluster.submit(self.jobscriptname)
			return

		def create_all_files(self):
			""" Creates the jobfile, jobdirectory and subdirectories if necessary and copys all other
				files, that a job debends on.
			"""
			for f in [self.folder, self.folder+"err/", self.folder+"out/", self.folder+"results/"]:
				os.makedirs(f, exist_ok = True)
	
			for dep in dependencies:
				os.system("cp "+dep+" "+self.folder)
			
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
									nmax = len(next(iter(self.arrayargsflat.values()))),
									argdefstring =  argdefstring,
									argstring = constargstring + arrayargstring
										)
			with open(self.jobscriptname, "w") as f:
					f.write(launchfilecontent)
					f.close()
			return 

	

		def retrieve_data(self):
			""" returns the result of the job, combined into a xarray

			
				Returns:
					data, an xarray.dataset containing one dataarray with coordinates specified by
					arrayargs and attributes specified by  constargs
			"""
			return


