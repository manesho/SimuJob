"""
SimuJob Module

A flexible tool to create and launch jobs and retrieve data on a SGE cluster.

provides the SimulationJob class.

"""


# System dependent default settings
validclusternames = ['itpwilson', 'neumann']
defaultuser = 'hornung'
defaultclustername = 'itpwilson'

import os
import paramiko
import xarray as xr

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

		Attributes:
			jobfilename
			
		"""
		def __init__(self, 
						folder ='',
						name ='run',
						arrayargs={},
						constargs={},
						workingcluster=Cluster(),
						)
				
			
		def run(self):
			""" Launches the job via ssh """
			return

		def create_all_files(self):
			""" Creates the jobfile, jobdirectory and subdirectories if necessary and copys all other
				files, that a job debends on.
			"""
			return

		def retrieve_data(self):
			""" returns the result of the job, combined into a xarray

			
				Returns:
					data, a xarray.dataset containing one dataarray with coordinates specified by
					arrayargs and attributes specified by  constargs
			"""
			return

