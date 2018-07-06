============
SimuJob
============

A small tool to facilitate trivial parallelization on a Sun Grid Engine (or similar) cluster.
Explicitly: starting the same program several times with different parameters.
It was developed to be used from within a Jupyter notebook.

What it can do:

    - Create a launchfile (to be submitted to SGE via qsub) for an arrayjob that will run
      over all combinations of the specified arguments.

    - Run the job via SSH.
    
    - Retrieve the resulting data to an Xarray n-dimensional Data Array with one dimension for 
      each specified array-argument (+ inner dimension depending on what you use it for).


Installation
============

git clone 

Usage
============

See example.ipynb

Requirements
============
To use SimuJob as it is, you need:
    
    - A SGE cluster 
    - A directory mounted on the cluster and on the machine wher SimuJob runs


