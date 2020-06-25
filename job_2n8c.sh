#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=job_2n8c.out
#SBATCH --time=00:05:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4

echo 'Job run on 2 nodes and 8 cores'
echo ''

mpirun python3 /home/heisenberg/twitter.py