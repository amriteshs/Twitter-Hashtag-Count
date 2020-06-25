#!/bin/bash

#SBATCH --partition=physical
#SBATCH --output=job_1n8c.out
#SBATCH --time=00:05:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8

echo 'Job run on 1 node and 8 cores'
echo ''

mpirun python3 /home/heisenberg/twitter.py