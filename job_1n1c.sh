#!/bin/bash
#SBATCH --partition=physical
#SBATCH --output=job_1n1c.out
#SBATCH --time=00:30:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1

echo 'Job run on 1 node and 1 core'
echo ''

mpirun python3 /home/heisenberg/twitter.py