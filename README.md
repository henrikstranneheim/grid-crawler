# grid-crawler

Query multiple vcf files simultaneously using sample identity, position(s), genotypes or other vcf data

## Overview

Grid-crawler uses a grid file to enable querying of multiple vcf files simultaneously by wrapping around bcftools view and merge. Each query can be either processed sequentially via SHELL or submitted via an SBATCH script to SLURM and proccessed in parallel using xargs. 

## The grid (YAML - MAIN)

SampleID -> Path to raw vcf data 
 
### Input


 - -e/--environment Environment (shell|sbatch)

## Functions

0. Collect raw data from grid (User input - MAIN)
1. Generate instructions (User input - MAIN)
 - Parallelize per path - xargs (env)
 - Position search - bcftools/tabix (env)
 - Filtering - bcfTools (env)
2. Aggregate (MAIN)
 - Merge results - bcftools (env)


### Subset

1. Position {X:Y-Z}
2. Filtering {#CHROM POS  ID REF  ALT  QUAL FILTER INFO FORMAT SampleIDs}

### Input
 
 - -p/--pos Search at region (X:Y-Z)
 - -e/--exclude Exclude sites for which the expression is true
 - -i/--include Include  sites for which the expression is true
 - -o/outputFile Write output to file
 - -ot/output-type b: compressed BCF, u: uncompressed BCF, z: compressed VCF, v: uncompressed VCF [v]

## Environments

 - Shell
 - Sbatch

## Dependencies

1. Perl & perl modules
2. Conda
3. xargs
4. bcftools
5. tabix
