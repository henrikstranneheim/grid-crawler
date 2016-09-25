# grid-crawler

## The grid (YAML - MAIN)

Three levels: 

1. Cohort
2. Case
3. SampleID -> Path to raw vcf data (indexed)

### Subset

 - None i.e. all
 - Cohort(s) i.e all cases and sampleIds within cohort
 - Case(s) i.e. all samples within case
 - Sample(s)
 
### Input

 - -g/--grid Grid YAML file
 - -c/--cluster Cluster to analyze (cohort=case-sampleID_1,sampleID_N)
 - -e/--environment Environment (shell|sbatch)
 - -pr/processes Number of parallel processes (xargs)

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
