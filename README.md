# grid-crawler

Query multiple vcf files simultaneously using sample identity, position(s), genotypes or other vcf data.

## Overview

Grid-crawler uses a grid file to enable querying of multiple vcf files simultaneously by wrapping around bcftools view and merge. Each query can be either processed sequentially via SHELL or submitted via an SBATCH script to SLURM and proccessed in parallel using xargs. 

## Example usage

```
$ perl grid-crawler grid-file.yaml -s sampleID_1 -s sampleID_2  -s sampleID_3 -p 1,X:200050-2000600,22:456793-456793 -g ^miss -e "DV>5 && INFO/SGB>3 " -env shell -sen source activate mip4.0 -ot b
```

## Installation

Grid-crawler is written in perl and therfore requires that perl is installed on your OS. 


 ```
 $ git clone https://github.com/henrikstranneheim/grid-crawler.git
 $ cd grid-crawler
 ```

After this you can decide whether to make grid-crawler an "executable" by either adding the install directory to the ``$PATH`` in e.g.  "``~/.bash_profile``" or move all the files from this directory to somewhere already in your path like "``~/usr/bin``". 
 Remember to make the file(s) executable by ``chmod +x file``.

### Testing

```
$ cd t; perl run-test.pl
```

## Input

### The grid (YAML - MAIN)

SampleID: [Path to vcf data]
 
Can be compressed/uncompressed bcf/vcf format. SampleIDs that point to the same vcf path will be handled within the same query.
 
## Subset the grid and/or data

### SampleID(s)

Limit the output to the specified sampleID(s) within the grid and vcf file - based on flag '-s sampleID'.

### Position(s)

Only query variants for positions - based on flag '-p [A,B:C,X:Y-Z]'.

### Filtering

Exclude ('-e') or include ('-i') variants based on filtering expression - uses bcftools API (see bcftools manual page for details). 
Basically anything in the vcf can be filtered using this approach.

### Genotype

Inlcude or exclude (prefix with ^) genotypes - based on flag '-g [het|hom|miss]'.

## Execution

### Environments

Execute the query sequenctially in shell or in parallel (xargs or classic print/wait) via sbatch and SLURM ('-env [shell|sbatch]').

#### SLURM

Requires a mandatory flag '--projectID [X]', and some additional options of setting email alerts and SLURM quality of service.

#### Conda environment

Optionally activate a conda environment prior to executing queries.

## Output

Grid-crawler will automatically merge the output variants to a single file if analysing more than two unique paths.

Redirect query output data using ('--outDataDir').

Set the output type using ('--outputType [b: compressed BCF, z: compressed VCF]').

## Dependencies

 - Perl (>=5.18)
 - Perl modules: "Modern::Perl", "autodie", "IPC::Cmd", "YAML", "Log::Log4perl", "TAP::Harness"
 - bcftools (>=1.3.0)
 
 When using Sbatch
 - SLURM
 - xargs (>=4.4.2)

