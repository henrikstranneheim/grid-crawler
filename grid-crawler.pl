#!/usr/bin/env perl

###Copyright 2016 Henrik Stranneheim

use Modern::Perl '2014';
use warnings qw( FATAL utf8 );
use autodie;
use v5.18;  #Require at least perl 5.18
use utf8;  #Allow unicode characters in this script
use open qw( :encoding(UTF-8) :std );
use charnames qw( :full :short );

use Getopt::Long;
use Params::Check qw[check allow last_error];
$Params::Check::PRESERVE_CASE = 1;  #Do not convert to lower case
use Time::Piece;
use Cwd;
use File::Path qw(make_path);
use File::Spec::Functions qw(catdir catfile devnull);
use List::Util qw(any);
use IPC::Cmd qw[can_run run];

## Third party module(s)
use YAML;
$YAML::QuoteNumericStrings = 1;  #Force numeric values to strings in YAML representation
use Log::Log4perl;

our $USAGE;

BEGIN {
    $USAGE =
	qq{grid-crawler.pl grid.yaml -s sampleID > outfile.vcf
           -s/--sampleIDs SampleIDs to analyze
           -p/--positions Search at region (A,B:C,X:Y-Z)
           -en/--exclude Exclude sites for which the expression is true
           -i/--include Include sites for which the expression is true
           -g/--genoType Require one or more hom/het/missing genotype or, if prefixed with "^", exclude sites with hom/het/missing genotypes
           -e/environment Execute search via SHELL or SBATCH (defaults to "SHELL")
           -pro/--projectID The project ID (mandatory if using SBATCH)
           -em/--email E-mail (defaults to "")
           -mc/--maximumCores The maximum number of cores per node used in the analysis (defaults to "16")
           -qos/--slurmQualityofService SLURM quality of service command in sbatch scripts (defaults to "low")
           -sen/--sourceEnvironmentCommand Source environment command in sbatch scripts (defaults to "")
           -x/--xargs Use xargs in sbatch for parallel execution (defaults to "1" (=yes))
           -od/--outDataDir The data files output directory
           -ot/--outputType The output type b: compressed BCF, z: compressed VCF (defaults to "b")
           -l/--logFile Log file (defaults to "grid-crawler-log/{date}/{scriptname}_{timestamp}.log")
           -h/--help Display this help message   
           -v/--version Display version
           verb/--verbose Verbose
        };    
}

my ($gridfile, $exclude, $include, $genoType, $outDataDir, $logfile, $verbose);
my ($environment, $outputType) = ("shell", "b");

my @positions;
my @sampleIDs;
my @programs = ("bcftools");
my @sourceEnvironmentCommand;

##Sbatch parameters
my %sbatchParameter;

$sbatchParameter{xargs} = 1;

$sbatchParameter{maximumCores} = 16;

my $logger;  #Will hold the logger object
my $gridCrawlerVersion = "0.0.0";

## Add dateTimestamp for later use in log
my $dateTime = localtime;
my $dateTimeStamp = $dateTime->datetime;
my $date = $dateTime->ymd;
my $script = (`basename $0`);  #Catches script name
chomp($dateTimeStamp, $date, $script);  #Remove \n;

## Enables cmd to print usage help 
if(!@ARGV) {

    &Help({USAGE => $USAGE,
	   exitCode => 0,
	  });
}
else { #Collect potential infile - otherwise read from STDIN

    $gridfile = $ARGV[0];
}

###User Options
GetOptions('s|sampleIDs:s' => \@sampleIDs,
	   'p|positions:s' => \@positions,
	   'e|exclude:s' => \$exclude,
	   'i|include:s' => \$include,
	   'g|genoType:s' => \$genoType,
	   'en|environment:s' => \$environment,
	   'pro|projectID:s'  => \$sbatchParameter{projectID},
	   'mc|maximumCores=n' => \$sbatchParameter{maximumCores},  #Per node
	   'em|email:s'  => \$sbatchParameter{email},  #Email adress
	   'qos|slurmQualityofService=s' => \$sbatchParameter{slurmQualityofService},
	   'sen|sourceEnvironmentCommand=s{,}' => \@sourceEnvironmentCommand,
	   'x|xargs=n' => \$sbatchParameter{xargs},
	   'od|outDataDir:s'  => \$outDataDir,
	   'ot|outputType:s' => \$outputType,
	   'l|logFile:s' => \$logfile,
	   'h|help' => sub { say STDOUT $USAGE; exit;},  #Display help text
	   'v|version' => sub { say STDOUT "\ngrid-crawler.pl ".$gridCrawlerVersion, "\n"; exit;},  #Display version number
	   'verb|verbose' => \$verbose,
    )  or &Help({USAGE => $USAGE,
		 exitCode => 1,
		});

## Create log directory, default logfile path, initiate logger object
$logfile = &InitiateLog({logfileRef => \$logfile,
			 scriptRef => \$script,
			 dateRef => \$date,
			 dateTimeStampRef => \$dateTimeStamp,
			});

if(! @sampleIDs) {

    $logger->fatal("Please provide sampleIDs");
    exit 1;
}

$logger->info("Including sampleIDs(s): ".join(", ", @sampleIDs), "\n");

if(@sourceEnvironmentCommand) {

    if ($sourceEnvironmentCommand[-1] !~ /\;$/) {

	push(@sourceEnvironmentCommand, ";");
    }
}

if($environment =~/SBATCH/i) {

    ## Required sbatch parameters
    my @sbatchParameters = ("projectID", "xargs");

    foreach my $parameter (@sbatchParameters) {

	if ( ! $sbatchParameter{$parameter}) {
	    
	    $logger->fatal("Please provide ".$parameter." when submitting via sbatch");
	    exit 1;
	}
	if ($parameter eq "xargs") {

	    push(@programs, "xargs");
	}
    }

    ## Check email adress format
    if ($sbatchParameter{email}) {  #Allow no malformed email adress
	
	&CheckEmailAddress({emailRef => \$sbatchParameter{email},
			   });
    }
}

## Check program(s) can run and log version if so. Otherwise exit
&CheckProgram({programsArrayRef => \@programs,
	       verbose => $verbose,
	      });

## Set fileending depending on outputType
my $outfileEnding .= ".bcf.gz";  

if ($outputType eq "z") {
    
    $outfileEnding = ".vcf.gz"
}

if (! $outDataDir) {
    
    $outDataDir = catfile(cwd(), "gc_analysis", $dateTimeStamp);
}


############
####MAIN####
############

## Create outDataDir
make_path($outDataDir);

## Loads a YAML file into an arbitrary hash and returns it.
my %grid = &LoadYAML({yamlFile => $gridfile,
		     });

my %path = &SearchGrid({gridHashRef => \%grid,
			sampleIDsArrayRef => \@sampleIDs,
		       });

my %cmd = &BcfToolsViewCmd({pathHashRef => \%path,
			    positionsArrayRef => \@positions,
			    outDataDir => $outDataDir,
			    exclude => $exclude,
			    include => $include,
			    genoType => $genoType,
			    outputType => $outputType,
			    outfileEnding => $outfileEnding,
			   });

my @mergecmds = &BcfToolsMergeCmd({pathHashRef => \%path,
				   outDataDir => $outDataDir,
				   outputType => $outputType,
				   outfileEnding => $outfileEnding,
				  });

&SubmitCommand({pathHashRef => \%path,
		cmdHashRef => \%cmd,
		sbatchParameterHashRef => \%sbatchParameter,
		mergecmdsArrayRef => \@mergecmds,
		sourceEnvironmentCommandArrayRef => \@sourceEnvironmentCommand,
		environment => $environment,
		verbose => $verbose
	       });


######################
####SubRoutines#######
######################


sub Help {

##Help
    
##Function : Print help text and exit with supplied exit code
##Returns  : ""
##Arguments: $USAGE, $exitCode
##         : $USAGE    => Help text
##         : $exitCode => Exit code

    my ($argHashRef) = @_;

    ## Flatten argument(s)
    my $USAGE;
    my $exitCode;

    my $tmpl = { 
	USAGE => {required => 1, defined => 1, strict_type => 1, store => \$USAGE},
	exitCode => { default => 0, strict_type => 1, store => \$exitCode},
    };
    
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];    
    
    say STDOUT $USAGE;
    exit $exitCode;
}


sub InitiateLog {

##InitiateLog
    
##Function : Create log directory, default logfile path, initiate logger object
##Returns  : ""
##Arguments: $logfileRef, $scriptRef, $dateRef, $dateTimeStampRef
##         : $logfileRef       => User supplied info on cmd for logFile option {REF}
##         : $scriptRef        => The script that is executed {REF}
##         : $dateRef          => The date {REF}
##         : $dateTimeStampRef => The date and time {REF}

    my ($argHashRef) = @_;

    ## Flatten argument(s)
    my $logfileRef;
    my $scriptRef;
    my $dateRef;
    my $dateTimeStampRef;

    my $tmpl = { 
	logfileRef => { default => \$$, strict_type => 1, store => \$logfileRef},
	scriptRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$scriptRef},
	dateRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$dateRef},
	dateTimeStampRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$dateTimeStampRef},
    };
    
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];
    
    if(! $$logfileRef) {  #No input from cmd i.e. create default logging directory and set default
	
	## Create default logfile dir and file path
	make_path(catdir(cwd(), "gc_analysis", "gc_log", $$dateRef));

	$logfile = catfile(cwd(), "gc_analysis", "gc_log", $$dateRef, $$scriptRef."_".$$dateTimeStampRef.".log");
    }
    
    ## Creates log for the master script
    my $config = &CreateLog4perlCongfig({filePathRef => \$$logfileRef,
					});
    Log::Log4perl->init(\$config);
    $logger = Log::Log4perl->get_logger("gcLogger");
}


sub CreateLog4perlCongfig {

##CreateLog4perlCongfig
    
##Function : Create log4perl config file. 
##Returns  : "$config"
##Arguments: $filePathRef
##         : $filePathRef => log4perl config file path {REF}

    my ($argHashRef) = @_;

    ## Flatten argument(s)
    my $filePathRef;

    my $tmpl = { 
	filePathRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$filePathRef},
    };
    
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    my $config = q?
        log4perl.category.gcLogger = TRACE, LogFile, ScreenApp
        log4perl.appender.LogFile = Log::Log4perl::Appender::File
        log4perl.appender.LogFile.filename = ?.$$filePathRef.q?
        log4perl.appender.LogFile.layout=PatternLayout
        log4perl.appender.LogFile.layout.ConversionPattern = [%p] %d %c - %m%n

        log4perl.appender.ScreenApp = Log::Log4perl::Appender::Screen
        log4perl.appender.ScreenApp.layout = PatternLayout
        log4perl.appender.ScreenApp.layout.ConversionPattern = [%p] %d %c - %m%n
        ?;
    return $config;
}


sub CheckProgram {

##CheckProgram

##Function : Check program(s) can run and log version if so. Otherwise exit.
##Returns  : ""
##Arguments: $programsArrayRef, $verbose
##         : $programsArrayRef => Programs to check
##         : $verbose          => Verbose

    my ($argHashRef) = @_;

    ## Flatten argument(s)
    my $programsArrayRef;
    my $verbose;

    my $tmpl = { 
	programsArrayRef => { required => 1, defined => 1, default => [], strict_type => 1, store => \$programsArrayRef},
	verbose => { strict_type => 1, store => \$verbose},
    };
        
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];
 
    foreach my $program (@{${programsArrayRef}}) {

	&CheckCommandinPath({program => $program,
		    });

	my $cmdsArrayRef = [$program, "--version"];

	my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
	    run( command => $cmdsArrayRef, verbose => $verbose );

	if ($error_message) {
	    
	    my $ret = join(" ", @$stderr_buf);
	   
	    if( $ret =~/(Version:\s+\S+)/) {
		
		$logger->info($program." ".$1, "\n");
	    }
	    if( $ret =~/(\d+.\d+.\d+)/) {  #Find version - assume semantic versioning major.minor.patch
		
		$logger->info("Version: ".$program." ".$1, "\n");
	    }
	}
	if($success) {
	    
	    my $ret = join(" ", @$full_buf);

	    if( $ret =~/(\d+.\d+.\d+)/) {  #Find version - assume semantic versioning major.minor.patch
		
		$logger->info("Version: ". $program." ".$1, "\n");
	    }
	}
    }
}


sub CheckCommandinPath {

##CheckCommandinPath

##Function : Checking command in your path and executable
##Returns  : ""
##Arguments: $program
##         : $program => Program to check

    my ($argHashRef) = @_;

    ## Flatten argument(s)
    my $program;

    my $tmpl = { 
	program => { required => 1, defined => 1, strict_type => 1, store => \$program},
    };
        
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    if(can_run($program)) {  #IPC::Cmd
	
	$logger->info("ProgramCheck: ".$program." installed\n");
    }
    else {
	
	$logger->fatal("Could not detect ".$program." in your Path\n");
	exit 1;
    }
}


sub LoadYAML {
 
##LoadYAML
    
##Function : Loads a YAML file into an arbitrary hash and returns it. Note: Currently only supports hashreferences and hashes and no mixed entries.
##Returns  : %yamlHash
##Arguments: $yamlFile
##         : $yamlFile => The yaml file to load

    my ($argHashRef) = @_;

    ##Flatten argument(s)
    my $yamlFile;

    my $tmpl = { 
	yamlFile => { required => 1, defined => 1, strict_type => 1, store => \$yamlFile},
    };

    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    my %yamlHash;

    open (my $YAML, "<", $yamlFile) or $logger->logdie("Can't open '".$yamlFile."':".$!, "\n");
    local $YAML::QuoteNumericStrings = 1;  #Force numeric values to strings in YAML representation
    %yamlHash = %{ YAML::LoadFile($yamlFile) };  #Load hashreference as hash
        
    close($YAML);

    $logger->info("Read YAML file: ".$yamlFile,"\n");
    return %yamlHash;
}


sub SearchGrid {
	
##SearchGrid
	
##Function : Search grid for input samples and add to paths
##Returns  : ""
##Arguments: $gridHashRef, sampleIDsArrayRef
##         : $gridHashRef       => Grid to search
##         : $sampleIDsArrayRef => Cohort from cmd

    my ($argHashRef) = @_;
    
    ## Flatten argument(s)
    my $gridHashRef;
    my $sampleIDsArrayRef;

    my $tmpl = { 
	gridHashRef => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$gridHashRef},
	sampleIDsArrayRef => { required => 1, defined => 1, default => [], strict_type => 1, store => \$sampleIDsArrayRef},
    };

    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    my @foundSampleIDs;
    my @paths;  #Temporary array for paths
    my %path;  #Collection of paths to analyse

    foreach my $sampleID (@sampleIDs) {

	if(${$gridHashRef}{$sampleID}) {

	    my $pathRef = \${$gridHashRef}{$sampleID};  #Alias
	    push(@{$path{$$pathRef}}, $sampleID);  #Collapse sampleIDs per path
	    push(@foundSampleIDs, $sampleID);
	}
	else {

	    $logger->fatal("Could not detect sampleID: ".$sampleID." in grid", "\n");
	    exit 1;
	}
    }
    if (@foundSampleIDs) {

	$logger->info("Found sampleID(s): ".join(", ", @foundSampleIDs)." in grid", "\n");
    }
    return %path;
}


sub BcfToolsViewCmd {
    
##BcfToolsViewCmd
    
##Function : Generate command line instructions for bcfTools view
##Returns  : ""
##Arguments: $pathHashRef, $positionsArrayRef, $outDataDir, $exclude, $include, $genoType, $outputType, $outfileEnding
##         : $pathHashRef       => PathHashRef
##         : $positionsArrayRef => Positions to analyse
##         : $outDataDir        => Outdata directory
##         : $exclude           => Filters to exclude variant
##         : $include           => Filters to include variant
##         : $genoType          => Require or exclude genotype
##         : $outputType        => The output data type
##         : $outfileEnding     => Outfile ending depending on outputType
    
    my ($argHashRef) = @_;
    
    ## Flatten argument(s)
    my $pathHashRef;
    my $positionsArrayRef;
    my $outDataDir;
    my $exclude;
    my $include;
    my $genoType;
    my $outputType;
    my $outfileEnding;
    
    my $tmpl = { 
	pathHashRef => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$pathHashRef},
	positionsArrayRef => { required => 1, defined => 1, default => [], strict_type => 1, store => \$positionsArrayRef},
	outDataDir => { required => 1, defined => 1, strict_type => 1, store => \$outDataDir},
	exclude => { strict_type => 1, store => \$exclude},
	include => { strict_type => 1, store => \$include},
	genoType => { strict_type => 1, store => \$genoType},
	outputType => { strict_type => 1, store => \$outputType},
	outfileEnding => { strict_type => 1, store => \$outfileEnding},
    };
    
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];
    
    foreach my $path (keys %{$pathHashRef}) {
	
	
	## Create array ref for cmd
	my $cmdsArrayRef;
	my $cmd;	
	
	push (@{$cmdsArrayRef}, "bcftools");
	push (@{$cmdsArrayRef}, "view");
	
	### Filters

	## Samples
	push (@{$cmdsArrayRef}, "--samples");
	push (@{$cmdsArrayRef}, join(",", @{${$pathHashRef}{$path}}) );

	## GenoType
	if ($genoType) {

	    push (@{$cmdsArrayRef}, "--genotype");
	    push (@{$cmdsArrayRef}, $genoType);
	}

	## VCF data
	if($exclude) {
	    
	    my @filter = split(/\s+/, $exclude);  #Seperate for whitespace to enable correct encoding to run
	    
	    push (@{$cmdsArrayRef}, "-e", "'",);
	    
	    foreach my $filterElement (@filter) {
		
		push (@{$cmdsArrayRef}, $filterElement);
	    }
	    push (@{$cmdsArrayRef}, "'",);
	}
	elsif ($include) {
	    
	    my @filter = split(/\s+/, $include);  #Seperate for whitespace to enable correct encoding to run
	    
	    push (@{$cmdsArrayRef}, "-i", "'",);
	    
	    foreach my $filterElement (@filter) {
		
		push (@{$cmdsArrayRef}, $filterElement);
	    }
	    push (@{$cmdsArrayRef}, "'",);
	}
	
	## Position(s)
	if (@{$positionsArrayRef}) {	
	    
	    push(@{$cmdsArrayRef}, "--regions");
	    push (@{$cmdsArrayRef}, join(",", @{$positionsArrayRef}));
	}
	
	## OutputType
	if($outputType) {
	    
	    push(@{$cmdsArrayRef}, "--output-type");
	    push(@{$cmdsArrayRef}, $outputType);
	}

	## Add vcf path
	push (@{$cmdsArrayRef}, $path);
	
	push (@{$cmdsArrayRef}, ">", catfile($outDataDir, join("_", @{${$pathHashRef}{$path}}).$outfileEnding));  #Outdata
	
	## Index
	push (@{$cmdsArrayRef}, ";");
	push (@{$cmdsArrayRef}, "bcftools");
	push (@{$cmdsArrayRef}, "index");
	push (@{$cmdsArrayRef}, catfile($outDataDir, join("_", @{${$pathHashRef}{$path}}).$outfileEnding));
	
	$cmd{$path} = $cmdsArrayRef;
    }
    return %cmd;
}


sub BcfToolsMergeCmd {
    
##BcfToolsMergeCmd
    
##Function : Generate command line instructions for bcfTools merge
##Returns  : ""
##Arguments: $pathHashRef, $outDataDir, $outputType, $outfileEnding
##         : $pathHashRef   => PathHashRef
##         : $outDataDir    => Outdata directory
##         : $outputType    => The output data type
##         : $outfileEnding => Outfile ending depending on outputType
    
    my ($argHashRef) = @_;
    
    ## Flatten argument(s)
    my $pathHashRef;
    my $outDataDir;
    my $outputType;
    my $outfileEnding;

    my $tmpl = { 
	pathHashRef => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$pathHashRef},
	outDataDir => { required => 1, defined => 1, strict_type => 1, store => \$outDataDir},
	outputType => { strict_type => 1, store => \$outputType},
	outfileEnding => { strict_type => 1, store => \$outfileEnding},
    };
    
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];
    
    ## Create array ref for cmd
    my $cmdsArrayRef;
    
    ## Merge files	
    push (@{$cmdsArrayRef}, "bcftools");
    push (@{$cmdsArrayRef}, "merge");

    ## OutputType
    if($outputType) {
	
	push(@{$cmdsArrayRef}, "--output-type");
	push(@{$cmdsArrayRef}, $outputType);
    }

    ## Add outdata paths
    foreach my $path (keys %{$pathHashRef}) {
	
	push (@{$cmdsArrayRef}, catfile($outDataDir, join("_", @{${$pathHashRef}{$path}}).$outfileEnding));
    }
    
    push (@{$cmdsArrayRef}, ">", catfile($outDataDir, "gc_merged".$outfileEnding));  #Outdata
    
    return @{$cmdsArrayRef};
}


sub SubmitCommand {

##SubmitCommand
    
##Function : Submit command via SHELL or SBATCH
##Returns  : ""
##Arguments: $pathHashRef, $cmdHashRef, $sbatchParameterHashRef, $mergecmdsArrayRef, $sourceEnvironmentCommandArrayRef, $email, $projectID, $nrofCores, $slurmQualityofService, verbose, $environment
##         : $pathHashRef                      => PathHashRef {REF}
##         : $cmdHashRef                       => Command to execute {REF}
##         : $sbatchParameterHashRef           => The sbatch parameter hash {REF}
##         : $mergecmdsArrayRef                => Merge command {REF}
##         : $sourceEnvironmentCommandArrayRef => Source environment command {REF}
##         : $verbose                          => Verbose
##         : $environment                      => Shell or SBATCH

    my ($argHashRef) = @_;

    ## Default(s)
    my $environment = ${$argHashRef}{environment} //= "SHELL";

    ## Flatten argument(s)
    my $pathHashRef;
    my $cmdHashRef;
    my $sbatchParameterHashRef;
    my $mergecmdsArrayRef;
    my $sourceEnvironmentCommandArrayRef;
    my $verbose;
    
    my $tmpl = { 
	pathHashRef => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$pathHashRef},
	cmdHashRef => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$cmdHashRef},
	sbatchParameterHashRef => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$sbatchParameterHashRef},
	mergecmdsArrayRef => { required => 1, defined => 1, default => [], strict_type => 1, store => \$mergecmdsArrayRef},
	sourceEnvironmentCommandArrayRef => { default => [],
					      strict_type => 1, store => \$sourceEnvironmentCommandArrayRef},
	environment => { required => 1, defined => 1, strict_type => 1, store => \$environment},
	verbose => { strict_type => 1, store => \$verbose},
    };
    
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];
	
    if ($environment =~/SHELL/i) {

	$logger->info("Executing grid search", "\n");

	foreach my $cmd (keys %{$cmdHashRef}) {
	    
	    if(@sourceEnvironmentCommand) {
		
		## Add source environment command 
		unshift(@{$cmd{$cmd}}, @{$sourceEnvironmentCommandArrayRef});
	    }
	    my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
		run( command => $cmd{$cmd}, verbose => $verbose );
	}
	## Bcftools merge
	if (keys %{$cmdHashRef} > 1) {

	    $logger->info("Merging grid search results", "\n");

	    if(@sourceEnvironmentCommand) {
		
		## Add source environment command 
		unshift(@{$mergecmdsArrayRef}, @{$sourceEnvironmentCommandArrayRef});
	    }
	    my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
		run( command => $mergecmdsArrayRef, verbose => $verbose );
	}
    }
    if ($environment =~/SBATCH/i) {
	
	my $FILEHANDLE = IO::Handle->new();  #Create anonymous filehandle
	my $XARGSFILEHANDLE = IO::Handle->new();  #Create anonymous filehandle
	
	my $fileName = &ProgramPreRequisites({sourceEnvironmentCommandArrayRef => $sourceEnvironmentCommandArrayRef,
					      dateTimeStampRef => \$dateTimeStamp,
					      FILEHANDLE => $FILEHANDLE,
					      projectID => ${$sbatchParameterHashRef}{projectID},
					      email => ${$sbatchParameterHashRef}{email},
					      nrofCores => ${$sbatchParameterHashRef}{maximumCores},
					      slurmQualityofService => ${$sbatchParameterHashRef}{slurmQualityofService},
					     });
	
	if(@sourceEnvironmentCommand) {
	    
	    ## Add source environment command 
	    say $FILEHANDLE join(" ", @{$sourceEnvironmentCommandArrayRef}), "\n";
	}
	
	###Method of parallelization

	## Use standard bash print/wait statements
	if (! ${$sbatchParameterHashRef}{xargs}) {

	    my $coreCounter = 1;
	    my $cmdCounter = 0;
	    foreach my $cmd (keys %{$cmdHashRef}) {
		
		## Calculates when to prints "wait" statement and prints "wait" to supplied FILEHANDLE when adequate.
		&PrintWait({counterRef => \$cmdCounter,
			    nrCoresRef => \${$sbatchParameterHashRef}{maximumCores},
			    coreCounterRef => \$coreCounter,
			    FILEHANDLE => $FILEHANDLE,
			   });
		
		map { print $FILEHANDLE $_." "} (@{$cmd{$cmd}});  #Write command instruction to sbatch
		say $FILEHANDLE "& \n";
		$cmdCounter++;
	    }
	    say $FILEHANDLE "wait", "\n";

	    if (keys %{$cmdHashRef} > 1) {
		
		##BcfTools merge
		map { print $FILEHANDLE $_." "} (@{$mergecmdsArrayRef});  #Write command instruction to sbatch
	    }
	}
	else {  # Use Xargs 
	    
	    ## Create file commands for xargs
	    my ($xargsFileCounter, $xargsFileName) = &XargsCommand({FILEHANDLE => $FILEHANDLE,
								    XARGSFILEHANDLE => $XARGSFILEHANDLE, 
								    fileName => $fileName,
								    nrCores => ${$sbatchParameterHashRef}{maximumCores},
								    firstCommand => "bcftools",
								    verbose => $verbose,
								   });
	    
	    foreach my $cmd (keys %{$cmdHashRef}) {
		
		my @escapeCharacters = qw(' ");

		## Add escape characters for xargs shell expansion
		for(my $elementCounter=0;$elementCounter<scalar(@{$cmd{$cmd}});$elementCounter++) {

		    foreach my $escapeCharacter (@escapeCharacters) {

			$cmd{$cmd}[$elementCounter] =~ s/$escapeCharacter/\\$escapeCharacter/g;
		    }
		}
		shift(@{$cmd{$cmd}});  #Since this command is handled in the xargs
		map { print $XARGSFILEHANDLE $_." "} (@{$cmd{$cmd}});  #Write command instruction to sbatch
		print $XARGSFILEHANDLE "\n";
	    }
	    if (keys %{$cmdHashRef} > 1) {
		
		## BcfTools merge
		map { print $FILEHANDLE $_." "} (@{$mergecmdsArrayRef});  #Write command instruction to sbatch
	    }
	}
	my @slurmSubmit = ["sbatch", $fileName]; 
	my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
	    run( command => @slurmSubmit, verbose => $verbose );

	$logger->info(join(" ", @{$stdout_buf}));
    }
}



sub ProgramPreRequisites {

##ProgramPreRequisites
    
##Function : Creates program directories (info & programData & programScript), program script filenames and writes sbatch header.
##Returns  : Path to stdout
##Arguments: $sourceEnvironmentCommandArrayRef, $FILEHANDLE, $projectID, $email, $dateTimeStampRef, $emailType, $slurmQualityofService, $nrofCores, $processTime, $pipefail, $errorTrap
##         : $sourceEnvironmentCommandArrayRef => Source environment command {REF}
##         : $FILEHANDLE                       => FILEHANDLE to write to
##         : $projectID                        => Sbatch projectID
##         : $email                            => Send email from sbatch 
##         : $dateTimeStampRef                 => The date and time {REF}
##         : $emailType                        => The email type
##         : $slurmQualityofService            => SLURM quality of service priority {Optional}
##         : $nrofCores                        => The number of cores to allocate {Optional}
##         : $processTime                      => Allowed process time (Hours) {Optional}
##         : $errorTrap                        => Error trap switch {Optional}
##         : $pipefail                         => Pipe fail switch {Optional}
 
    my ($argHashRef) = @_;

    ## Default(s)
    my $emailType = ${$argHashRef}{emailType} //= "F";
    my $slurmQualityofService = ${$argHashRef}{slurmQualityofService} //= "low";
    my $nrofCores = ${$argHashRef}{nrofCores} //= 16;
    my $processTime;
    my $pipefail;
    my $errorTrap;

    ## Flatten argument(s)
    my $sourceEnvironmentCommandArrayRef;
    my $FILEHANDLE;
    my $projectID;
    my $email;
    my $dateTimeStampRef;

    my $tmpl = { 
	sourceEnvironmentCommandArrayRef => { default => [],
					      strict_type => 1, store => \$sourceEnvironmentCommandArrayRef},
	FILEHANDLE => { required => 1, defined => 1, store => \$FILEHANDLE},
	projectID => { required => 1, defined => 1, strict_type => 1, store => \$projectID},
	email => { strict_type => 1, store => \$email},
	dateTimeStampRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$dateTimeStampRef},
	emailType => { allow => ["B", "F", "E"],
		       strict_type => 1, store => \$emailType},
	slurmQualityofService => { default => "low",
				   allow => ["low", "high", "normal"],
				   strict_type => 1, store => \$slurmQualityofService},
	nrofCores => { allow => qr/^\d+$/,
		       strict_type => 1, store => \$nrofCores},
	processTime => { default => 1,
			 allow => qr/^\d+$/,
			 strict_type => 1, store => \$processTime},
	pipefail => { default => 1,
		      allow => [0, 1],
		      strict_type => 1, store => \$pipefail},
	errorTrap  => { default => 1,
			allow => [0, 1],
			strict_type => 1, store => \$errorTrap},
    };
        
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    ### Sbatch script names and directory creation
    
    my $outDataDir = catfile(cwd(), "gc_analysis");
    my $outScriptDir = catfile(cwd(), "gc_analysis", "scripts");

    my $programName = "grid-crawler";
    my $fileNameEnd = ".sh";
    my $fileNameStub = $$dateTimeStampRef;  #The sbatch script
    my $fileNamePath = catfile($outScriptDir, $programName."_".$fileNameStub.$fileNameEnd);
    my $fileInfoPath = catfile($outDataDir, "info", $programName."_".$fileNameStub);

    ## Create directories
    make_path(catfile($outDataDir, "info"),  #Creates the outdata directory folder and info data file directory
	      catfile($outScriptDir),  #Creates the folder script file directory
	);

    ###Info and Log
    $logger->info("Creating sbatch script for ".$programName." and writing script file(s) to: ".$fileNamePath."\n");
    $logger->info("Sbatch script ".$programName." data files will be written to: ".$outDataDir."\n");

    ###Sbatch header
    open ($FILEHANDLE, ">",$fileNamePath) or $logger->logdie("Can't write to '".$fileNamePath."' :".$!."\n");
    
    say $FILEHANDLE "#! /bin/bash -l";

    if ($pipefail) {

	say $FILEHANDLE "set -o pipefail";  #Detect errors within pipes 
    }
    say $FILEHANDLE "#SBATCH -A ".$projectID;
    say $FILEHANDLE "#SBATCH -n ".$nrofCores;
    say $FILEHANDLE "#SBATCH -t ".$processTime.":00:00";
    say $FILEHANDLE "#SBATCH --qos=".$slurmQualityofService;
    say $FILEHANDLE "#SBATCH -J ".$programName;
    say $FILEHANDLE "#SBATCH -e ".catfile($fileInfoPath.".stderr.txt");
    say $FILEHANDLE "#SBATCH -o ".catfile($fileInfoPath.".stdout.txt");
    
    if ($email) {
	
	if ($emailType =~/B/i) {

	    say $FILEHANDLE "#SBATCH --mail-type=BEGIN";
	}
	if ($emailType =~/E/i) {
	 
	    say $FILEHANDLE "#SBATCH --mail-type=END";
	}
	if ($emailType =~/F/i) {
	    
	    say $FILEHANDLE "#SBATCH --mail-type=FAIL";
	}
	say $FILEHANDLE "#SBATCH --mail-user=".$email, "\n";	
    }
    
    say $FILEHANDLE q?echo "Running on: $(hostname)"?;
    say $FILEHANDLE q?PROGNAME=$(basename $0)?,"\n";

    if (@{$sourceEnvironmentCommandArrayRef}) {

	say $FILEHANDLE "##Activate environment";
	say $FILEHANDLE join(' ', @{$sourceEnvironmentCommandArrayRef}), "\n";
    }

    if ($errorTrap) {

	## Create error handling function and trap
	say $FILEHANDLE q?error() {?, "\n";
	say $FILEHANDLE "\t".q?## Display error message and exit?;
	say $FILEHANDLE "\t".q{ret="$?"};
	say $FILEHANDLE "\t".q?echo "${PROGNAME}: ${1:-"Unknown Error - ExitCode="$ret}" 1>&2?, "\n";
	say $FILEHANDLE "\t".q?exit 1?;
	say $FILEHANDLE q?}?;
	say $FILEHANDLE q?trap error ERR?, "\n";
    }
    return $fileNamePath;
}


sub CheckEmailAddress { 
    
##CheckEmailAddress
    
##Function : Check the syntax of the email adress is valid not that it is actually exists.  
##Returns  : ""
##Arguments: $emailRef
##         : $emailRef => The email adress
    
    my ($argHashRef) = @_;
    
    ## Flatten argument(s)
    my $emailRef;
    
    my $tmpl = { 
	emailRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$emailRef},
    };
        
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    $$emailRef =~ /[ |\t|\r|\n]*\"?([^\"]+\"?@[^ <>\t]+\.[^ <>\t][^ <>\t]+)[ |\t|\r|\n]*/;

    unless (defined($1)) {
	
	$logger->fatal("The supplied email: ".$$emailRef." seem to be malformed. ", "\n");
	exit 1;
    }
}

sub PrintWait {

##PrintWait
    
##Function : Calculates when to prints "wait" statement and prints "wait" to supplied FILEHANDLE when adequate. 
##Returns  : Incremented $$coreCounterRef
##Arguments: $counterRef, $nrCoresRef, $coreCounterRef
##         : $counterRef     => The number of used cores {REF}
##         : $nrCoresRef     => The maximum number of cores to be use before printing "wait" statement {REF}
##         : $coreCounterRef => Scales the number of $nrCoresRef cores used after each print "wait" statement {REF}
##         : $FILEHANDLE     => FILEHANDLE to print "wait" statment to
    
    my ($argHashRef) = @_;

    ## Flatten argument(s)
    my $counterRef;
    my $nrCoresRef;
    my $coreCounterRef;
    my $FILEHANDLE;
   
    my $tmpl = { 
	counterRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$counterRef},
	nrCoresRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$nrCoresRef},
	coreCounterRef => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$coreCounterRef},
	FILEHANDLE => { required => 1, defined => 1, store => \$FILEHANDLE},
    };
        
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    if ($$counterRef == $$coreCounterRef * $$nrCoresRef) {  #Using only nr of cores eq to lanes or maximumCores
	
	say $FILEHANDLE "wait", "\n";
	$$coreCounterRef=$$coreCounterRef+1;  #Increase the maximum number of cores allowed to be used since "wait" was just printed 
    }    
}


sub XargsCommand {

##XargsCommand
    
##Function : Creates the command line for xargs. Writes to sbatch FILEHANDLE and opens xargs FILEHANDLE
##Returns  : "xargsFileCounter + 1, $xargsFileName"
##Arguments: $FILEHANDLE, $XARGSFILEHANDLE, $fileName, $nrCores, $firstCommand, $programInfoPath, $memoryAllocation, $javaUseLargePagesRef, $javaTemporaryDirectory, $javaJar, $xargsFileCounter, $verbose
##         : $FILEHANDLE             => Sbatch filehandle to write to
##         : $XARGSFILEHANDLE        => XARGS filehandle to write to 
##         : $fileName               => File name
##         : $nrCores                => The number of cores to use
##         : $firstCommand           => The inital command 
##         : $programInfoPath        => The program info path
##         : $memoryAllocation       => Memory allocation for java
##         : $javaUseLargePagesRef   => Use java large pages {REF}
##         : $javaTemporaryDirectory => Redirect tmp files to java temp {Optional}
##         : $javaJar                => The JAR
##         : $xargsFileCounter       => The xargs file counter
##         : $verbose                => Verbose

    my ($argHashRef) = @_;

    ## Default(s)
    my $xargsFileCounter;

    ## Flatten argument(s)
    my $FILEHANDLE;
    my $XARGSFILEHANDLE;
    my $fileName;
    my $nrCores;
    my $firstCommand;
    my $programInfoPath;
    my $memoryAllocation;
    my $javaUseLargePagesRef;
    my $javaTemporaryDirectory;
    my $javaJar;
    my $verbose;

    my $tmpl = { 
	FILEHANDLE => { required => 1, defined => 1, store => \$FILEHANDLE},
	XARGSFILEHANDLE => { required => 1, defined => 1, store => \$XARGSFILEHANDLE},
	fileName => { required => 1, defined => 1, strict_type => 1, store => \$fileName},
	nrCores => { required => 1, defined => 1, strict_type => 1, store => \$nrCores},
	firstCommand => { required => 1, defined => 1, strict_type => 1, store => \$firstCommand},
	programInfoPath => { strict_type => 1, store => \$programInfoPath},
	memoryAllocation => { strict_type => 1, store => \$memoryAllocation},
	javaUseLargePagesRef => { default => \$$, strict_type => 1, store => \$javaUseLargePagesRef},
	javaTemporaryDirectory => { strict_type => 1, store => \$javaTemporaryDirectory},
	javaJar => { strict_type => 1, store => \$javaJar},
	xargsFileCounter => { default => 0,
			      allow => qr/^\d+$/,
			      strict_type => 1, store => \$xargsFileCounter},
	verbose => { strict_type => 1, store => \$verbose},
    };
        
    check($tmpl, $argHashRef, 1) or die qw[Could not parse arguments!];

    my $xargsFileName;

    ##Check if there is a xargsFileName to concatenate
    if (defined($programInfoPath)) {

	$xargsFileName = $programInfoPath.".".$xargsFileCounter;
    }

    print $FILEHANDLE "cat ".$fileName.".".$xargsFileCounter.".xargs ";  #Read xargs command file
    print $FILEHANDLE "| ";  #Pipe
    print $FILEHANDLE "xargs ";
    print $FILEHANDLE "-i ";  #replace-str; Enables us to tell xargs where to put the command file lines
    if ($verbose) {

	print $FILEHANDLE "--verbose ";  #Print the command line on the standard error output before executing it
    }
    print $FILEHANDLE "-n1 ";  #Use at most max-args arguments per command line
    print $FILEHANDLE q?-P?.$nrCores.q? ?;  #Run up to max-procs processes at a time
    print $FILEHANDLE q?sh -c "?;  #The string following this command will be interpreted as a shell command

    print $FILEHANDLE $firstCommand." ";

    say $FILEHANDLE q? {} "?, "\n";  #Set placeholder
    open ($XARGSFILEHANDLE, ">",$fileName.".".$xargsFileCounter.".xargs") or $logger->logdie("Can't write to '".$fileName.".".$xargsFileCounter.".xargs"."' :".$!."\n\n");  #Open XARGSFILEHANDLE 
    return ( ($xargsFileCounter + 1), $xargsFileName);  #Increment to not overwrite xargs file with next call (if used) and xargsFileName stub
}


##Investigate potential autodie error
if ($@ and $@->isa("autodie::exception")) {
    
    if ($@->matches("default")) {
	
	say "Not an autodie error at all";
    }
    if ($@->matches("open")) { 

	say "Error from open";
    }
    if ($@->matches(":io" )) {

	say "Non-open, IO error.\n";
    }
}
elsif ($@) {

    say "A non-autodie exception.";
}
