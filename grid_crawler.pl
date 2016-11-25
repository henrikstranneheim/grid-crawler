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
	qq{grid_crawler.pl grid.yaml -s sample_id > outfile.vcf
           -s/--sample_ids SampleIDs to analyze
           -p/--positions Search at region (A,B:C,X:Y-Z)
           -en/--exclude Exclude sites for which the expression is true
           -i/--include Include sites for which the expression is true
           -g/--genotype Require one or more hom/het/missing genotype or, if prefixed with "^", exclude sites with hom/het/missing genotypes
           -e/environment Execute search via SHELL or SBATCH (defaults to "SHELL")
           -pro/--project_id The project ID (mandatory if using SBATCH)
           -em/--email E-mail (defaults to "")
           -mc/--core_processor_number The maximum number of cores per node used in the analysis (defaults to "16")
           -qos/--slurm_quality_of_service SLURM quality of service command in sbatch scripts (defaults to "low")
           -sen/--source_environment_commands Source environment command in sbatch scripts (defaults to "")
           -x/--xargs Use xargs in sbatch for parallel execution (defaults to "1" (=yes))
           -od/--outdata_dir The data files output directory
           -ot/--output_type The output type b: compressed BCF, z: compressed VCF (defaults to "b")
           -l/--log_file Log file (defaults to "grid-crawler-log/{date}/{scriptname}_{timestamp}.log")
           -h/--help Display this help message
           -v/--version Display version
           verb/--verbose Verbose
        };
}

my ($grid_file, $exclude, $include, $genotype, $outdata_dir, $log_file, $verbose);
my ($environment, $output_type) = ("shell", "b");

my @positions;
my @sample_ids;
my @programs = ("bcftools");
my @source_environment_commands;

##Sbatch parameters
my %sbatch_parameter;

$sbatch_parameter{xargs} = 1;

$sbatch_parameter{core_processor_number} = 16;

my $logger;  #Will hold the logger object
my $grid_crawler_version = "0.0.0";

## Add date_timestamp for later use in log
my $date_time = localtime;
my $date_time_stamp = $date_time->datetime;
my $date = $date_time->ymd;
my $script = (`basename $0`);  #Catches script name
chomp($date_time_stamp, $date, $script);  #Remove \n;

## Enables cmd to print usage help
if(!@ARGV) {

    help({USAGE => $USAGE,
	  exit_code => 0,
	 });
}
else { #Collect potential infile - otherwise read from STDIN

    $grid_file = $ARGV[0];
}

###User Options
GetOptions('s|sample_ids:s' => \@sample_ids,
	   'p|positions:s' => \@positions,
	   'e|exclude:s' => \$exclude,
	   'i|include:s' => \$include,
	   'g|genotype:s' => \$genotype,
	   'en|environment:s' => \$environment,
	   'pro|project_id:s'  => \$sbatch_parameter{project_id},
	   'mc|core_processor_number=n' => \$sbatch_parameter{core_processor_number},  #Per node
	   'em|email:s'  => \$sbatch_parameter{email},  #Email adress
	   'qos|slurm_quality_of_service=s' => \$sbatch_parameter{slurm_quality_of_service},
	   'sen|source_environment_commands=s{,}' => \@source_environment_commands,
	   'x|xargs=n' => \$sbatch_parameter{xargs},
	   'od|outdata_dir:s'  => \$outdata_dir,
	   'ot|output_type:s' => \$output_type,
	   'l|log_file:s' => \$log_file,
	   'h|help' => sub { say STDOUT $USAGE; exit;},  #Display help text
	   'v|version' => sub { say STDOUT "\ngrid_crawler.pl ".$grid_crawler_version, "\n"; exit;},  #Display version number
	   'verb|verbose' => \$verbose,
    )  or help({USAGE => $USAGE,
		exit_code => 1,
	       });

## Create log directory, default log_file path, initiate logger object
$log_file = initiate_log({log_file_ref => \$log_file,
			  script_ref => \$script,
			  date_ref => \$date,
			  date_time_stamp_ref => \$date_time_stamp,
			 });

if(! @sample_ids) {

    $logger->fatal("Please provide sample_ids");
    exit 1;
}

$logger->info("Including sample_ids(s): ".join(", ", @sample_ids), "\n");

if(@source_environment_commands) {

    if ($source_environment_commands[-1] !~ /\;$/) {

	push(@source_environment_commands, ";");
    }
}

if($environment =~/SBATCH/i) {

    ## Required sbatch parameters
    my @sbatch_parameters = ("project_id", "xargs");

    foreach my $parameter (@sbatch_parameters) {

	if ( ! $sbatch_parameter{$parameter}) {

	    $logger->fatal("Please provide ".$parameter." when submitting via sbatch");
	    exit 1;
	}
	if ($parameter eq "xargs") {

	    push(@programs, "xargs");
	}
    }

    ## Check email adress format
    if ($sbatch_parameter{email}) {  #Allow no malformed email adress

	check_email_address({email_ref => \$sbatch_parameter{email},
			    });
    }
}

## Check program(s) can run and log version if so. Otherwise exit
check_program({programs_ref => \@programs,
	       verbose => $verbose,
	      });

## Set fileending depending on output_type
my $outfile_ending .= ".bcf.gz";

if ($output_type eq "z") {

    $outfile_ending = ".vcf.gz"
}

if (! $outdata_dir) {

    $outdata_dir = catfile(cwd(), "gc_analysis", $date_time_stamp);
}


############
####MAIN####
############

## Create outdata_dir
make_path($outdata_dir);

## Loads a YAML file into an arbitrary hash and returns it.
my %grid = load_yaml({yaml_file => $grid_file,
		     });

my %path = search_grid({grid_href => \%grid,
			sample_ids_ref => \@sample_ids,
		       });

my %cmd = bcftools_view_cmd({path_href => \%path,
			     positions_ref => \@positions,
			     outdata_dir => $outdata_dir,
			     exclude => $exclude,
			     include => $include,
			     genotype => $genotype,
			     output_type => $output_type,
			     outfile_ending => $outfile_ending,
			    });

my @merge_cmds = bcftools_merge_cmd({path_href => \%path,
				     outdata_dir => $outdata_dir,
				     output_type => $output_type,
				     outfile_ending => $outfile_ending,
				    });

submit_command({path_href => \%path,
		cmd_href => \%cmd,
		sbatch_parameter_href => \%sbatch_parameter,
		merge_cmds_ref => \@merge_cmds,
		source_environment_commands_ref => \@source_environment_commands,
		environment => $environment,
		verbose => $verbose
	       });


######################
####SubRoutines#######
######################


sub help {

##help

##Function : Print help text and exit with supplied exit code
##Returns  : ""
##Arguments: $USAGE, $exit_code
##         : $USAGE     => Help text
##         : $exit_code => Exit code

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $USAGE;
    my $exit_code;

    my $tmpl = {
	USAGE => {required => 1, defined => 1, strict_type => 1, store => \$USAGE},
	exit_code => { default => 0, strict_type => 1, store => \$exit_code},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    say STDOUT $USAGE;
    exit $exit_code;
}


sub initiate_log {

##initiate_log

##Function : Create log directory, default log_file path, initiate logger object
##Returns  : ""
##Arguments: $log_file_ref, $script_ref, $date_ref, $date_time_stamp_ref
##         : $log_file_ref        => User supplied info on cmd for log_file option {REF}
##         : $script_ref          => The script that is executed {REF}
##         : $date_ref            => The date {REF}
##         : $date_time_stamp_ref => The date and time {REF}

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $log_file_ref;
    my $script_ref;
    my $date_ref;
    my $date_time_stamp_ref;

    my $tmpl = {
	log_file_ref => { default => \$$, strict_type => 1, store => \$log_file_ref},
	script_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$script_ref},
	date_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$date_ref},
	date_time_stamp_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$date_time_stamp_ref},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    if(! $$log_file_ref) {  #No input from cmd i.e. create default logging directory and set default

	## Create default log_file dir and file path
	make_path(catdir(cwd(), "gc_analysis", "gc_log", $$date_ref));

	$log_file = catfile(cwd(), "gc_analysis", "gc_log", $$date_ref, $$script_ref."_".$$date_time_stamp_ref.".log");
    }

    ## Creates log for the master script
    my $config = create_log4perl_congfig({file_path_ref => \$$log_file_ref,
					 });
    Log::Log4perl->init(\$config);
    $logger = Log::Log4perl->get_logger("gc_logger");
}


sub create_log4perl_congfig {

##create_log4perl_congfig

##Function : Create log4perl config file.
##Returns  : "$config"
##Arguments: $file_path_ref
##         : $file_path_ref => log4perl config file path {REF}

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $file_path_ref;

    my $tmpl = {
	file_path_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$file_path_ref},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    my $config = q?
        log4perl.category.gc_logger = TRACE, LogFile, ScreenApp
        log4perl.appender.LogFile = Log::Log4perl::Appender::File
        log4perl.appender.LogFile.filename = ?.$$file_path_ref.q?
        log4perl.appender.LogFile.layout=PatternLayout
        log4perl.appender.LogFile.layout.ConversionPattern = [%p] %d %c - %m%n

        log4perl.appender.ScreenApp = Log::Log4perl::Appender::Screen
        log4perl.appender.ScreenApp.layout = PatternLayout
        log4perl.appender.ScreenApp.layout.ConversionPattern = [%p] %d %c - %m%n
        ?;
    return $config;
}


sub check_program {

##check_program

##Function : Check program(s) can run and log version if so. Otherwise exit.
##Returns  : ""
##Arguments: $programs_ref, $verbose
##         : $programs_ref => Programs to check
##         : $verbose      => Verbose

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $programs_ref;
    my $verbose;

    my $tmpl = {
	programs_ref => { required => 1, defined => 1, default => [], strict_type => 1, store => \$programs_ref},
	verbose => { strict_type => 1, store => \$verbose},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    foreach my $program (@$programs_ref) {

	check_command_in_path({program => $program,
			      });

	my $cmds_ref = [$program, "--version"];

	my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
	    run( command => $cmds_ref, verbose => $verbose );

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


sub check_command_in_path {

##check_command_in_path

##Function : Checking command in your path and executable
##Returns  : ""
##Arguments: $program
##         : $program => Program to check

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $program;

    my $tmpl = {
	program => { required => 1, defined => 1, strict_type => 1, store => \$program},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    if(can_run($program)) {  #IPC::Cmd

	$logger->info("Program check: ".$program." installed\n");
    }
    else {

	$logger->fatal("Could not detect ".$program." in your Path\n");
	exit 1;
    }
}


sub load_yaml {

##load_yaml

##Function : Loads a YAML file into an arbitrary hash and returns it.
##Returns  : %yaml
##Arguments: $yaml_file
##         : $yaml_file => The yaml file to load

    my ($arg_href) = @_;

    ##Flatten argument(s)
    my $yaml_file;

    my $tmpl = {
	yaml_file => { required => 1, defined => 1, strict_type => 1, store => \$yaml_file},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    my %yaml;

    open (my $YAML, "<", $yaml_file) or $logger->logdie("Can't open '".$yaml_file."':".$!, "\n");
    local $YAML::QuoteNumericStrings = 1;  #Force numeric values to strings in YAML representation
    %yaml = %{ YAML::LoadFile($yaml_file) };  #Load hashreference as hash

    close($YAML);

    $logger->info("Read YAML file: ".$yaml_file,"\n");
    return %yaml;
}


sub search_grid {

##search_grid

##Function : Search grid for input samples and add to paths
##Returns  : ""
##Arguments: $grid_href, sample_ids_ref
##         : $grid_href      => Grid to search
##         : $sample_ids_ref => Cohort from cmd

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $grid_href;
    my $sample_ids_ref;

    my $tmpl = {
	grid_href => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$grid_href},
	sample_ids_ref => { required => 1, defined => 1, default => [], strict_type => 1, store => \$sample_ids_ref},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    my @found_sample_ids;
    my %path;  #Collection of paths to analyse

    foreach my $sample_id (@sample_ids) {

	if($grid_href->{$sample_id}) {

	    my $path_ref = \$grid_href->{$sample_id};  #Alias
	    push(@{ $path{$$path_ref} }, $sample_id);  #Collapse sample_ids per path
	    push(@found_sample_ids, $sample_id);
	}
	else {

	    $logger->fatal("Could not detect sample_id: ".$sample_id." in grid", "\n");
	    exit 1;
	}
    }
    if (@found_sample_ids) {

	$logger->info("Found sample_id(s): ".join(", ", @found_sample_ids)." in grid", "\n");
    }
    return %path;
}


sub bcftools_view_cmd {

##bcftools_view_cmd

##Function : Generate command line instructions for bcfTools view
##Returns  : ""
##Arguments: $path_href, $positions_ref, $outdata_dir, $exclude, $include, $genotype, $output_type, $outfile_ending
##         : $path_href      => PathHashRef
##         : $positions_ref  => Positions to analyse
##         : $outdata_dir    => Outdata directory
##         : $exclude        => Filters to exclude variant
##         : $include        => Filters to include variant
##         : $genotype       => Require or exclude genotype
##         : $output_type    => The output data type
##         : $outfile_ending => Outfile ending depending on output_type

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $path_href;
    my $positions_ref;
    my $outdata_dir;
    my $exclude;
    my $include;
    my $genotype;
    my $output_type;
    my $outfile_ending;

    my $tmpl = {
	path_href => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$path_href},
	positions_ref => { required => 1, defined => 1, default => [], strict_type => 1, store => \$positions_ref},
	outdata_dir => { required => 1, defined => 1, strict_type => 1, store => \$outdata_dir},
	exclude => { strict_type => 1, store => \$exclude},
	include => { strict_type => 1, store => \$include},
	genotype => { strict_type => 1, store => \$genotype},
	output_type => { strict_type => 1, store => \$output_type},
	outfile_ending => { strict_type => 1, store => \$outfile_ending},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    foreach my $path (keys %$path_href) {

	## Create array ref for cmd
	my $cmds_ref;
	my $cmd;

	push (@$cmds_ref, "bcftools");
	push (@$cmds_ref, "view");

	### Filters

	## Samples
	push (@$cmds_ref, "--samples");
	push (@$cmds_ref, join(",", @{ $path_href->{$path} }) );

	## GenoType
	if ($genotype) {

	    push (@$cmds_ref, "--genotype");
	    push (@$cmds_ref, $genotype);
	}

	## VCF data
	if($exclude) {

	    my @filter = split(/\s+/, $exclude);  #Seperate for whitespace to enable correct encoding to run

	    push (@$cmds_ref, "-e", "'",);

	    foreach my $filter_element (@filter) {

		push (@$cmds_ref, $filter_element);
	    }
	    push (@$cmds_ref, "'",);
	}
	elsif ($include) {

	    my @filter = split(/\s+/, $include);  #Seperate for whitespace to enable correct encoding to run

	    push (@$cmds_ref, "-i", "'",);

	    foreach my $filter_element (@filter) {

		push (@$cmds_ref, $filter_element);
	    }
	    push (@$cmds_ref, "'",);
	}

	## Position(s)
	if (@$positions_ref) {

	    push(@$cmds_ref, "--regions");
	    push (@$cmds_ref, join(",", @$positions_ref));
	}

	## OutputType
	if($output_type) {

	    push(@$cmds_ref, "--output-type");
	    push(@$cmds_ref, $output_type);
	}

	## Add vcf path
	push (@$cmds_ref, $path);

	push (@$cmds_ref, ">", catfile($outdata_dir, join("_", @{ $path_href->{$path} }).$outfile_ending));  #Outdata

	## Index
	push (@$cmds_ref, ";");
	push (@$cmds_ref, "bcftools");
	push (@$cmds_ref, "index");
	push (@$cmds_ref, catfile($outdata_dir, join("_", @{ $path_href->{$path} }).$outfile_ending));

	$cmd{$path} = $cmds_ref;
    }
    return %cmd;
}


sub bcftools_merge_cmd {

##bcftools_merge_cmd

##Function : Generate command line instructions for bcftools merge
##Returns  : ""
##Arguments: $path_href, $outdata_dir, $output_type, $outfile_ending
##         : $path_href      => Path hash ref
##         : $outdata_dir    => Outdata directory
##         : $output_type    => The output data type
##         : $outfile_ending => Outfile ending depending on output_type

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $path_href;
    my $outdata_dir;
    my $output_type;
    my $outfile_ending;

    my $tmpl = {
	path_href => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$path_href},
	outdata_dir => { required => 1, defined => 1, strict_type => 1, store => \$outdata_dir},
	output_type => { strict_type => 1, store => \$output_type},
	outfile_ending => { strict_type => 1, store => \$outfile_ending},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    ## Create array ref for cmd
    my $cmds_ref;

    ## Merge files
    push (@$cmds_ref, "bcftools");
    push (@$cmds_ref, "merge");

    ## OutputType
    if($output_type) {

	push(@$cmds_ref, "--output-type");
	push(@$cmds_ref, $output_type);
    }

    ## Add outdata paths
    foreach my $path (keys %$path_href) {

	push (@$cmds_ref, catfile($outdata_dir, join("_", @{ $path_href->{$path} }).$outfile_ending));
    }

    push (@$cmds_ref, ">", catfile($outdata_dir, "gc_merged".$outfile_ending));  #Outdata

    return @$cmds_ref;
}


sub submit_command {

##submit_command

##Function : Submit command via SHELL or SBATCH
##Returns  : ""
##Arguments: $path_href, $cmd_href, $sbatch_parameter_href, $merge_cmds_ref, $source_environment_commands_ref, $email, $project_id, $core_processor_number, $slurm_quality_of_service, verbose, $environment
##         : $path_href                       => PathHashRef {REF}
##         : $cmd_href                        => Command to execute {REF}
##         : $sbatch_parameter_href           => The sbatch parameter hash {REF}
##         : $merge_cmds_ref                  => Merge command {REF}
##         : $source_environment_commands_ref => Source environment command {REF}
##         : $verbose                         => Verbose
##         : $environment                     => Shell or SBATCH

    my ($arg_href) = @_;

    ## Default(s)
    my $environment = ${$arg_href}{environment} //= "SHELL";

    ## Flatten argument(s)
    my $path_href;
    my $cmd_href;
    my $sbatch_parameter_href;
    my $merge_cmds_ref;
    my $source_environment_commands_ref;
    my $verbose;

    my $tmpl = {
	path_href => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$path_href},
	cmd_href => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$cmd_href},
	sbatch_parameter_href => { required => 1, defined => 1, default => {}, strict_type => 1, store => \$sbatch_parameter_href},
	merge_cmds_ref => { required => 1, defined => 1, default => [], strict_type => 1, store => \$merge_cmds_ref},
	source_environment_commands_ref => { default => [],
					     strict_type => 1, store => \$source_environment_commands_ref},
	environment => { required => 1, defined => 1, strict_type => 1, store => \$environment},
	verbose => { strict_type => 1, store => \$verbose},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    if ($environment =~/SHELL/i) {

	$logger->info("Executing grid search", "\n");

	foreach my $cmd (keys %$cmd_href) {

	    if(@source_environment_commands) {

		## Add source environment command
		unshift(@{ $cmd{$cmd} }, @$source_environment_commands_ref);
	    }
	    my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
		run( command => $cmd{$cmd}, verbose => $verbose );
	}
	## Bcftools merge
	if (keys %$cmd_href > 1) {

	    $logger->info("Merging grid search results", "\n");

	    if(@source_environment_commands) {

		## Add source environment command
		unshift(@$merge_cmds_ref, @$source_environment_commands_ref);
	    }
	    my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
		run( command => $merge_cmds_ref, verbose => $verbose );
	}
    }
    if ($environment =~/SBATCH/i) {

	my $FILEHANDLE = IO::Handle->new();  #Create anonymous filehandle
	my $XARGSFILEHANDLE = IO::Handle->new();  #Create anonymous filehandle

	my $file_name = program_prerequisites({source_environment_commands_ref => $source_environment_commands_ref,
					       date_time_stamp_ref => \$date_time_stamp,
					       FILEHANDLE => $FILEHANDLE,
					       project_id => $sbatch_parameter_href->{project_id},
					       email => $sbatch_parameter_href->{email},
					       core_processor_number => $sbatch_parameter_href->{core_processor_number},
					       slurm_quality_of_service => $sbatch_parameter_href->{slurm_quality_of_service},
					      });

	if(@source_environment_commands) {

	    ## Add source environment command
	    say $FILEHANDLE join(" ", @$source_environment_commands_ref), "\n";
	}

	###Method of parallelization

	## Use standard bash print/wait statements
	if (! $sbatch_parameter_href->{xargs}) {

	    my $core_counter = 1;
	    my $cmd_counter = 0;
	    foreach my $cmd (keys %$cmd_href) {

		## Calculates when to prints "wait" statement and prints "wait" to supplied FILEHANDLE when adequate.
		print_wait({counter_ref => \$cmd_counter,
			    core_number_ref => \$sbatch_parameter_href->{core_processor_number},
			    core_counter_ref => \$core_counter,
			    FILEHANDLE => $FILEHANDLE,
			   });

		map { print $FILEHANDLE $_." "} (@{ $cmd{$cmd} });  #Write command instruction to sbatch
		say $FILEHANDLE "& \n";
		$cmd_counter++;
	    }
	    say $FILEHANDLE "wait", "\n";

	    if (keys %$cmd_href > 1) {

		##BcfTools merge
		map { print $FILEHANDLE $_." "} (@$merge_cmds_ref);  #Write command instruction to sbatch
	    }
	}
	else {  # Use Xargs

	    ## Create file commands for xargs
	    my ($xargs_file_counter, $xargs_file_name) = xargs_command({FILEHANDLE => $FILEHANDLE,
									XARGSFILEHANDLE => $XARGSFILEHANDLE,
									file_name => $file_name,
									core_number => $sbatch_parameter_href->{core_processor_number},
									first_command => "bcftools",
									verbose => $verbose,
								       });

	    foreach my $cmd (keys %$cmd_href) {

		my @escape_characters = qw(' ");

		## Add escape characters for xargs shell expansion
		for(my $element_counter=0;$element_counter<scalar(@{ $cmd{$cmd} });$element_counter++) {

		    foreach my $escape_character (@escape_characters) {

			$cmd{$cmd}[$element_counter] =~ s/$escape_character/\\$escape_character/g;
		    }
		}
		shift(@{ $cmd{$cmd} });  #Since this command is handled in the xargs
		map { print $XARGSFILEHANDLE $_." "} (@{ $cmd{$cmd} });  #Write command instruction to sbatch
		print $XARGSFILEHANDLE "\n";
	    }
	    if (keys %$cmd_href > 1) {

		## Bcftools merge
		map { print $FILEHANDLE $_." "} (@$merge_cmds_ref);  #Write command instruction to sbatch
	    }
	}
	my @slurm_submit = ["sbatch", $file_name];
	my( $success, $error_message, $full_buf, $stdout_buf, $stderr_buf ) =
	    run( command => @slurm_submit, verbose => $verbose );

	$logger->info(join(" ", @$stdout_buf));
    }
}



sub program_prerequisites {

##program_prerequisites

##Function : Creates program directories (info & programData & programScript), program script filenames and writes sbatch header.
##Returns  : Path to stdout
##Arguments: $source_environment_commands_ref, $FILEHANDLE, $project_id, $email, $date_time_stamp_ref, $email_type, $slurm_quality_of_service, $core_processor_number, $process_time, $pipefail, $error_trap
##         : $source_environment_commands_ref => Source environment command {REF}
##         : $FILEHANDLE                      => FILEHANDLE to write to
##         : $project_id                      => Sbatch project_id
##         : $email                           => Send email from sbatch
##         : $date_time_stamp_ref             => The date and time {REF}
##         : $email_type                       => The email type
##         : $slurm_quality_of_service        => SLURM quality of service priority {Optional}
##         : $core_processor_number           => The number of cores to allocate {Optional}
##         : $process_time                    => Allowed process time (Hours) {Optional}
##         : $error_trap                      => Error trap switch {Optional}
##         : $pipefail                        => Pipe fail switch {Optional}

    my ($arg_href) = @_;

    ## Default(s)
    my $email_type = ${$arg_href}{email_type} //= "F";
    my $slurm_quality_of_service = ${$arg_href}{slurm_quality_of_service} //= "low";
    my $core_processor_number = ${$arg_href}{core_processor_number} //= 16;
    my $process_time;
    my $pipefail;
    my $error_trap;

    ## Flatten argument(s)
    my $source_environment_commands_ref;
    my $FILEHANDLE;
    my $project_id;
    my $email;
    my $date_time_stamp_ref;

    my $tmpl = {
	source_environment_commands_ref => { default => [],
					     strict_type => 1, store => \$source_environment_commands_ref},
	FILEHANDLE => { required => 1, defined => 1, store => \$FILEHANDLE},
	project_id => { required => 1, defined => 1, strict_type => 1, store => \$project_id},
	email => { strict_type => 1, store => \$email},
	date_time_stamp_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$date_time_stamp_ref},
	email_type => { allow => ["B", "F", "E"],
			strict_type => 1, store => \$email_type},
	slurm_quality_of_service => { default => "low",
				      allow => ["low", "high", "normal"],
				      strict_type => 1, store => \$slurm_quality_of_service},
	core_processor_number => { allow => qr/^\d+$/,
				   strict_type => 1, store => \$core_processor_number},
	process_time => { default => 1,
			  allow => qr/^\d+$/,
			  strict_type => 1, store => \$process_time},
	pipefail => { default => 1,
		      allow => [0, 1],
		      strict_type => 1, store => \$pipefail},
	error_trap  => { default => 1,
			 allow => [0, 1],
			 strict_type => 1, store => \$error_trap},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    ### Sbatch script names and directory creation

    my $outdata_dir = catfile(cwd(), "gc_analysis");
    my $outscript_dir = catfile(cwd(), "gc_analysis", "scripts");

    my $program_name = "grid-crawler";
    my $file_name_end = ".sh";
    my $file_name_stub = $$date_time_stamp_ref;  #The sbatch script
    my $file_name_path = catfile($outscript_dir, $program_name."_".$file_name_stub.$file_name_end);
    my $file_info_path = catfile($outdata_dir, "info", $program_name."_".$file_name_stub);

    ## Create directories
    make_path(catfile($outdata_dir, "info"),  #Creates the outdata directory folder and info data file directory
	      catfile($outscript_dir),  #Creates the folder script file directory
	);

    ###Info and Log
    $logger->info("Creating sbatch script for ".$program_name." and writing script file(s) to: ".$file_name_path."\n");
    $logger->info("Sbatch script ".$program_name." data files will be written to: ".$outdata_dir."\n");

    ###Sbatch header
    open ($FILEHANDLE, ">",$file_name_path) or $logger->logdie("Can't write to '".$file_name_path."' :".$!."\n");

    say $FILEHANDLE "#! /bin/bash -l";

    if ($pipefail) {

	say $FILEHANDLE "set -o pipefail";  #Detect errors within pipes
    }
    say $FILEHANDLE "#SBATCH -A ".$project_id;
    say $FILEHANDLE "#SBATCH -n ".$core_processor_number;
    say $FILEHANDLE "#SBATCH -t ".$process_time.":00:00";
    say $FILEHANDLE "#SBATCH --qos=".$slurm_quality_of_service;
    say $FILEHANDLE "#SBATCH -J ".$program_name;
    say $FILEHANDLE "#SBATCH -e ".catfile($file_info_path.".stderr.txt");
    say $FILEHANDLE "#SBATCH -o ".catfile($file_info_path.".stdout.txt");

    if ($email) {

	if ($email_type =~/B/i) {

	    say $FILEHANDLE "#SBATCH --mail-type=BEGIN";
	}
	if ($email_type =~/E/i) {

	    say $FILEHANDLE "#SBATCH --mail-type=END";
	}
	if ($email_type =~/F/i) {

	    say $FILEHANDLE "#SBATCH --mail-type=FAIL";
	}
	say $FILEHANDLE "#SBATCH --mail-user=".$email, "\n";
    }

    say $FILEHANDLE q?echo "Running on: $(hostname)"?;
    say $FILEHANDLE q?PROGNAME=$(basename $0)?,"\n";

    if (@$source_environment_commands_ref) {

	say $FILEHANDLE "##Activate environment";
	say $FILEHANDLE join(' ', @$source_environment_commands_ref), "\n";
    }

    if ($error_trap) {

	## Create error handling function and trap
	say $FILEHANDLE q?error() {?, "\n";
	say $FILEHANDLE "\t".q?## Display error message and exit?;
	say $FILEHANDLE "\t".q{ret="$?"};
	say $FILEHANDLE "\t".q?echo "${PROGNAME}: ${1:-"Unknown Error - ExitCode="$ret}" 1>&2?, "\n";
	say $FILEHANDLE "\t".q?exit 1?;
	say $FILEHANDLE q?}?;
	say $FILEHANDLE q?trap error ERR?, "\n";
    }
    return $file_name_path;
}


sub check_email_address {

##check_email_address

##Function : Check the syntax of the email adress is valid not that it is actually exists.
##Returns  : ""
##Arguments: $email_ref
##         : $email_ref => The email adress

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $email_ref;

    my $tmpl = {
	email_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$email_ref},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    $$email_ref =~ /[ |\t|\r|\n]*\"?([^\"]+\"?@[^ <>\t]+\.[^ <>\t][^ <>\t]+)[ |\t|\r|\n]*/;

    unless (defined($1)) {

	$logger->fatal("The supplied email: ".$$email_ref." seem to be malformed. ", "\n");
	exit 1;
    }
}

sub print_wait {

##print_wait

##Function : Calculates when to prints "wait" statement and prints "wait" to supplied FILEHANDLE when adequate.
##Returns  : Incremented $$core_counter_ref
##Arguments: $counter_ref, $core_number_ref, $core_counter_ref
##         : $counter_ref      => The number of used cores {REF}
##         : $core_number_ref  => The maximum number of cores to be use before printing "wait" statement {REF}
##         : $core_counter_ref => Scales the number of $core_number_ref cores used after each print "wait" statement {REF}
##         : $FILEHANDLE       => FILEHANDLE to print "wait" statment to

    my ($arg_href) = @_;

    ## Flatten argument(s)
    my $counter_ref;
    my $core_number_ref;
    my $core_counter_ref;
    my $FILEHANDLE;

    my $tmpl = {
	counter_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$counter_ref},
	core_number_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$core_number_ref},
	core_counter_ref => { required => 1, defined => 1, default => \$$, strict_type => 1, store => \$core_counter_ref},
	FILEHANDLE => { required => 1, defined => 1, store => \$FILEHANDLE},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    if ($$counter_ref == $$core_counter_ref * $$core_number_ref) {  #Using only nr of cores eq to lanes or core_processor_number

	say $FILEHANDLE "wait", "\n";
	$$core_counter_ref=$$core_counter_ref+1;  #Increase the maximum number of cores allowed to be used since "wait" was just printed
    }
}


sub xargs_command {

##xargs_command

##Function : Creates the command line for xargs. Writes to sbatch FILEHANDLE and opens xargs FILEHANDLE
##Returns  : "xargs_file_counter + 1, $xargs_file_name"
##Arguments: $FILEHANDLE, $XARGSFILEHANDLE, $file_name, $core_number, $first_command, $program_info_path, $memory_allocation, $java_use_large_pages_ref, $java_temporary_dir, $java_jar, $xargs_file_counter, $verbose
##         : $FILEHANDLE               => Sbatch filehandle to write to
##         : $XARGSFILEHANDLE          => XARGS filehandle to write to
##         : $file_name                => File name
##         : $core_number              => The number of cores to use
##         : $first_command            => The inital command
##         : $program_info_path        => The program info path
##         : $memory_allocation        => Memory allocation for java
##         : $java_use_large_pages_ref => Use java large pages {REF}
##         : $java_temporary_dir       => Redirect tmp files to java temp {Optional}
##         : $java_jar                 => The JAR
##         : $xargs_file_counter       => The xargs file counter
##         : $verbose                  => Verbose

    my ($arg_href) = @_;

    ## Default(s)
    my $xargs_file_counter;

    ## Flatten argument(s)
    my $FILEHANDLE;
    my $XARGSFILEHANDLE;
    my $file_name;
    my $core_number;
    my $first_command;
    my $program_info_path;
    my $memory_allocation;
    my $java_use_large_pages_ref;
    my $java_temporary_dir;
    my $java_jar;
    my $verbose;

    my $tmpl = {
	FILEHANDLE => { required => 1, defined => 1, store => \$FILEHANDLE},
	XARGSFILEHANDLE => { required => 1, defined => 1, store => \$XARGSFILEHANDLE},
	file_name => { required => 1, defined => 1, strict_type => 1, store => \$file_name},
	core_number => { required => 1, defined => 1, strict_type => 1, store => \$core_number},
	first_command => { required => 1, defined => 1, strict_type => 1, store => \$first_command},
	program_info_path => { strict_type => 1, store => \$program_info_path},
	memory_allocation => { strict_type => 1, store => \$memory_allocation},
	java_use_large_pages_ref => { default => \$$, strict_type => 1, store => \$java_use_large_pages_ref},
	java_temporary_dir => { strict_type => 1, store => \$java_temporary_dir},
	java_jar => { strict_type => 1, store => \$java_jar},
	xargs_file_counter => { default => 0,
				allow => qr/^\d+$/,
				strict_type => 1, store => \$xargs_file_counter},
	verbose => { strict_type => 1, store => \$verbose},
    };

    check($tmpl, $arg_href, 1) or die qw[Could not parse arguments!];

    my $xargs_file_name;

    ##Check if there is a xargs_file_name to concatenate
    if (defined($program_info_path)) {

	$xargs_file_name = $program_info_path.".".$xargs_file_counter;
    }

    print $FILEHANDLE "cat ".$file_name.".".$xargs_file_counter.".xargs ";  #Read xargs command file
    print $FILEHANDLE "| ";  #Pipe
    print $FILEHANDLE "xargs ";
    print $FILEHANDLE "-i ";  #replace-str; Enables us to tell xargs where to put the command file lines

    if ($verbose) {

	print $FILEHANDLE "--verbose ";  #Print the command line on the standard error output before executing it
    }
    print $FILEHANDLE "-n1 ";  #Use at most max-args arguments per command line
    print $FILEHANDLE q?-P?.$core_number.q? ?;  #Run up to max-procs processes at a time
    print $FILEHANDLE q?sh -c "?;  #The string following this command will be interpreted as a shell command

    print $FILEHANDLE $first_command." ";

    say $FILEHANDLE q? {} "?, "\n";  #Set placeholder
    open ($XARGSFILEHANDLE, ">",$file_name.".".$xargs_file_counter.".xargs") or $logger->logdie("Can't write to '".$file_name.".".$xargs_file_counter.".xargs"."' :".$!."\n\n");  #Open XARGSFILEHANDLE
    return ( ($xargs_file_counter + 1), $xargs_file_name);  #Increment to not overwrite xargs file with next call (if used) and xargs_file_name stub
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
