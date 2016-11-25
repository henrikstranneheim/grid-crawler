#!/usr/bin/env perl

###Will test perl modules and selected funtions.

###Copyright 2016 Henrik Stranneheim

use v5.18;  #Require at least perl 5.18


BEGIN {

    my @modules = ("Modern::Perl",
		   "warnings",
		   "autodie",
		   "utf8",
		   "open",
		   "charnames",
		   "Params::Check",
		   "Time::Piece",
		   "Cwd",
		   "File::Path",
		   "File::Spec::Functions",
		   "List::Util",
		   "IPC::Cmd",
		   "YAML",
		   "Log::Log4perl",
	);

    ## Evaluate that all modules required are installed
    eval_modules(\@modules);

    sub eval_modules {

	##eval_modules

	##Function : Evaluate that all modules required are installed
	##Returns  : ""
	##Arguments: $modules_ref
	##         : $modules_ref => Array of module names

	my $modules_ref = $_[0];

	foreach my $module (@$modules_ref) {

	    $module =~s/::/\//g;  #Replace "::" with "/" since the automatic replacement magic only occurs for barewords.
	    $module .= ".pm";  #Add perl module ending for the same reason

	    eval {

		require $module;
	    };
	    if($@) {

		warn("NOTE: ".$module." not installed - Please install to run MIP.\n");
		warn("NOTE: Aborting!\n");
		exit 1;
	    }
	}
    }
}


use Modern::Perl '2014';
use warnings qw( FATAL utf8 );
use autodie;
use utf8;  #Allow unicode characters in this script
use open qw( :encoding(UTF-8) :std );
use charnames qw( :full :short );

use Test::More;
use Getopt::Long;
use FindBin qw($Bin); #Find directory of script
use File::Basename qw(dirname);
use File::Spec::Functions qw(catdir catfile);
use Params::Check qw[check allow last_error];
$Params::Check::PRESERVE_CASE = 1;  #Do not convert to lower case

##Cpan
use YAML;

our $USAGE;

BEGIN {

    $USAGE =
	qq{test.t
           -h/--help Display this help message
           -v/--version Display version
        };
}

my $test_version = "0.0.0";

###User Options
GetOptions('h|help' => sub { print STDOUT $USAGE, "\n"; exit;},  #Display help text
	   'v|version' => sub { print STDOUT "\ntest.t ".$test_version, "\n\n"; exit;},  #Display version number
    );


############
####MAIN####
############

## Test perl modules and functions
test_modules();

say "\nTesting grid file\n";

use YAML;
$YAML::QuoteNumericStrings = 1;  #Force numeric values to strings in YAML representationxs
my $yaml_file = catdir(dirname($Bin), "templates", "grid_test_file.yaml");

## Loads a YAML file into an arbitrary hash and returns it.
my %grid = load_yaml({yaml_file => $yaml_file,
		     });
ok(%grid, "Loaded grid file");
ok(keys %grid == 4, "Check load of correct number of sampleIDs (=3)");
ok(values %grid == 4, "Check load of correct number of paths (=3)");

foreach my $path (keys %grid) {

    ok(-f catfile(dirname($Bin), "templates", $grid{$path}), "Check existance of test files")
}

done_testing();   # reached the end safely


######################
####SubRoutines#######
######################

sub test_modules {

##test_modules

##Function : Test perl modules and functions
##Returns  : ""
##Arguments:
##         :

    print STDOUT "\nTesting perl modules and selected functions\n\n";

    use FindBin qw($Bin); #Find directory of script

    ok(defined($Bin),"FindBin: Locate directory of script");

    use File::Basename qw(dirname);  #Strip the last part of directory

    ok(dirname($Bin), "File::Basename qw(dirname): Strip the last part of directory");

    use File::Spec::Functions qw(catdir);

    ok(catdir(dirname($Bin), "t"),"File::Spec::Functions qw(catdir): Concatenate directories");

    use YAML;

    my $yaml_file = catdir(dirname($Bin), "templates", "grid_test_file.yaml");
    ok( -f $yaml_file,"YAML: File= $yaml_file in grid-crawler directory");

    my $yaml = YAML::LoadFile($yaml_file);  #Create an object
    ok( defined $yaml,"YAML: Load File" );  #Check that we got something
    ok(Dump( $yaml ),"YAML: Dump file");

    use Log::Log4perl;
    ## Creates log
    my $log_file = catdir(dirname($Bin), "templates", "grid_crawler_log.yaml");
    ok( -f $log_file,"Log::Log4perl: File= $log_file in grid-crawler directory");

    ## Create log4perl config file
    my $config = create_log4perl_congfig(\$log_file);

    ok(Log::Log4perl->init(\$config), "Log::Log4perl: Initate");
    ok(Log::Log4perl->get_logger("grid-crawler_logger"), "Log::Log4perl: Get logger");

    my $logger = Log::Log4perl->get_logger("grid-crawler_logger");
    ok($logger->info("1"), "Log::Log4perl: info");
    ok($logger->warn("1"), "Log::Log4perl: warn");
    ok($logger->error("1"), "Log::Log4perl: error");
    ok($logger->fatal("1"), "Log::Log4perl: fatal");

    use Getopt::Long;
    push(@ARGV, ("-verbose", "2"));
    my $verbose = 1;
    ok(GetOptions("verbose:n"  => \$verbose), "Getopt::Long: Get options call");
    ok ($verbose == 2, "Getopt::Long: Get options modified");

    ## Check time
    use Time::Piece;
    my $date_time = localtime;
    ok($date_time, "localtime = $date_time");
    my $date_time_stamp = $date_time->datetime;
    ok($date_time_stamp, "datetime = $date_time");
    my $date = $date_time->ymd;
    ok($date, "ymd = $date");

    ## Locate name of script
    my $script = (`basename $0`);
    ok((`basename $0`), "Detect script name = $script");
}


sub create_log4perl_congfig {

##create_log4perl_congfig

##Function : Create log4perl config file.
##Returns  : "$config"
##Arguments: $file_name
##         : $file_name => log4perl config file {REF}

    my $file_nameRef = $_[0];

    my $conf = q?
        log4perl.category.grid-crawler_logger = TRACE, ScreenApp
        log4perl.appender.LogFile = Log::Log4perl::Appender::File
        log4perl.appender.LogFile.filename = ?.$$file_nameRef.q?
        log4perl.appender.LogFile.layout=PatternLayout
        log4perl.appender.LogFile.layout.ConversionPattern = [%p] %d %c - %m%n
        log4perl.appender.ScreenApp = Log::Log4perl::Appender::Screen
        log4perl.appender.ScreenApp.layout = PatternLayout
        log4perl.appender.ScreenApp.layout.ConversionPattern = [%p] %d %c - %m%n
        ?;
    return $conf;
}


sub load_yaml {

##load_yaml

##Function : Loads a YAML file into an arbitrary hash and returns it. Note: Currently only supports hashreferences and hashes and no mixed entries.
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

    open (my $YAML, "<", $yaml_file) or die "can't open ".$yaml_file.":".$!, "\n";  #Log4perl not initialised yet, hence no logdie
    local $YAML::QuoteNumericStrings = 1;  #Force numeric values to strings in YAML representation
    %yaml = %{ YAML::LoadFile($yaml_file) };  #Load hashreference as hash

    close($YAML);

    return %yaml;
}
