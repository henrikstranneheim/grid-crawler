#!/usr/bin/env perl

###Copyright 2016 Henrik Stranneheim

BEGIN {
    
    my @modules = ("TAP::Harness",
	);
    
    ## Evaluate that all modules required are installed
    &EvalModules(\@modules);
    
    sub EvalModules {
	
	##EvalModules
	
	##Function : Evaluate that all modules required are installed 
	##Returns  : ""
	##Arguments: $modulesArrayRef
	##         : $modulesArrayRef => Array of module names
	
	my $modulesArrayRef = $_[0];
	
	foreach my $module (@{$modulesArrayRef}) {
	    
	    $module =~s/::/\//g;  #Replace "::" with "/" since the automatic replacement magic only occurs for barewords.
	    $module .= ".pm";  #Add perl module ending for the same reason
	    
	    eval { 
		
		require $module; 
	    };
	    if($@) {
		
		warn("NOTE: ".$module." not installed - Please install to run grid-crawler tests.\n");
		warn("NOTE: Aborting!\n");
		exit 1;
	    }
	}
    }
}

use TAP::Harness;

my %args = (verbosity => 1);

my $harness = TAP::Harness->new( \%args );
my @tests = [ "test.t" ];
$harness->runtests(@tests);
