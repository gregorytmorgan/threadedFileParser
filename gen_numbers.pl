#!/usr/bin/perl
#
# Create a file of number, one number per line between 0 and max_number.
#
# Note:  9,999,999 is ~76 megabytes
#       99,999,999 is ~848 megabytes
#

use strict;
use warnings;
use Getopt::Std;

#
# Globals
#
use vars qw/ %opt /;

my $i;
my $fname;
my $max_number = 10;
my $outfile;
my $opt_string = 'hn:o:';

#
# usage
#
sub usage()
{
	print STDERR "Usage: $0 [-h] [-n max_number] [-o file]\n";
	print STDERR "\t-n number : Max number Default=$max_number.\n";
	print STDERR "\t-h        : This message.\n";
	print STDERR "\t-o file   : Output file, Default=STDOUT.\n";
	print STDERR "\n";
	print STDERR "Example: $0 -n 100\n";
	exit();
}

getopts( "$opt_string", \%opt ) or usage();

usage() if $opt{h};

if ($opt{n}) {
    $max_number = int($opt{n});
}

if ($opt{o}) {
    open ($outfile, ">", $opt{o}) or die "Cannot open output file $opt{o}: $!";
    for ($i = 0; $i < $max_number; $i++) {
        printf $outfile "%d\n", $i;
    }
    close($outfile);
} else {
    for ($i = 0; $i < $max_number; $i++) {
        printf "%d\n", $i;
    }
}

# end file
