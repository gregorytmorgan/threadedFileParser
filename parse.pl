#!/usr/bin/perl 
#
# @author Greg Morgan
# @version 0.1
#
# Parse a test file
#

use strict;
use warnings;
use threads;
use POSIX qw(ceil floor);
use Data::Dumper;

use Config;
$Config{useithreads} or die('Recompile Perl with threads to run this program.');

my $infile = \*STDIN;
my $outfile = \*STDOUT;

my $filesize;
#my $filename = "testData.txt";
my $filename = "testData.2.txt";
my $totalLines = 0;
my $totalBytes = 0;
my @workers;
my $nThreads = 3;
my $offset;
my $remainder;

print "Workers: $nThreads\n";

if ($filename) {
	my @stat = stat $filename;
	$filesize = $stat[7];
} else {
	$filesize = -1;
}

print "Filesize: $filesize bytes.\n";

$offset = floor($filesize / $nThreads);
$remainder = $filesize % $nThreads;

for my $i (0 .. $nThreads - 1) {

	my $startOffset = floor($i * $offset); # -1 since zero based

	if ($i == ($nThreads - 1)) {
		$offset = 0;
	}

	push @workers, threads->create(\&pWorker, $i, $filename, $startOffset, $offset);
}

foreach (@workers) {
	my $worker = $_;
	my @returnData = $worker->join();
	$totalLines += $returnData[0];
	$totalBytes += $returnData[1];
}

print "Done. Totals: Parsed $totalLines lines, $totalBytes bytes.\n";

#
# Given a start offset into file, move backwards until begin of line, then read
# lines while not exceeding maxBytes.
#
sub pWorker {
	my @args = @_;
	my $n = 0;
	my $t_id = $args[0];
	my $filename = $args[1];
	my $readOffset = $args[2];
	my $maxOffset = $args[3];
	my $tmp;
	my $charsRead = 0;
	my $bytesSkipped = 0;
	my $bytesProcessed = 0;

	print "$t_id, In thread $t_id\n";

	print "$t_id, Initial offset (bytes): $readOffset\n";
	print "$t_id, Max offset (bytes): $maxOffset\n";

	open my $infile, '<', $filename or die "$t_id, File open for $filename failed. !$\n";

	if ($readOffset != 0) {
		seek($infile, $readOffset, 0); # offset bytes, 0 = SEEK_SET

		while ((read($infile, $tmp, 1) != 0))
		{
			print "$t_id, Read char $tmp @ byte " . tell($infile) . "\n";

			use bytes;
			$bytesSkipped += length($tmp);

			if ($tmp eq "\n" && $bytesSkipped > 1) {
				print "$t_id, Found endline\n";
				last;
			}

			# move back to find the start of the line
			seek($infile, -2, 1); # offset bytes, 1 = SEEK_CUR
		}
	}

	print "$t_id, Starting line reads @ byte " . tell($infile) . "\n";

	while (my $line = <$infile>) {
		use bytes;
		$bytesProcessed += length($line);

		if ($maxOffset && $bytesProcessed > $maxOffset) {
			$bytesProcessed -= length($line);
			last;
		}

		$n++;
		print "$t_id, Line $n: $line";
	}

	print "$t_id, Thread complete.  Parsed $n lines, $bytesProcessed bytes.\n";

	close($infile);

	return ($n, $bytesProcessed);
}

# end file
