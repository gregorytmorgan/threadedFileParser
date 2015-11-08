#!/usr/bin/perl
#
# @author Greg Morgan
# @version 0.1
#
# Threaded file parser.
#

use strict;
use warnings;

use threads;
use POSIX qw(floor);

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

print "Workers: $nThreads\n";

if ($filename) {
	my @stat = stat $filename or die "Could open input file filename. $!\n";
	$filesize = $stat[7];

} else {
	die "No input file specified\n";
}

print "Filesize: $filesize bytes.\n";

$offset = floor($filesize / $nThreads);

#
# Create worker threads
#
for my $i (0 .. $nThreads - 1) {
	my $startOffset = floor($i * $offset);

	# the last worker handles to EOF to cover remainder bytes
	if ($i == ($nThreads - 1)) {
		$offset = 0;
	}

	push @workers, threads->create(\&pWorker, $i, $filename, $startOffset, $offset);
}

#
# Aggregate worker results
#
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
# @param {integer} Thread id.
# @param {string} Input filename.
# @param {integer} Start byte offset for this worker.
# @param {integer} Max bytes to process.
# @return {array} [totalLines, totalBytes]
sub pWorker {
	my @args = @_;
	my $n = 0;
	my $t_id = $args[0];
	my $filename = $args[1];
	my $startOffset = $args[2];
	my $maxBytes = $args[3];
	my $data;
	my $bytesSkipped = 0;
	my $bytesProcessed = 0;

	print "$t_id In thread $t_id\n";
	print "$t_id Initial offset (bytes): $startOffset\n";
	print "$t_id Max bytes: $maxBytes\n";

	open my $infile, '<', $filename or die "$t_id File open for $filename failed. !$\n";

	if ($startOffset != 0) {
		seek($infile, $startOffset, 0); # offset bytes, 0 = SEEK_SET

		# find he beginning of the current line
		while ((read($infile, $data, 1) != 0))
		{
			print "$t_id Read byte " . tell($infile) . "\n";

			use bytes; # config length() to return bytes not chars
			$bytesSkipped += length($data);

			if ($data eq "\n" && $bytesSkipped > 1) {
				print "$t_id Found endline @ byte " . tell($infile) . "\n";
				last;
			}

			# move back to find the start of the line
			seek($infile, -2, 1); # offset bytes, 1 = SEEK_CUR
		}
	}

	print "$t_id Starting line reads @ byte " . tell($infile) . "\n";

	# read lines up until maxBytes
	while (my $line = <$infile>) {
		use bytes; # config length() to return bytes not chars
		$bytesProcessed += length($line);

		if ($maxBytes && $bytesProcessed > $maxBytes) {
			$bytesProcessed -= length($line);
			last;
		}

		$n++;

		#
		# Do some work
		#

		print "$t_id Line $n: $line";
	}

	print "$t_id Thread complete.  Parsed $n lines, $bytesProcessed bytes.\n";

	close($infile);

	return ($n, $bytesProcessed);
}

# end file
