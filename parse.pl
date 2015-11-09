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
use File::Temp;

use Config;
$Config{useithreads} or die('Recompile Perl with threads to run this program.');

my $outFileName = "result.txt";
#my $outFileName = "STDOUT";

my $inFileName = "testData.txt";
#my $inFileName = "testData.2.txt";

my $filesize;
my $totalLines = 0;
my $totalBytes = 0;
my @workers;
my $nThreads = 3;
my $offset;
my @outTempFiles;

print "Workers: $nThreads\n";

if ($inFileName) {
	my @stat = stat $inFileName or die "Could open input file inFileName. $!\n";
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

	push @workers, threads->create(\&pWorker, $i, $inFileName, $startOffset, $offset * ($i + 1));
}

#
# Aggregate worker results
#
foreach (@workers) {
	my $worker = $_;
	my @returnData = $worker->join();
	$totalLines += $returnData[0];
	$totalBytes += $returnData[1];
	push @outTempFiles, $returnData[2];
}

print "Summary: Parsed $totalLines lines, $totalBytes bytes.\n";

foreach my $file (@outTempFiles) {
	if ($outFileName eq "STDOUT") {
		open my $fh, '<', $file or die "File open for $file failed. !$\n";
		while (my $line = <$fh>) {
			print STDOUT $line;
		}
	} else {
		system qq( cat "$file" >> "$outFileName" );
	}
	unlink $file or warn "Could not unlink $file: $!";
}

print "\nDone.\n";

#
# Given a start offset into file, move backwards until begin of line, then read
# lines while not exceeding maxOffset.
#
# @param {integer} Thread id.
# @param {string} Input inFileName.
# @param {integer} Start byte offset for this worker.
# @param {integer} Max bytes to process.
# @return {array} [totalLines, totalBytes]
sub pWorker {
	my @args = @_;
	my $n = 0;
	my $t_id = $args[0];
	my $inFileName = $args[1];
	my $startOffset = $args[2];
	my $maxOffset = $args[3];
	my $data;
	my $bytesSkipped = 0;
	my $bytesProcessed = 0;

	print "$t_id In thread $t_id\n";
	print "$t_id Initial offset (bytes): $startOffset\n";
	print "$t_id Max bytes: $maxOffset\n";

    my $tmpFile = File::Temp->new(
        TEMPLATE => 'tempXXXXX',
        DIR => '.',
        SUFFIX => '.tmp',
		UNLINK => 0
    );

	open my $tmpOutFile, ">", $tmpFile->filename or die "Couldn't open temp file - " . $tmpFile->filename . ": $!";

	print "$t_id Temp file: " . $tmpFile->filename . "\n";

	open my $infile, '<', $inFileName or die "$t_id File open for $inFileName failed. !$\n";

	if ($startOffset != 0) {
		seek($infile, $startOffset, 0); # offset bytes, 0 = SEEK_SET

		# find he beginning of the current line
		while ((read($infile, $data, 1) != 0))
		{
			my $b = ($data eq "\n") ? "NEWLINE" : $data;
			print "$t_id Read byte $b, now @ byte " . tell($infile) . "\n";

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

	# read lines up until maxOffset
	while (my $line = <$infile>) {
		use bytes; # config length() to return bytes not chars
		$bytesProcessed += length($line);

		if ($maxOffset && (tell($infile) - 1) >= $maxOffset) {
			print "$t_id Breaking at " . tell($infile) . " after reading $bytesProcessed bytes\n";
			$bytesProcessed -= length($line);
			last;
		}

		$n++;

		#
		# Do some work
		#
		print $tmpOutFile "$t_id Line $n: $line";
	}

	print "$t_id Thread complete.  Parsed $n lines, $bytesProcessed bytes.\n";

	close($tmpOutFile);
	close($infile);

	return ($n, $bytesProcessed, $tmpFile->filename);
}

# end file
