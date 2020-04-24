#!/usr/bin/perl
#
# Threaded file parser.
#
# Open a file and use n threads to read/evaluate approx equal parts. Output the
# each part to file.  Concatenate call the file parts to form the result
#
# If input is multiple files, processing blocks between files.
#

use strict;
use warnings;

use threads;
use POSIX qw(floor);
use File::Temp;
use Time::HiRes qw( time );
use Number::Format qw(format_bytes format_number);

use Config;
$Config{useithreads} or die('Recompile Perl with threads to run this program.');

no strict 'refs'; # needed to assign STDOUT

use Getopt::Std;

#
# Globals
#
use vars qw/ %opt /;

my $outfile;
my $opt_string = 'dhvf:o:q';
my @fileList;
my $filesize;
my $totalLines = 0;
my $totalBytes = 0;
my $nThreads = 4;
my $offset;
my @outTempFiles;
my $debug = 0;
my $t_id = 0;

#
# usage and comman line opts
#
sub usage()
{
    print STDERR "Usage: $0 [-hqv] [-f file]\n";
    print STDERR "\t-f file   : Input files. Use quotes with whitespace delim for multiples.\n";
    print STDERR "\t-d        : Debug (Will corrupt/inject text into output)\n";
    print STDERR "\t-h        : this message\n";
	print STDERR "\t-q		  : Quite. No console output\n";
    print STDERR "\t-v        : Verbose output to STDERR\n";
	print STDERR "\t-o file   : Output file (Default STDOUT)\n";
    print STDERR "\n";
    print STDERR "Example: $0 -v infile\n";
    exit();
}

getopts( "$opt_string", \%opt ) or usage();

usage() if $opt{h};

if ($opt{d}) {
    $debug = 1;
}

if ($opt{v} && $opt{q}) {
	$opt{q} = 0;
}

if ($opt{f}) {
	@fileList = split(/\s+/, "$opt{f}");
} else {
	if (@ARGV == 0) {
        # we have to have file vs STDIN because we're going to partition the file into
        # sections for the thread workers
		die("Error - No files\n");
	} else {
		@fileList = @ARGV;
	}
}

if ($opt{o}) {
	open ($outfile, ">", $opt{o}) or die "Cannot open output file $opt{o}: $!";
} else {
    $outfile = "STDOUT";
#	$outfile = select (STDOUT);
}

if (!$opt{q}) {
    print STDOUT "Workers: $nThreads\n";
}

#
# for each input file
#

my $prog_begin_time = time();

foreach my $file (@fileList) {
    if (!$opt{q}) {
        print "Processing file $file\n";
    }

    my @stat = stat $file or die "Could not open input file $file. $!\n";
    $filesize = $stat[7];

    if (!$opt{q}) {
        print "Filesize: " . format_bytes($filesize, precision => 2) . " bytes.\n";
    }

    my @workers;
    $offset = floor($filesize / $nThreads);

    #
    # Create worker processes
    #
    for my $i (0 .. $nThreads - 1) {
        my $startOffset = floor($i * $offset);

        # the last worker handles to EOF to cover remainder bytes
        if ($i == ($nThreads - 1)) {
            $offset = 0;
        }

        my $t_id = sprintf("%02d", ($t_id * $nThreads) + $i);

        push @workers, threads->create(\&pWorker, $t_id, $file, $startOffset, $offset * ($i + 1));
    }

    my $parse_begin_time = time();

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

    # at this point all threads are joined/complete
    undef @workers;

    if (!$opt{q}) {
        printf("Parse complete: Parsed %s lines, %s bytes in %.5f seconds. Reassembling ...\n", format_number($totalLines), format_bytes($totalBytes), time() - $parse_begin_time);
    }

    # tmp file to cleanup
    my @tmpfiles = ();

    my $reassembly_begin_time = time();

    # reassemble the parts
    foreach my $file (@outTempFiles) {
        if ($outfile eq "STDOUT") {
            open my $fh, '<', $file or die "File open for $file failed. !$\n";
            while (my $line = <$fh>) {
                print STDOUT $line;
            }
        }
    } # each output file

    if ($opt{o}) {
        my $filelist = join(" ", @outTempFiles);
        system qq( cat $filelist >> "$opt{o}" );
    }

    unlink @outTempFiles or warn "Could not unlink " . join(",", @outTempFiles) . ": $!";

    undef @outTempFiles;
    $t_id++;

    if (!$opt{q}) {
        printf("Reassembly complete in %.5f seconds\n", time() - $reassembly_begin_time);
    }
}

if (!$opt{q}) {
    printf("Total elapsed time (all files): %.5f seconds\n", time() - $prog_begin_time);
}

if ($opt{o}) {
	close($outfile);
}

if (!$opt{q}) {
    print "Done.\n";
}

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

    if (!$opt{q}) {
        print "$t_id In thread $t_id\n";
        print "$t_id Initial offset (bytes): $startOffset\n";
        print "$t_id Max bytes: " . format_number($maxOffset) . "\n";
    }

    my $tmpFile = File::Temp->new(
        TEMPLATE => $t_id . 'tempXXXXX',
        DIR => '.',
        SUFFIX => '.tmp',
		UNLINK => 0
    );

	open my $tmpOutFile, ">", $tmpFile->filename or die "Couldn't open temp file - " . $tmpFile->filename . ": $!";

    if (!$opt{q}) {
        print "$t_id Temp file: " . $tmpFile->filename . "\n";
    }

	open my $infile, '<', $inFileName or die "$t_id File open for $inFileName failed. !$\n";

    my $thread_parse_begin_time = time();

	if ($startOffset != 0) {
		seek($infile, $startOffset, 0); # offset bytes, 0 = SEEK_SET

		# find he beginning of the current line
		while ((read($infile, $data, 1) != 0))
		{
			my $b = ($data eq "\n") ? "NEWLINE" : $data;

            if (!$opt{q}) {
                print "$t_id Read byte $b, now @ byte " . tell($infile) . "\n";
            }

			use bytes; # config length() to return bytes not chars
			$bytesSkipped += length($data);

			if ($data eq "\n" && $bytesSkipped > 1) {
                if (!$opt{q}) {
                    print "$t_id Found endline @ byte " . tell($infile) . "\n";
                }
				last;
			}

			# move back to find the start of the line
			seek($infile, -2, 1); # offset bytes, 1 = SEEK_CUR
		}
	}

    if (!$opt{q}) {
        print "$t_id Starting line reads @ byte " . tell($infile) . "\n";
    }

	# read lines up until maxOffset
	while (my $line = <$infile>) {
		use bytes; # config length() to return bytes not chars
		$bytesProcessed += length($line);

		if ($maxOffset && (tell($infile) - 1) >= $maxOffset) {
            if (!$opt{q}) {
                print "$t_id Breaking at " . tell($infile) . " after reading $bytesProcessed bytes\n";
            }
			$bytesProcessed -= length($line);
			last;
		}

		$n++;

		#
		# Do some work
		#
        if ($debug) {
            print $tmpOutFile "$t_id Line $n: $line";
        } else {
            print $tmpOutFile "$line";
        }
	}

    if (!$opt{q}) {
        my $tstamp = localtime();
        my $thread_elasped_time = time() - $thread_parse_begin_time;
        printf("%s Thread completed at %s. Parsed %s lines, %s bytes in %.5f seconds.\n", $t_id, $tstamp, format_number($n), format_bytes($bytesProcessed), $thread_elasped_time);
    }

	close($tmpOutFile);
	close($infile);

	return ($n, $bytesProcessed, $tmpFile->filename);
}

# end file
