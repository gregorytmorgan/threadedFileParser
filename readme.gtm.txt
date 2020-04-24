#
# Explore file parsing with n threads.
#

# file parser
parse.pl

# util to generate test data
gen_numbers.pl

# Example #1
> ./parse.with-threads.pl -f numbersTo1m.data -o big.out


#
# testing
#
# sudo dmidecode | grep | grep "Family:\|Version:"
# Machine has 4 cores. Family:Core 2 Duo, Version:Intel(R) Core(TM) i5-2320 CPU @ 3.00GHz

# flush cache
# https://stackoverflow.com/questions/9551838/how-to-purge-disk-i-o-caches-on-linux
echo 3 | sudo tee /proc/sys/vm/drop_caches

#
# Using grep - assuming all that is required is search
#
Searching numbersTo100m.data, a 848 mb file took ~15 seconds.

# grep using regex for a pattern, careful not to dump too much.
s="$(date -u +%s)" && echo $s && grep -G "9999998." numbersTo100m.data && e="$(date -u +%s)" && echo $(($e-$s)) && echo $e

# output
1587755338
99999980
99999981
99999982
99999983
99999984
99999985
99999986
99999987
99999988
99999989
15
1587755353

#
# Using worker threads
#
With 4 workers:
    Parse complete: Parsed 99,999,999 lines, 847.71M bytes in 25.34755 seconds. Reassembling ...
    Reassembly complete in 6.58997 seconds
    Total elapsed time (all files): 31.96773 seconds

#
# Using worker processes
#
With 4 workers:
    Parse complete: Parsed 0 lines, 0 bytes in 26.04190 seconds. Reassembling ...
    Reassembly complete in 7.64397 seconds
    Total elapsed time (all files): 33.68633 seconds

#
# Example output
#

>./threadedFileParser$ ./parse.with-procs.pl -f numbersTo1m.data -o big.out

Workers: 4
Processing file numbersTo1m.data
Filesize: 6.57M bytes.
00 In thread 00
00 Initial offset (bytes): 0
00 Max bytes: 1,722,222
00 Temp file: ./00tempiFbge.tmp
00 Starting line reads @ byte 0
02 In thread 02
02 Initial offset (bytes): 3444444
02 Max bytes: 5,166,666
02 Temp file: ./02temp_1aKU.tmp
01 In thread 01
01 Initial offset (bytes): 1722222
02 Read byte 7, now @ byte 3444445
02 Read byte 0, now @ byte 3444444
02 Read byte 5, now @ byte 3444443
02 Read byte NEWLINE, now @ byte 3444442
02 Found endline @ byte 3444442
02 Starting line reads @ byte 3444442
01 Max bytes: 3,444,444
01 Temp file: ./01tempVjjZv.tmp
01 Read byte 0, now @ byte 1722223
01 Read byte 9, now @ byte 1722222
01 Read byte 1, now @ byte 1722221
01 Read byte 6, now @ byte 1722220
01 Read byte 2, now @ byte 1722219
01 Read byte NEWLINE, now @ byte 1722218
01 Found endline @ byte 1722218
01 Starting line reads @ byte 1722218
03 In thread 03
03 Initial offset (bytes): 5166666
03 Max bytes: 0
03 Temp file: ./03tempDOvfR.tmp
03 Read byte 7, now @ byte 5166667
03 Read byte NEWLINE, now @ byte 5166666
03 Found endline @ byte 5166666
03 Starting line reads @ byte 5166666
03 Worker finished at Fri Apr 24 15:20:20 2020. Parsed 246,032 lines, 1.64M bytes in 0.11653 seconds.
00 Breaking at 1722225 after reading 1722225 bytes
00 Worker finished at Fri Apr 24 15:20:20 2020. Parsed 261,904 lines, 1.64M bytes in 0.15220 seconds.
02 Breaking at 5166673 after reading 1722231 bytes
02 Worker finished at Fri Apr 24 15:20:20 2020. Parsed 246,032 lines, 1.64M bytes in 0.15628 seconds.
01 Breaking at 3444449 after reading 1722231 bytes
01 Worker finished at Fri Apr 24 15:20:20 2020. Parsed 246,032 lines, 1.64M bytes in 0.18176 seconds.
Parse complete: Parsed 0 lines, 0 bytes in 1.00512 seconds. Reassembling ...
Reassembly complete in 0.01307 seconds
Total elapsed time (all files): 1.01853 seconds
Done.

# end file