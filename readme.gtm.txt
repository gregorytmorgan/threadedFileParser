#
# Explore file parsing with n threads.
#

# file parser
parse.pl

# util to generate test data
gen_numbers.pl

# Example #1

>





# testing
#
# flush cache
# https://stackoverflow.com/questions/9551838/how-to-purge-disk-i-o-caches-on-linux
echo 3 | sudo tee /proc/sys/vm/drop_caches

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

Searching numbersTo100m.data, a 848 mb file took ~15 seconds.

Using parse.pl and 3 threads it too 20-30 seconds to parse, ~10 seconds to assemble.

Increasing to 4 threads didn't change results. Machine only has 4 cores.

# end file