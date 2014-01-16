Example implementation of Oracle's optimizer statistics gathering row source.
==============================================================================

Dependencies:

a)  Oracle JDBC driver ojdbc6.jar <br>
b)  IndexMinPQ and Queue implementation from algs4.jar (http://algs4.cs.princeton.edu/code/algs4.jar)
<br><br>

Compilation:
<br><br>

a) Download the above jar files and add them to the classpath <br>
b) Compile all the java source files in this repository in a single directory<br>

<br><br>

CardinalitySketch.java     -   Implements approximate NDV algorithm to estimate NDV <br>
CountMinSketch.java        -   Implements Count-Min sketch data structure (Thanks  Michael Spiegel)
                               (at https://github.com/addthis/stream-lib/).<br>
CountSketch.java           -   Implements Count sketch data structure.<br>
TopK.java                  -   Implements Top-K algorithm using either count-min or count sketch<br>
MurmurHash.java            -   Implements Murmur2 hashing (Thanks Andrzej Bialecki at getopt org)<br>
RowidMap.java              -   Data structure used in TopK.java to put on priority queue.<br>
ColumnStats.java           -   Data structure to hold column statistics and sketches <br>       
SqlStatistics.java         -   Main driver that uses all of the above to estimate NDV and top-n frequencies.<br>
                               Change the sql in the main method. The sql should be like
                                select a.rowid, a.col1, a.col2 .... from table a
                                
