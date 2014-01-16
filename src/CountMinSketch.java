/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.io.UnsupportedEncodingException;

/**
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CountMinSketch
{
    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private int depth;
    private int width;
    private long[][] table;
    private long[] hashA;
    private long[] hashB;
    private long size;
    private double eps;
    private double confidence;

    private CountMinSketch()
    {
    }

    public CountMinSketch(int depth, int width, int seed)
    {
        this.depth = depth;
        this.width = width;
        this.eps = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        initTablesWith(depth, width, seed);
    }

    public CountMinSketch(double epsOfTotalCount, double confidence, int seed)
    {
        // 2/w = eps ; w = 2/eps
        // 1/2^depth <= 1-confidence ; depth >= -log2 (1-confidence)
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(2 / epsOfTotalCount);
        this.depth = (int) Math.ceil(-Math.log(1 - confidence) / Math.log(2));
        initTablesWith(depth, width, seed);
    }

    private CountMinSketch(int depth, int width, int size, long[] hashA, long[][] table)
    {
        this.depth = depth;
        this.width = width;
        this.eps   = 2.0 / width;
        this.confidence = 1 - 1 / Math.pow(2, depth);
        this.hashA = hashA;
        this.table = table;
        this.size  = size;
    }

    private void initTablesWith(int depth, int width, int seed)
    {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        this.hashB = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i)
        {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
            hashB[i] = r.nextInt(Integer.MAX_VALUE);
        }
    }

    public double getRelativeError()
    {
        return eps;
    }

    public double getConfidence()
    {
        return confidence;
    }

    private int[] getHashBuckets(byte[] b, int hashCount, int max)
    {
        int[] result = new int[hashCount];
        int hash1 = MurmurHash.hash(b, b.length, 0);
        int hash2 = MurmurHash.hash(b, b.length, hash1);
        for (int i = 0; i < hashCount; i++)
        {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    private int[] getHashBuckets(String key, int hashCount, int max)
    {
        byte[] b;
        try
        {
            b = key.getBytes("UTF-16");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
        return getHashBuckets(b, hashCount, max);
    }

    private int hash(long item, int i)
    {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    public void add(long item, long count)
    {
        if (count < 0)
        {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        for (int i = 0; i < depth; ++i)
        {
            table[i][hash(item, i)] += count;
        }
        size += count;
    }

    public void add(String item, long count)
    {
        if (count < 0)
        {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i)
        {
            table[i][buckets[i]] += count;
        }
        size += count;
    }

    public long size()
    {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    public long estimateCount(long item)
    {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i)
        {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    public long estimateCount(String item)
    {
        long res = Long.MAX_VALUE;
        int[] buckets = getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i)
        {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    private long median(long[] array) {
        Arrays.sort(array);
        int middle = array.length/2;
        long medianValue = 0;

        if (array.length % 2 == 1)
            medianValue = array[middle];
        else
            medianValue = (array[middle-1] + array[middle]) / 2;

        return medianValue;
    }

    public long unbiasedEstimateCount(long item) {
        long[] result = new long[depth];
        for (int i=0; i < depth; ++i) {
            result[i] = table[i][hash(item ,i)] - (size - table[i][hash(item ,i)])/(width - 1);
        }
        return(median(result));
    }

    public long unbiasedEstimateCount(String item) {
        long[] result = new long[depth];
        int[] buckets = getHashBuckets(item, depth, width);
        for (int i=0; i < depth; ++i) {
            result[i] = table[i][buckets[i]] - (size - table[i][buckets[i]])/(width - 1);
        }
        return(median(result));
    }

    /**
     * Merges count min sketches to produce a count min sketch for their combined streams
     *
     * @param estimators
     * @return merged estimator or null if no estimators were provided
     */
    public static CountMinSketch merge(CountMinSketch... estimators)
    {
        CountMinSketch merged = null;
        if (estimators != null && estimators.length > 0)
        {
            int depth = estimators[0].depth;
            int width = estimators[0].width;
            long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);

            long[][] table = new long[depth][width];
            int size = 0;

            for (CountMinSketch estimator : estimators)
            {
                for (int i = 0; i < table.length; i++)
                {
                    for (int j = 0; j < table[i].length; j++)
                    {
                        table[i][j] += estimator.table[i][j];
                    }
                }
                size += estimator.size;
            }

            merged = new CountMinSketch(depth, width, size, hashA, table);
        }

        return merged;
    }

    public static byte[] serialize(CountMinSketch sketch)
    {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream s = new DataOutputStream(bos);
        try
        {
            s.writeLong(sketch.size);
            s.writeInt(sketch.depth);
            s.writeInt(sketch.width);
            for (int i = 0; i < sketch.depth; ++i)
            {
                s.writeLong(sketch.hashA[i]);
                for (int j = 0; j < sketch.width; ++j)
                {
                    s.writeLong(sketch.table[i][j]);
                }
            }
            return bos.toByteArray();
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }

    public static CountMinSketch deserialize(byte[] data)
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try
        {
            CountMinSketch sketch = new CountMinSketch();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.eps = 2.0 / sketch.width;
            sketch.confidence = 1 - 1 / Math.pow(2, sketch.depth);
            sketch.hashA = new long[sketch.depth];
            sketch.table = new long[sketch.depth][sketch.width];
            for (int i = 0; i < sketch.depth; ++i)
            {
                sketch.hashA[i] = s.readLong();
                for (int j = 0; j < sketch.width; ++j)
                {
                    sketch.table[i][j] = s.readLong();
                }
            }
            return sketch;
        }
        catch (IOException e)
        {
            // Shouldn't happen
            throw new RuntimeException(e);
        }
    }
}