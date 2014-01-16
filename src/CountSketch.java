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

import java.io.*;
import java.util.Random;
import java.util.Arrays;

/**
 * Created by ap349 on 12/11/13.
 */
public class CountSketch {
    public static final long PRIME_MODULUS = (1L << 31) - 1;

    private int depth;
    private int width;
    private long[][] sketch;
    private long[] hashA;
    private long[] hashB;
    private long[] hashgA;
    private long[] hashgB;
    private long size;
    private double epsilon;
    private double confidence;

    public CountSketch() {

    }

    public CountSketch(int depth, int width, int seed) {
        this.depth = depth;
        this.width = width;
        this.size = 0;
        this.epsilon = Math.sqrt(2/width);
        this.confidence = 1 - (1/Math.pow(2.0, depth));
        initTablesWith(depth, width, seed);
    }

    public CountSketch(double epsilon, double confidence, int seed) {
        this.epsilon = epsilon;
        this.confidence = confidence;
        this.width = (int) (2/Math.pow(epsilon,2));
        this.depth = (int) (((-1) * Math.log(1 - confidence))/Math.log(2));
        this.size = 0;
        initTablesWith(depth, width, seed);
    }

    public double getRelativeError() {
        return this.epsilon;
    }

    public double getConfidence() {
        return this.confidence;
    }

    public long getSize() {
        return this.size;
    }

    private void initTablesWith(int depth, int width, int seed) {
        Random random = new Random(seed);
        this.sketch = new long[depth][width];
        this.hashA = new long[depth];
        this.hashB = new long[depth];
        this.hashgA = new long[depth];
        this.hashgB = new long[depth];

        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // We can set b=0 but setting b>0 makes the hashes
        // 2-independent (pairwise independent)

        for ( int i=0; i<depth; ++i) {
            hashA[i] = random.nextInt(Integer.MAX_VALUE);
            hashB[i] = random.nextInt(Integer.MAX_VALUE);
            hashgA[i] = random.nextInt(Integer.MAX_VALUE);
            hashgB[i] = random.nextInt(Integer.MAX_VALUE);
        }
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

    private int[] getHashBucketsg(byte[] b, int hashCount, int max)
    {
        int[] result = new int[hashCount];
        int hash1 = MurmurHash.hash(b, b.length, Integer.MAX_VALUE);
        int hash2 = MurmurHash.hash(b, b.length, hash1);
        for (int i = 0; i < hashCount; i++)
        {
            int temp = Math.abs((hash2 + i * hash1) % max) % 2;
            if ( temp == 0 ) result[i] = 1;
            if ( temp == 1 ) result[i] = -1;
        }
        return result;
    }

    private int[] getHashBucketsg(String key, int hashCount, int max)
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
        return getHashBucketsg(b, hashCount, max);
    }

    private int hash(long item, int i) {
        long hash = hashA[i] * item;
        hash = hash + hashB[i];

        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.

        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        return ((int) hash % this.width);
    }

    private int hashg(long item, int i) {
        long hash = hashgA[i] * item;
        hash = hash + hashgB[i];

        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.

        hash += hash >> 32;
        hash &= PRIME_MODULUS;

        if ( (int) hash % 2 == 0 ) return 1;
        return -1;
    }

    private long median(long[] array) {
        Arrays.sort(array);
        int middle = array.length/2;
        long medianValue = 0;

        if (array.length % 2 == 1)
            medianValue = array[middle];
        else
            medianValue = (array[middle-1] + array[middle]) / 2;

        //System.out.println(medianValue);
        return medianValue;
    }

    public void add(long item, long count) {
        if ( count < 0 ) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }

        for ( int i=0; i < depth; ++i ) {
            this.sketch[i][hash(item, i)] += count * hashg(item, i);
        }

        this.size += count;
    }

    public void add(String item, long count) {
        if (count < 0)
        {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }
        int[] buckets = getHashBuckets(item, depth, width);
        int[] bucketsg = getHashBucketsg(item, depth, width);

        for (int i=0; i < depth; ++i ) {
            this.sketch[i][buckets[i]] += count * bucketsg[i];
        }
    }

    public void add(double item, long count) {
        add(Double.toString(item), count);
    }

    public long estimateCount(long item) {
        long[] result = new long[this.depth];

        for (int i=0; i<this.depth; ++i) {
            result[i] = this.sketch[i][hash(item, i)] * hashg(item, i);
        }

        return median(result);
    }

    public long estimateCount(String item) {
        long[] result = new long[this.depth];
        int[] buckets = getHashBuckets(item, depth, width);
        int[] bucketsg = getHashBucketsg(item, depth,width);

        for (int i=0; i<this.depth; ++i) {
            result[i] = this.sketch[i][buckets[i]] * bucketsg[i];
        }

        return median(result);
    }

    public static byte[] serialize(CountSketch sketch)
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
                s.writeLong(sketch.hashB[i]);
                s.writeLong(sketch.hashgA[i]);
                s.writeLong(sketch.hashgB[i]);

                for (int j = 0; j < sketch.width; ++j)
                {
                    s.writeLong(sketch.sketch[i][j]);
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

    public static CountSketch deserialize(byte[] data)
    {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        DataInputStream s = new DataInputStream(bis);
        try
        {
            CountSketch sketch = new CountSketch();
            sketch.size = s.readLong();
            sketch.depth = s.readInt();
            sketch.width = s.readInt();
            sketch.epsilon = Math.sqrt(2/sketch.width);
            sketch.confidence = 1 - (1/Math.pow(2.0, sketch.depth));;
            sketch.hashA = new long[sketch.depth];
            sketch.hashB = new long[sketch.depth];
            sketch.hashgA = new long[sketch.depth];
            sketch.hashgB = new long[sketch.depth];
            sketch.sketch = new long[sketch.depth][sketch.width];

            for (int i = 0; i < sketch.depth; ++i)
            {
                sketch.hashA[i] = s.readLong();
                sketch.hashB[i] = s.readLong();
                sketch.hashgA[i] = s.readLong();
                sketch.hashgB[i] = s.readLong();

                for (int j = 0; j < sketch.width; ++j)
                {
                    sketch.sketch[i][j] = s.readLong();
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

    public static CountSketch merge(CountSketch... estimators) {
        CountSketch merged = new CountSketch();

        if ( estimators != null && estimators.length > 0 ) {
            merged.depth = estimators[0].depth;
            merged.width = estimators[0].width;
            long[] hashA = Arrays.copyOf(estimators[0].hashA, estimators[0].hashA.length);
            long[] hashB = Arrays.copyOf(estimators[0].hashB, estimators[0].hashB.length);
            long[] hashgA = Arrays.copyOf(estimators[0].hashgA, estimators[0].hashgA.length);
            long[] hashgB = Arrays.copyOf(estimators[0].hashgB, estimators[0].hashgB.length);

            merged.hashA = hashA;
            merged.hashB = hashB;
            merged.hashgA = hashgA;
            merged.hashgB = hashgB;
            long[][] sketch = new long[merged.depth][merged.width];

            for (CountSketch estimator : estimators) {
                for (int i=0; i<sketch.length; ++i)  {
                    for (int j=0; j<sketch[i].length; ++j) {
                        sketch[i][j] += estimator.sketch[i][j];
                    }
                }
                merged.size += estimator.size;
            }
        }

        return merged;
    }
}
