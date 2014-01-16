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

import oracle.sql.ROWID;

import java.util.*;

/**
 * Created by ap349 on 12/14/13.
 */

public class CardinalitySketch {
    private static final long HASH_SIZE = 64;
    private static final int MAX_ELEMENTS = 16384;
    private static final long MEGABYTE = 1024L * 1024L;

    public class Node {
        private long hash;
        private Node[] next;
        private Node[] previous;
        private long frequency;
        private String value;
        private ROWID rowid;

        public Node(long hash, String value, ROWID rowid) {
            this.hash = hash;
            this.next = new Node[64];
            this.previous = new Node[64];
            this.frequency = 1;
            this.value = value;
            this.rowid = rowid;

            for (int i=0; i<64; ++i) {
                next[i] = null;
                previous[i] = null;
            }
        }

        public long getFrequency() {
            return frequency;
        }

        public void incrementFrequency() {
            ++this.frequency;
        }

        public String toString() {
            return (value + ": " + frequency);
        }

        public String getValue() {
            return this.value;
        }

        public ROWID getRowid() {
            return this.rowid;
        }
    }

    private class NodeComparator implements Comparator<Node> {
        public int compare(Node n1, Node n2) {
            return (int)(n2.getFrequency() - n1.getFrequency());
        }
    }

    public static class Synopsis {
        private int split;
        private Set<Long> synopsis;
        private long ndv;

        public Synopsis(Set<Long> synopsis, int split) {
            this.split = split;
            this.synopsis = synopsis;
            this.ndv = synopsis.size() * (1L << split);
        }

        public String toString() {
            StringBuffer stringBuffer = new StringBuffer();
            for (Long value: synopsis) {
                stringBuffer.append(String.format("%" + HASH_SIZE + "s\n", Long.toBinaryString(value)).replace(' ', '0'));
            }

            stringBuffer.append("Size: " + synopsis.size() + " Split: " + split);

            return stringBuffer.toString();
        }
    }

    private Node[] listsHead;
    private Node[] listsTail;
    private HashMap<Long, Node> sketchMap;
    private int size;
    private int split;
    private long splitMask;
    private int maxelements;

    public CardinalitySketch() {
        this(MAX_ELEMENTS);
    }

    public CardinalitySketch(int maxelements) {
        this.listsHead = new Node[64];
        this.listsTail = new Node[64];
        this.size = 0;
        this.split = 0;
        this.splitMask = 0;
        this.maxelements = maxelements;
        this.sketchMap = new HashMap<Long, Node>();
        for (int i=0; i<HASH_SIZE; ++i) {
            this.listsHead[i] = null;
            this.listsTail[i] = null;
        }
    }

    public int getMaxelements() {
        return maxelements;
    }

    private void incrementSplit() {
        ++this.split;
        splitMask = splitMask | ( 1L << (HASH_SIZE - this.split) );
    }

    private void evictNodesOnSplit() {
        int splitArrayIndex = this.split - 1;
        System.out.println("####################################");
        printMemoryUsage();
        System.out.println("Splitting for: " + splitArrayIndex + " size: " + size);
        System.out.println("Split mask " + Long.toBinaryString(splitMask));

        Node node = this.listsHead[splitArrayIndex];

        while ( node != null ) {
            for (int i=0; i<HASH_SIZE; ++i) {
                if ( i != splitArrayIndex ) {
                    Node previousNode = node.previous[i];
                    Node nextNode = node.next[i];
                    if ( previousNode != null ) {
                        previousNode.next[i] = nextNode;
                        if ( nextNode == null )
                            listsTail[i] = previousNode;
                    }

                    if ( nextNode != null ) {
                        nextNode.previous[i] = previousNode;
                        if ( previousNode == null )
                            listsHead[i] = nextNode;
                    }
                }
            }

            Node nextNode = node.next[splitArrayIndex];

            if ( nextNode != null ) {
                node.next[splitArrayIndex] = null;
                nextNode.previous[splitArrayIndex] = null;
            }

            sketchMap.remove(node.hash);
            --size;
            node = nextNode;
        }

        listsHead[splitArrayIndex] = null;
        System.out.println("Split done for: " + splitArrayIndex + " size: " + size);
        printMemoryUsage();
        System.out.println("####################################");
        System.out.println();
    }

    private void splitSketch() {
        incrementSplit();
        evictNodesOnSplit();
    }

    private boolean belongsInSketch(long hash) {
        if ( !sketchMap.containsKey(hash) && (hash & this.splitMask) == 0  ) {
            return true;
        } else {
            if ( sketchMap.containsKey(hash) ) {
                sketchMap.get(hash).incrementFrequency();
            }
            return false;
        }
    }

    public int getSize() {
        return this.size;
    }

    public int getSplit() { return this.split; }

    public void printSketch(int bit) {
        for ( int i=0; i<listsHead.length; ++i ) {
            if ( bit == -1 || bit == i) {
                System.out.println("List for bit " + i);
                Node node = listsHead[i];
                if ( node != null ) {
                    while ( node != null ) {
                        System.out.println("  " + Long.toBinaryString(node.hash) + " (" + node.hash + ")");
                        node = node.next[i];
                    }
                }
            }
        }
    }

    public void printSketchMap() {
        System.out.println(" Sketch Map After Split: " + split);
        for ( Long key : sketchMap.keySet() ) {
            System.out.println(Long.toBinaryString(key));
        }
        System.out.println();
    }

    private void processHashForBitposition(Node node, int position) {
        if ( this.listsHead[position] == null ) {
            this.listsHead[position] = node;
            this.listsTail[position] = node;
        } else {
            Node tail = listsTail[position];
            tail.next[position] = node;
            listsTail[position] = node;
            node.previous[position] = tail;
        }
    }

    public void add(String item) {
        add(item, null);
    }

    public void add(String item, ROWID rowid) {
        long hash = MurmurHash.hash64(item);
        String binaryHashString =
                String.format("%" + HASH_SIZE + "s", Long.toBinaryString(hash)).replace(' ', '0');

        if ( belongsInSketch(hash) ) {
            ++size;
            Node node = new Node(hash, item, rowid);
            sketchMap.put(hash, node);

            for (int i=0; i<HASH_SIZE; ++i) {
                if ( binaryHashString.substring( i, i + 1 ).equals("1") ) {
                    processHashForBitposition(node, i);
                }
            }

            if ( size > maxelements ) {
                splitSketch();
            }
        }
    }

    public void add(long item) {
        add(Long.toString(item));
    }

    public void add(int item) {
        add(Integer.toString(item));
    }

    public void add(double item) {
        add(Double.toString(item));
    }

    public void add(float item) {
        add(Float.toString(item));
    }

    public long estimateNDV() {
        return (1L << split) * size;
    }

    public Synopsis getSynopsis() {
        return (new Synopsis(sketchMap.keySet(), split));
    }

    public PriorityQueue<Node> getFrequencies() {
        PriorityQueue<Node> nodepq = new PriorityQueue<Node>(this.maxelements, new NodeComparator());
        for (Long key : sketchMap.keySet() ) {
            nodepq.offer(sketchMap.get(key));
        }

        return nodepq;
    }

    public static Synopsis mergeSynopses(Synopsis... synopsises) {
        if ( synopsises.length > 1 ) {
            int maxSplit = 0;
            Set<Long> mergedSet = new HashSet<Long>();

            for (int i=0; i<synopsises.length; ++i) {
                if ( synopsises[i].split > maxSplit )
                    maxSplit = synopsises[i].split;
            }

            long splitMask = 1L << (HASH_SIZE - maxSplit);
            for (int i=0; i<synopsises.length; ++i) {
                Set<Long> s = synopsises[i].synopsis;
                for (Long value : s) {
                    if ( (value & splitMask) == 0 ) {
                        mergedSet.add(value);
                    }
                }
            }
            return (new Synopsis(mergedSet, maxSplit));
        } else {
            return synopsises[0];
        }
    }

    private void printMemoryUsage() {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        System.out.println("Total Memory: " + totalMemory/MEGABYTE + " (MB)");
        System.out.println("Free  Memory: " + freeMemory/MEGABYTE + " (MB)");
        System.out.println("Used  Memory: " + usedMemory/MEGABYTE + " (MB)");
    }

    public static void main(String[] args) {
        CardinalitySketch cardinalitySketch = new CardinalitySketch();
        long startTime = System.currentTimeMillis();
        for (int i=0; i<26829201; ++i) {
            cardinalitySketch.add(i);
        }

        System.out.println("Elapsed Time: " + (System.currentTimeMillis() - startTime) + " ms");

        System.out.println("Size: " + cardinalitySketch.estimateNDV());
        System.out.println("Split: " + cardinalitySketch.getSplit());
        //System.out.println(cardinalitySketch.getSynopsis().toString());
        PriorityQueue<Node> pq = cardinalitySketch.getFrequencies();
        Node n = pq.poll();

        while ( n != null ) {
            System.out.println(n.toString());
            n = pq.poll();
        }
    }
}