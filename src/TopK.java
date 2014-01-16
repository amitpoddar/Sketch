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
 * Created by ap349 on 12/17/13.
 */
public class TopK {
    private static final int COUNT_SKETCH_DEPTH = 8;
    private static final int COUNT_SKETCH_WIDTH = 32768;

    private int topk;
    private Queue<Integer> identifiers;
    private CountSketch countSketch;
    private HashMap<String, RowidMap> topkMap;
    private HashMap<Integer, RowidMap> topkIdMap;
    private IndexMinPQ<RowidMap> indexMinPQ;

    public void initTopK(int topk, int depth, int width) {
        this.topk = topk;
        this.identifiers = new Queue<Integer>();
        this.countSketch = new CountSketch(depth, width,(int)System.currentTimeMillis());
        this.topkMap = new HashMap<String, RowidMap>();
        this.topkIdMap = new HashMap<Integer, RowidMap>();
        this.indexMinPQ = new IndexMinPQ<RowidMap>(topk + 2);

        for (int i=1; i<=topk+1; ++i) {
            identifiers.enqueue(i);
        }
    }

    public TopK(int topk) {
        initTopK(topk, COUNT_SKETCH_DEPTH, COUNT_SKETCH_WIDTH);
    }

    public TopK(int topk, int depth, int width) {
        initTopK(topk, depth, width);
    }

    public void add(String item, int count, ROWID rowid) {
        countSketch.add(item, count);
        long estimatedCount = countSketch.estimateCount(item);

        if ( topkMap.containsKey(item) ) {
            RowidMap map = topkMap.get(item);
            RowidMap nmap = new RowidMap(map.getRowid(), map.getObject(), estimatedCount, map.getPqidentifier());
            topkMap.put(item, nmap);
            topkIdMap.put(map.getPqidentifier(), nmap);
            indexMinPQ.delete(map.getPqidentifier());
            indexMinPQ.insert(map.getPqidentifier(), nmap);
        } else {
            int identifier = identifiers.dequeue().intValue();
            RowidMap map = new RowidMap(rowid, item, estimatedCount, identifier);
            topkMap.put(item, map);
            topkIdMap.put(identifier, map);
            indexMinPQ.insert(identifier, map);

            if ( indexMinPQ.size() > topk ) {
                String key = indexMinPQ.minKey().getObject().toString();
                topkMap.remove(key);
                int delidentifier = indexMinPQ.delMin();
                topkIdMap.remove(delidentifier);
                identifiers.enqueue(delidentifier);
            }
        }
    }

    public void add(long item, int count, ROWID rowid) {
        add(Long.toString(item), count, rowid);
    }

    public void add(double item, int count, ROWID rowid) {
        add(Double.toString(item), count, rowid);
    }

    public void add(float item, int count, ROWID rowid) {
        add(Float.toString(item), count, rowid);
    }

    public java.util.Stack<RowidMap> getTopKElements() {
        java.util.Stack<RowidMap> stack = new java.util.Stack<RowidMap>();

        Iterator<Integer> iterator = indexMinPQ.iterator();
        while (iterator.hasNext()) {
            int identifier = iterator.next().intValue();
            stack.push(topkIdMap.get(identifier));
        }

        return (stack);
    }

    public HashMap<String,Long> getTopKElementsHash() {
        HashMap<String, Long> map = new HashMap<String, Long>();

        Iterator<Integer> iterator = indexMinPQ.iterator();
        while ( iterator.hasNext() ) {
            int identifier = iterator.next().intValue();
            RowidMap rowidMap = topkIdMap.get(identifier);
            map.put( rowidMap.getObject().toString(), rowidMap.getCount() );
        }

        return map;
    }
}
