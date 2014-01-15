import oracle.sql.ROWID;
import java.util.Comparator;

/**
 * Created by ap349 on 12/6/13.
 */
public class RowidMap implements Comparable<RowidMap> {
    private ROWID rowid;
    private long  value;
    private Object object;
    private long  count;
    private int   pqidentifier;

    public RowidMap(ROWID rowid, long value, long count, int pqidentifier) {
        this.rowid = rowid;
        this.value = value;
        this.count = count;
        this.object = null;
        this.pqidentifier = pqidentifier;
    }

    public RowidMap(ROWID rowid, Object value, long count, int pqidentifier) {
        this.rowid = rowid;
        this.count = count;
        this.object = value;
        this.pqidentifier = pqidentifier;
    }

    public ROWID getRowid() {
        return rowid;
    }

    public void setRowid(ROWID rowid) {
        this.rowid = rowid;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getPqidentifier() {
        return this.pqidentifier;
    }

    public void setPqidentifier (int pqidentifier) {
        this.pqidentifier = pqidentifier;
    }

    public Object getObject() { return this.object; }

    public void setObject(Object object) { this.object = object; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RowidMap rowidMap = (RowidMap) o;

        if (value != rowidMap.value) return false;
        return true;
    }

    @Override
    public String toString() {
        return "RowidMap{" +
                "rowid=" + rowid +
                ", value=" + value +
                ", count=" + count +
                '}';
    }

    @Override
    public int hashCode() {
        int result = rowid.hashCode();
        result = 31 * result + (int) (value ^ (value >>> 32));
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }

    @Override
    public int compareTo(RowidMap map) {
        if ( this.count > map.getCount() ) return 1;
        if ( this.count < map.getCount() ) return -1;
        return 0;
    }

    public static Comparator<RowidMap> RowidMapComparator
             = new Comparator<RowidMap>() {
        @Override
        public int compare(RowidMap map1, RowidMap map2) {
            if ( map1.getCount() > map2.getCount() ) return 1;
            if ( map1.getCount() < map2.getCount() ) return -1;
            return 0;
        }
    };
}
