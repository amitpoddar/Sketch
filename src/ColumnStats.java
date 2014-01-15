/**
 * Created by ap349 on 12/18/13.
 */
public class ColumnStats {
    private TopK topk;
    private CardinalitySketch cardinalitySketch;
    private String columnName;
    private String columnClassName;
    private long numRows;
    private long numNulls;
    private boolean doingLossyCounting;

    public ColumnStats() {
        this.numNulls = 0;
        this.numRows = 0;
        doingLossyCounting = false;
    }

    public String getColumnClassName() {
        return columnClassName;
    }

    public void setColumnClassName(String columnClassName) {
        this.columnClassName = columnClassName;
    }

    public TopK getTopk() {
        return topk;
    }

    public void setTopk(TopK topk) {
        this.topk = topk;
    }

    public CardinalitySketch getCardinalitySketch() {
        return cardinalitySketch;
    }

    public void setCardinalitySketch(CardinalitySketch cardinalitySketch) {
        this.cardinalitySketch = cardinalitySketch;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public void incrementNumRows() {
        ++this.numRows;
    }

    public void setDoingLossyCounting(boolean doingLossyCounting) {
        this.doingLossyCounting = doingLossyCounting;
    }

    public boolean isDoingLossyCounting() {
        return this.doingLossyCounting;
    }

    public void incrementNumNulls() {
        ++this.numNulls;
    }
}
