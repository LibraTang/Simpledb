package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private int aFieldIndex;
    private int gFieldIndex;
    private Aggregator.Op op;
    private TupleDesc td;
    private Aggregator aggregator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of aFieldIndex, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param aFieldIndex The column over which we are computing an aggregate.
     * @param gFieldIndex The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param op    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int aFieldIndex, int gFieldIndex, Aggregator.Op op) {
        // some code goes here
        this.child = child;
        this.aFieldIndex = aFieldIndex;
        this.gFieldIndex = gFieldIndex;
        this.op = op;
        this.td = child.getTupleDesc();

        Type gFieldType = td.getFieldType(gFieldIndex);
        Type aFieldType = td.getFieldType(aFieldIndex);
        if (aFieldType == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gFieldIndex, gFieldType, aFieldIndex, op);
        } else {
            this.aggregator = new StringAggregator(gFieldIndex, gFieldType, aFieldIndex, op);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gFieldIndex;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return td.getFieldName(gFieldIndex);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return this.aFieldIndex;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return td.getFieldName(aFieldIndex);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op op) {
        return op.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(op) (child_td.getFieldName(aFieldIndex))" where op and aFieldIndex are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return null;
    }

    public void close() {
        // some code goes here
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
