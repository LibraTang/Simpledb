package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private static final Field DEFAULT_FIELD = new StringField("Default", 10);

    // group-by field index
    private final int gbFieldIndex;
    // group-by field type
    private final Type gbFieldType;
    // aggregate field
    private final int aFieldIndex;
    // aggregation operator
    private final Op what;
    // 按 group-by field 组织的map
    private Map<Field, Integer> groups;

    /**
     * Aggregate constructor
     * @param gbFieldIndex the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aFieldIndex the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    public StringAggregator(int gbFieldIndex, Type gbFieldType, int aFieldIndex, Op what) {
        // some code goes here
        this.gbFieldIndex = gbFieldIndex;
        this.gbFieldType = gbFieldType;
        this.aFieldIndex = aFieldIndex;
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("only supports COUNT");
        }
        this.what = what;
        this.groups = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // COUNT
        Field gbField = this.gbFieldIndex == NO_GROUPING ? DEFAULT_FIELD : tup.getField(this.gbFieldIndex);
        groups.compute(gbField, (k, v) -> {
            if (v == null) {
                return 1;
            } else {
                return v + 1;
            }
        });
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        if (this.gbFieldIndex != NO_GROUPING) {
            td = new TupleDesc(new Type[] {gbFieldType, Type.INT_TYPE});
            groups.forEach((k, v) -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, k);
                tuple.setField(1, new IntField(v));
                tuples.add(tuple);
            });
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            groups.forEach((k, v) -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(v));
                tuples.add(tuple);
            });
        }
        return new TupleIterator(td, tuples);
    }

}
