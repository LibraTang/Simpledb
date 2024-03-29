package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

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
    // aggregator according to operator
    private AbstractIntegerAggregator aggregator;

    /**
     * Aggregate constructor
     * 
     * @param gbFieldIndex
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbFieldType
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param aFieldIndex
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbFieldIndex, Type gbFieldType, int aFieldIndex, Op what) {
        // some code goes here
        this.gbFieldIndex = gbFieldIndex;
        this.gbFieldType = gbFieldType;
        this.aFieldIndex = aFieldIndex;
        this.what = what;
        getAggregator();
    }

    private void getAggregator() {
        switch (what) {
            case COUNT:
                this.aggregator = new IntegerCountAggregator();
                break;
            case SUM:
                this.aggregator = new IntegerSumAggregator();
                break;
            case AVG:
                this.aggregator = new IntegerAvgAggregator();
                break;
            case MIN:
                this.aggregator = new IntegerMinAggregator();
                break;
            case MAX:
                this.aggregator = new IntegerMaxAggregator();
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField;
        if (this.gbFieldIndex == NO_GROUPING) {
            gbField = DEFAULT_FIELD;
        } else {
            gbField = tup.getField(this.gbFieldIndex);
        }
        IntField aField = (IntField) tup.getField(aFieldIndex);
        this.aggregator.apply(gbField, aField.getValue());
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Map<Field, Integer> result = this.aggregator.result();
        List<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        if (this.gbFieldIndex != NO_GROUPING) {
            td = new TupleDesc(new Type[] {gbFieldType, Type.INT_TYPE});
            result.forEach((k, v) -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, k);
                tuple.setField(1, new IntField(v));
                tuples.add(tuple);
            });
        } else {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            result.forEach((k, v) -> {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(v));
                tuples.add(tuple);
            });
        }
        return new TupleIterator(td, tuples);
    }

}

abstract class AbstractIntegerAggregator {
    // 按 group-by field 组织的map
    protected Map<Field, Integer> groups = new HashMap<>();

    public abstract void apply(Field group, Integer value);

    public Map<Field, Integer> result() {
        return groups;
    }
}

class IntegerCountAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (k, v) -> {
            if (v == null) {
                return 1;
            } else {
                return v + 1;
            }
        });
    }
}

class IntegerSumAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (k, v) -> {
           if (v == null) {
               return value;
           } else {
               return v + value;
           }
        });
    }
}

class IntegerAvgAggregator extends AbstractIntegerAggregator {
    private final IntegerSumAggregator sumAggregator = new IntegerSumAggregator();
    private final IntegerCountAggregator countAggregator = new IntegerCountAggregator();

    @Override
    public void apply(Field group, Integer value) {
        sumAggregator.apply(group, value);
        countAggregator.apply(group, value);
    }

    @Override
    public Map<Field, Integer> result() {
        Map<Field, Integer> sumResult = sumAggregator.result();
        Map<Field, Integer> countResult = countAggregator.result();

        sumResult.forEach((group, sum) -> groups.put(group, sum / countResult.get(group)));

        return groups;
    }
}

class IntegerMinAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (k, v) -> {
            if (v == null) {
                v = Integer.MAX_VALUE;
            }
            return Math.min(value, v);
        });
    }
}

class IntegerMaxAggregator extends AbstractIntegerAggregator {
    @Override
    public void apply(Field group, Integer value) {
        groups.compute(group, (k, v) -> {
            if (v == null) {
                v = Integer.MIN_VALUE;
            }
            return Math.max(value, v);
        });
    }
}
