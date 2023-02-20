package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

import static java.lang.Math.max;
import static java.lang.Math.min;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int min;
    private int max;
    private int[] heights;
    private int totalTuples;
    private int width;
    private int lastBucketWidth;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.heights = new int[buckets];
        this.totalTuples = 0;
        this.width = max((max-min+1) / buckets, 1);
        this.lastBucketWidth = (max-min+1) - width*(buckets-1);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        if (v < min || v > max) {
            return;
        }
        // 计算v所在的bucket下标
        int bucketIndex = (v - min) / width;
        if (bucketIndex >= buckets) {
            // 超出范围
            return;
        }
        totalTuples++;
        heights[bucketIndex]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double res;
        // 计算v所在的bucket下标
        int bucketIndex = min((v - min) / width, buckets-1);
        // v所在的bucket的宽度
        int bucketWidth = bucketIndex == buckets-1 ? lastBucketWidth : width;
        // 分类讨论
        switch (op) {
            case EQUALS:
                res = estimateEqual(v, bucketIndex, bucketWidth);
                break;
            case GREATER_THAN:
                res = estimateGreater(v, bucketIndex, bucketWidth);
                break;
            case GREATER_THAN_OR_EQ:
                res = estimateGreater(v, bucketIndex, bucketWidth) + estimateEqual(v, bucketIndex, bucketWidth);
                break;
            case LESS_THAN:
                res = 1.0 - estimateGreater(v, bucketIndex, bucketWidth) - estimateEqual(v, bucketIndex, bucketWidth);
                break;
            case LESS_THAN_OR_EQ:
                res = 1.0 - estimateGreater(v, bucketIndex, bucketWidth);
                break;
            case NOT_EQUALS:
                res = 1.0 - estimateEqual(v, bucketIndex, bucketWidth);
                break;
            default:
                return -1;
        }
        return res;
    }

    private double estimateEqual(int v, int bucketIndex, int bucketWidth) {
        if (v < min || v > max) {
            return 0;
        }
        double groupTuples = heights[bucketIndex];
        return (groupTuples/bucketWidth)/totalTuples;
    }

    private double estimateGreater(int v, int bucketIndex, int bucketWidth) {
        if (v < min) {
            return 1;
        } else if (v >= max) {
            return 0;
        }
        // 当前bucket中的最大值（最右边）
        int bucketRight = (bucketIndex+1) * width + min - 1;
        double groupTuples = heights[bucketIndex];
        // 估算在该组中大于v的个数
        double greaterInGroup = ((double) (bucketRight - v) / bucketWidth) * groupTuples;
        // 加上其他组中大于v的个数
        double greaterTotal = greaterInGroup;
        for (int i = bucketIndex+1; i < buckets; i++) {
            greaterTotal += heights[i];
        }
        return greaterTotal/totalTuples;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "IntHistogram{" + "max=" + max + ", min=" + min + ", heights=" + Arrays.toString(heights)
                + ", buckets=" + buckets + ", totalTuples=" + totalTuples + ", width=" + width + ", lastBucketWidth="
                + lastBucketWidth + '}';
    }
}
