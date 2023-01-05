package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pid;
    private final int tupleNum;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleNum
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleNum) {
        // some code goes here
        this.pid = pid;
        this.tupleNum = tupleNum;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleNum;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (o == this) {
            return true;
        }
        if (o instanceof RecordId) {
            RecordId recordId = (RecordId) o;
            return this.pid.equals(recordId.getPageId()) && this.tupleNum == recordId.getTupleNumber();
        } else {
            return false;
        }
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        int result = 17;
        result += result * 31 + pid.hashCode();
        result += result * 31 + tupleNum;
        return result;
    }

}
