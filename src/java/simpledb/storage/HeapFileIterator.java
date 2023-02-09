package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {

    private final int tableId;
    private final TransactionId tid;
    private final int totalPageNum;
    private int pgNum;
    private Iterator<Tuple> iterator;

    public HeapFileIterator(int tableId, TransactionId tid, int totalPageNum) {
        this.tableId = tableId;
        this.tid = tid;
        this.totalPageNum = totalPageNum;
        this.pgNum = -1;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        openNextPage();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (iterator == null) {
            // not open
            return false;
        }
        if (iterator.hasNext()) {
            // 遍历当前page
            return true;
        }
        if (pgNum >= totalPageNum-1) {
            // 已经是最后一个page
            return false;
        }
        // 下一个page
        openNextPage();
        return hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (iterator == null) {
            // not open
            throw new NoSuchElementException("Should open iterator first");
        }
        return iterator.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // 重置
        pgNum = -1;
        open();
    }

    @Override
    public void close() {
        iterator = null;
    }

    // 获取要读的下一个page的iterator
    private void openNextPage() throws TransactionAbortedException, DbException {
        HeapPageId pid = new HeapPageId(tableId, ++pgNum);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        iterator = page.iterator();
    }
}
