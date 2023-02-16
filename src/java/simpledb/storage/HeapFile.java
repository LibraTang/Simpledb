package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    // 先计算出Page在内存的位置offset
    // 通过RandomAccessFile随机访问HeapFile，读出对应的Page数据，构造HeapPage对象并返回
    public Page readPage(PageId pid) {
        // some code goes here
        // page的起始位置
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        try (RandomAccessFile rf = new RandomAccessFile(file, "r")) {
            byte[] data = new byte[BufferPool.getPageSize()];
            rf.seek(offset);
            rf.readFully(data, 0, data.length);
            return new HeapPage(new HeapPageId(getId(), pid.getPageNumber()), data);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // page的起始位置
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        try (RandomAccessFile rf = new RandomAccessFile(file, "rw")) {
            byte[] data = page.getPageData();
            rf.seek(offset);
            rf.write(data);
        } catch (IOException e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> dirtyPageList = new ArrayList<>();
        // 寻找一个有空位的page并插入
        for (int i=0; i<this.numPages(); i++) {
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if (page != null && page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                dirtyPageList.add(page);
                return dirtyPageList;
            }
        }
        // 所有page都满了，需要创建一个新的page
        HeapPageId pid = new HeapPageId(this.getId(), this.numPages());
        HeapPage newPage = new HeapPage(pid, HeapPage.createEmptyPageData());
        // 将新的page写入disk
        writePage(newPage);
        // 从BufferPool读取page
        newPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        newPage.insertTuple(t);
        dirtyPageList.add(newPage);
        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> dirtyPageList = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        if (page != null && page.isSlotUsed(t.getRecordId().getTupleNumber())) {
            page.deleteTuple(t);
            dirtyPageList.add(page);
        }
        return dirtyPageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(getId(), tid, numPages());
    }

}

