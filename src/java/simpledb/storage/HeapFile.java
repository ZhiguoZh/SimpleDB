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

    File f;
    TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pgSize = BufferPool.getPageSize();
        if (pid.getTableId() != getId() || pid.getPageNumber() * pgSize >= f.length()) return null;

        try {
            RandomAccessFile raf = new RandomAccessFile(f, "r");
            byte[] data = new byte[pgSize];
            raf.seek(pid.getPageNumber() * pgSize);
            raf.read(data, 0, pgSize);

            return new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber()), data);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (f.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }

}

class HeapFileIterator extends AbstractDbFileIterator {
    Iterator<Tuple> it = null;
    HeapPage curp = null;
    int pageNo = 0;

    final TransactionId tid;
    final HeapFile f;

    public HeapFileIterator(TransactionId tid, HeapFile f) {
        this.tid = tid;
        this.f = f;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pageNo = 0;
        curp = (HeapPage) Database.getBufferPool()
                .getPage(this.tid, new HeapPageId(f.getId(), pageNo), Permissions.READ_ONLY);
        it = curp.iterator();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (it != null && !it.hasNext()) it = null;

        while (it == null && curp != null) {
            if (pageNo + 1 == f.numPages()) curp = null;
            else {
                curp = (HeapPage) Database.getBufferPool()
                        .getPage(this.tid, new HeapPageId(f.getId(), ++pageNo), Permissions.READ_ONLY);
                it = curp.iterator();
                if (!it.hasNext()) it = null;
            }
        }

        return it == null ? null : it.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        super.close();
        pageNo = 0;
        it = null;
        curp = null;
    }
}

