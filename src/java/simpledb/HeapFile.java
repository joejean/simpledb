package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	File fileOnDisk;
	TupleDesc td;
	int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.fileOnDisk = f;
        this.tableId = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        
        return this.fileOnDisk;
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
        return this.tableId;
//        throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
//        throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
    	
    	HeapPageId id = (HeapPageId) pid;
    	BufferedInputStream bis = null;
    	
    	try {
    	
    	//The Heapfile is converted to a BufferedInputStream, so the content can be read as byte.
    	bis = new BufferedInputStream(new FileInputStream(fileOnDisk));
    	byte pageBuffer[] = new byte [BufferPool.PAGE_SIZE];
    	//the skip method skips over n number of bytes and returns the actual # of bytes to be skipped
    	if (bis.skip(id.pageNumber() * BufferPool.PAGE_SIZE) != id
    	.pageNumber() * BufferPool.PAGE_SIZE) {
    	throw new IllegalArgumentException(
    	"Cannot seek to correct place in the Heapfile");
    	}
    	int returnVal = bis.read(pageBuffer, 0, BufferPool.PAGE_SIZE);
    	if (returnVal == -1) {
    	throw new IllegalArgumentException("Reached end of the table");
    	}
    	if (returnVal < BufferPool.PAGE_SIZE) {
    	throw new IllegalArgumentException("Cannot read "
    	+ BufferPool.PAGE_SIZE + " bytes from Heapfile");
    	}
    	
    	HeapPage p = new HeapPage(id, pageBuffer);
    	return p;
    	} catch (IOException e) {
    	throw new RuntimeException(e);
    	} finally {
    	// Close the file no matter what; success or error
	    	try {
	    	if (bis != null)
	    	bis.close();
	    	} catch (IOException ioe) {
	    	// Ignore failures closing the file
	    	}
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
    	return (int) (fileOnDisk.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
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
    	return new HeapFileIterator(this, tid);
    }

}

/**
* This is a helper class which implements the Java Iterator for tuples 
* that are inside a HeapFile.
*/
class HeapFileIterator extends AbstractDbFileIterator {
		TransactionId tid;
		HeapFile heapFile;
		int currentPageNum = 0;
		Iterator<Tuple> iter = null;
		
		
		
		public HeapFileIterator(HeapFile hf, TransactionId tid) {
			this.heapFile = hf;
			this.tid = tid;
		}
		public void open() throws DbException, TransactionAbortedException {
			currentPageNum = -1;
		}
		@Override
		protected Tuple readNext() throws TransactionAbortedException, DbException {
			
		if (iter != null && !iter.hasNext())
			iter = null;
		
		while (iter == null && currentPageNum < heapFile.numPages() - 1) {
			currentPageNum++;
			HeapPageId curpid = new HeapPageId(heapFile.getId(), currentPageNum);
			HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
			curpid, Permissions.READ_ONLY);
			iter = curp.iterator();
			
			if (!iter.hasNext())
				iter = null;
		}
		
		if (iter == null)
			return null;
		
		return iter.next();
		}
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}
		public void close() {
			super.close();
			iter = null;
			currentPageNum = Integer.MAX_VALUE;
		}
}
