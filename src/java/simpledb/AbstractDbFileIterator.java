package simpledb;

import java.util.NoSuchElementException;

/**
 * Abstract Class that helps in the implementation of an iterator over tuples in an HeapFile.
 * @author Joe Jean
 *
 */

public abstract class AbstractDbFileIterator implements DbFileIterator {

	public boolean hasNext() throws DbException, TransactionAbortedException {
		if (next == null) 
			next = readNext();
		return next != null;
	}
	public Tuple next() throws DbException, TransactionAbortedException,
	NoSuchElementException {
		if (next == null) {
			next = readNext();
		if (next == null) 
			throw new NoSuchElementException();
		}
		Tuple result = next;
		next = null;
		return result;
	}
	/** Close the iterator */
	public void close() {
	
		next = null;
	}
	
	/** 
	* @return the next Tuple in the iterator, null if the iteration is done.
	*  */
	protected abstract Tuple readNext() throws DbException, TransactionAbortedException;
	private Tuple next = null;
}
