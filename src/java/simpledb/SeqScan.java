package simpledb;

import java.util.*;

import simpledb.TupleDesc.TDItem;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {
	
	private String tableAlias;
	private TransactionId tid;
	private int tableid;
	private DbFileIterator iter = null;
    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableAlias = tableAlias;
        this.tableid = tableid;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableAlias = tableAlias;
        this.tableid = tableid;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        iter = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
        iter.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        TupleDesc result;
        Iterator<TDItem> iter = td.iterator();
        
        Type fieldType;
        String fieldName;
        Type[] aliasFieldTypeArr;
        String[] aliasFieldNameArr;
        ArrayList<String> nameArr = new ArrayList<String>();
        ArrayList<Type> typeArr = new ArrayList<Type>();
        
        String alias = getAlias();
        if(alias == null)
        	alias = this.getTableName();
        
        while(iter.hasNext()){
        	TDItem tditem = iter.next();
        	fieldType = tditem.fieldType;
        	fieldName = tditem.fieldName;
        	
        	if (fieldName=="N/A"){
        		fieldName = "null";
        	}
        	
        	fieldName = alias+"."+fieldName;
        	nameArr.add(fieldName);
        	typeArr.add(fieldType);
          }
        
        aliasFieldTypeArr = typeArr.toArray(new Type[typeArr.size()]);
        aliasFieldNameArr = nameArr.toArray(new String[nameArr.size()]);
        result = new TupleDesc(aliasFieldTypeArr, aliasFieldNameArr);
        
      
        
        return result;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(iter == null)
        	return false;
        
    	if(iter.hasNext())
        	return true;
        
        return false;
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if(iter == null){
        	throw new NoSuchElementException("The iterator is null/empty");
        }
        Tuple tuple = iter.next();
        
        if(tuple == null){
        	throw new NoSuchElementException("Tuple not found yo");
        }
        else{
        
        return tuple;
        
        }
    }

    public void close() {
        
    	iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iter.close();
        iter.open();
    }
}
