package simpledb;

import java.io.Serializable;
import java.util.*;

import com.sun.org.apache.bcel.internal.generic.INSTANCEOF;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	private final ArrayList<TDItem> tupleDescArr = new ArrayList<TDItem>();
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
       
        return tupleDescArr.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */   
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	
    	
        for (int i=0; i< typeAr.length; i++){
        	String a = new String();
        	
        	if(fieldAr[i]!= null){
        		
        	a = fieldAr[i];
        	
        	TDItem tditem = new TDItem(typeAr[i], a);
        	
        	tupleDescArr.add(tditem);
        	
        	}
        	
        	else{
        		
            	TDItem tditem = new TDItem(typeAr[i], "N/A");
            	tupleDescArr.add(tditem);
        		
        	}
        	
        }
    	
       
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	
        for (int i=0; i< typeAr.length; i++){
        	
        	//Create TDItem element
        	TDItem tditem = new TDItem(typeAr[i], "N/A");
        	tupleDescArr.add(tditem);
        	
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
     
        return tupleDescArr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
    	String name;
    	
    	if ( i > numFields()){
    		throw new NoSuchElementException();
    	}
    	
    	else{
    		
    		 name = tupleDescArr.get(i).fieldName;
    	}
        return name;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        Type fieldType;
        
        
    	if ( i > numFields()){
    		throw new NoSuchElementException();
    	}
    	
    	else{
    		
    		 fieldType = tupleDescArr.get(i).fieldType;
    	}
        return fieldType;
        
        
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
       
        Iterator<TDItem> iter = tupleDescArr.iterator();
    	while(iter.hasNext()){
    		TDItem current = iter.next();
    		if(current.fieldName.equals(name)){
    			
    			return  tupleDescArr.indexOf(current);
    		}
    		
    	}
    	
    	
    	throw new NoSuchElementException();
    	
        
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        Iterator<TDItem> iter = tupleDescArr.iterator();
        while(iter.hasNext()){
        	size += iter.next().fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	 
    	 Iterator<TDItem> iter1 = td1.iterator();
    	 Iterator<TDItem> iter2 = td2.iterator();
    	 Type[] typeArr = new Type[td1.numFields() + td2.numFields()];
    	 String[] nameArr = new String[td1.numFields() + td2.numFields()];
    	 int i = 0;
    	 while (iter1.hasNext()) {
    	 TDItem curr = iter1.next();
    	 nameArr[i] = curr.fieldName;
    	 typeArr[i] = curr.fieldType;
    	 i++;
    	 }
    	 
    	 while (iter2.hasNext()) {
    	 TDItem curr = iter2.next();
    	 nameArr[i] = curr.fieldName;
    	 typeArr[i] = curr.fieldType;
    	 i++;
    	 }
    	 TupleDesc td = new TupleDesc(typeArr, nameArr);
    	 return td;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o == null || !(o instanceof TupleDesc)){
    		return false;
    	}
    	
    	Iterator<TDItem> iter1 = ((TupleDesc) o).iterator();
    	
    	Iterator<TDItem> iter2 = this.iterator();
    	
    	while (iter1.hasNext() && iter2.hasNext()){
    		Type current1 = iter1.next().fieldType;
    		Type current2 = iter2.next().fieldType;
    		if(current1.getLen() != current2.getLen()){
    			return false;
    		}
    	}
        
        return !(iter1.hasNext() || iter2.hasNext());
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	Iterator<TDItem> iter = tupleDescArr.iterator();
    	String result = " ";
    	int i = 0;
        while(iter.hasNext()){
            TDItem item = iter.next();
        	result.concat(item.fieldType+"["+i+"]("+item.fieldName+"["+i+"])");
        	i++;
        }
        
        return result;
    }
}
