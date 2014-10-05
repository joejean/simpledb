package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */


public class Catalog {
	
	ArrayList<Table> Catalog_list; 
	
	private class Table {
		public DbFile table_file;
		
		public TupleDesc myTupleDesc;

		public String name;

		public int tableID;

		public String pkeyField; 
		
		public Table(DbFile file_, String name_, String pkeyField_){
			this.table_file = file_;
			this.myTupleDesc = table_file.getTupleDesc();  
			this.name = name_;
			this.tableID = table_file.getId();
			if (pkeyField != null){
				this.pkeyField = pkeyField_;
			}
			else {
				this.pkeyField = "";
			}
		}
		
		public String toString() {
		return name + " (" + "Primary Key: " + pkeyField + "; fileID: " + tableID + ")";
		}
		
	}
	
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    	
    	Catalog_list = new ArrayList<Table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
     
    	
    	Table mytable = new Table(file, name, pkeyField);
    	
    	this.Catalog_list.add(mytable);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
    	int id = 0;
    	if(name == null) 
        	throw new NoSuchElementException();
    	
       
    	for (Table table: Catalog_list)
    	{
    		if(table.name.equals(name)){
    			 id = table.tableID;
    			 return id;
    		
    		}
    		else id = -1;
    		
    	}
        if (id == -1){
        	throw new NoSuchElementException();
        }
        
     return id;
    }
    

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        
    	for (Table table: Catalog_list)
    	{
    		if(table.tableID == tableid){
    			return table.myTupleDesc;
    		}
    	}
        return null;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {

    	for (Table table: Catalog_list)
    	{
    		if(table.tableID == tableid){
    			return table.table_file;
    		}
    	}
        return null;
    }

    public String getPrimaryKey(int tableid) {

    	for (Table table: Catalog_list)
    	{
    		if(table.tableID == tableid){
    			return table.pkeyField;
    		}
    	}
        return null;
    }

    public Iterator<Integer> tableIdIterator() {
        ArrayList<Integer> idArr = new ArrayList<Integer>();
        for (Table table : Catalog_list){
        	idArr.add(table.tableID);
        }
        return idArr.iterator();
    }

    
    public String getTableName(int id) {

    	for (Table table: Catalog_list)
    	{
    		if(table.tableID == id){
    			return table.name;
    		}
    	}
        return null;
    }
    
    /** Delete all tables from the catalog */
    public void clear() {

    	for (Table table: Catalog_list)
    	{
    		Catalog_list.remove(table);
    	}
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

