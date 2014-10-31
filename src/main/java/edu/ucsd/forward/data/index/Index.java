/**
 * 
 */
package edu.ucsd.forward.data.index;

import java.util.List;

import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.TupleValue;

/**
 * An index.
 * 
 * @author Yupeng
 * 
 */
public interface Index
{
    /**
     * Retrieves all the records from the index that the key is within the given range. NOTE: this method is only valid for the
     * index that has only one key.
     * 
     * @param ranges
     *            the ranges of the keys in the index.
     * @return the records retrieved from the index.
     */
    public List<TupleValue> get(List<KeyRange> ranges);
    
    /**
     * Gets the index declaration.
     * 
     * @return the index declaration.
     */
    public IndexDeclaration getDeclaration();
    
    /**
     * Loads a collection value into the index.
     * 
     * @param collection
     *            the collection to load.
     */
    public void load(CollectionValue collection);
    
    /**
     * Inserts a new tuple into the index.
     * 
     * @param tuple
     *            tuple to be inserted.
     */
    public void insert(TupleValue tuple);
    
    /**
     * Deletes a tuple from the index.
     * 
     * @param tuple
     *            tuple to be deleted.
     */
    public void delete(TupleValue tuple);
    
    /**
     * Clears the index.
     */
    public void clear();
    
    /**
     * Checks if the index is empty.
     * 
     * @return <code>true</code> if the index is empty, <code>false</code> otherwise.
     */
    public boolean isEmpty();
    
    /**
     * Gets the size of the index.
     * 
     * @return
     */
    public int size();
}
