/**
 * 
 */
package edu.ucsd.forward.data.source;

import java.util.Collection;

import edu.ucsd.forward.query.QueryExecutionException;

/**
 * A data source transaction.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public interface DataSourceTransaction
{
    /**
     * An enumeration of transaction states.
     */
    public enum TransactionState
    {
        // The transaction has not started yet
        INACTIVE,

        // The transaction has started but has not committed or aborted
        ACTIVE,

        // The transaction has finished and has committed
        COMMITED,

        // The transaction has finished and has aborted
        ABORTED;
    }
    
    /**
     * Returns the data source the transaction is associated with.
     * 
     * @return the data source.
     */
    public DataSource getDataSource();
    
    /**
     * Returns the state of the transaction.
     * 
     * @return the state of the transaction.
     */
    public TransactionState getState();
    
    /**
     * Commits the transaction.
     * 
     * @throws QueryExecutionException
     *             if the transaction fails to commit.
     */
    public void commit() throws QueryExecutionException;
    
    /**
     * Rolls back the transaction.
     * 
     * @throws QueryExecutionException
     *             if the transaction fails to rollback.
     */
    public void rollback() throws QueryExecutionException;
    
    /**
     * Determines whether the connection has a temporary schema object with the given name.
     * 
     * @param name
     *            the name of the temporary schema object.
     * @return true, if the connection has a temporary schema object with the given name; false, otherwise.
     */
    public boolean hasTemporarySchemaObject(String name);
    
    /**
     * Gets a temporary schema object from the connection.
     * 
     * @param name
     *            the name of the temporary schema object to get.
     * @return a schema object.
     * @exception QueryExecutionException
     *                if there is no temporary schema object with the given name.
     */
    public SchemaObject getTemporarySchemaObject(String name) throws QueryExecutionException;
    
    /**
     * Gets the temporary schema objects from the connection.
     * 
     * @return a collection of schema objects.
     */
    public Collection<SchemaObject> getTemporarySchemaObjects();
    
    /**
     * Removes a temporary schema object from the connection.
     * 
     * @param name
     *            the name of the temporary schema object to remove.
     * @return the removed schema object.
     * @exception QueryExecutionException
     *                if there is no temporary schema object with the given name.
     */
    public SchemaObject removeTemporarySchemaObject(String name) throws QueryExecutionException;
    
}
