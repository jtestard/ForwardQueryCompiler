/**
 * 
 */
package edu.ucsd.forward.data.source;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.exception.ExceptionMessages;
import edu.ucsd.forward.exception.ExceptionMessages.QueryExecution;
import edu.ucsd.forward.query.QueryExecutionException;

/**
 * Abstract class for implementing data source transactions.
 * 
 * @author Kian Win
 * @author Yupeng
 */
public abstract class AbstractDataSourceTransaction implements DataSourceTransaction
{
    private static final Logger       log = Logger.getLogger(AbstractDataSourceTransaction.class);
    
    /**
     * The state of the transaction.
     */
    private TransactionState          m_state;
    
    /**
     * The temporary schema objects hosted by the connection.
     */
    private Map<String, SchemaObject> m_temp_schema_objects;
    
    /**
     * Constructs a data source transaction.
     */
    protected AbstractDataSourceTransaction()
    {
        m_state = TransactionState.INACTIVE;
        m_temp_schema_objects = new HashMap<String, SchemaObject>();
    }
    
    @Override
    public TransactionState getState()
    {
        return m_state;
    }
    
    /**
     * Sets the state of the transaction.
     * 
     * @param state
     *            the new state of the transaction.
     */
    protected void setTransactionState(TransactionState state)
    {
        assert (state != null);
        
        assert (isValidNewState(state));
        
        m_state = state;
    }
    
    /**
     * Determines if the new state of the transaction is valid.
     * 
     * @param new_state
     *            the new transaction state.
     * @return true, if the new transaction state is valid; otherwise, false.
     */
    private boolean isValidNewState(TransactionState new_state)
    {
        switch (m_state)
        {
            case INACTIVE:
                if (new_state == TransactionState.INACTIVE) return false;
                break;
            case ACTIVE:
                if (new_state == TransactionState.INACTIVE) return false;
                if (new_state == TransactionState.ACTIVE) return false;
                break;
            case COMMITED:
                return false;
            case ABORTED:
                return false;
        }
        
        return true;
    }
    
    /**
     * Begins the transaction.
     */
    protected void begin()
    {
        this.setTransactionState(TransactionState.ACTIVE);
        ((AbstractDataSource) this.getDataSource()).addTransaction(this);
    }
    
    @Override
    public void commit() throws QueryExecutionException
    {
        assert (m_state == TransactionState.ACTIVE);
        
        if (!m_temp_schema_objects.isEmpty())
        {
            log.warn("Encountered temporary data objects when commiting a transaction in data source: "
                    + this.getDataSource().getMetaData().getName());
        }
        
        // Drop the temporary schema objects
        for (String name : new ArrayList<String>(m_temp_schema_objects.keySet()))
        {
            this.removeTemporarySchemaObject(name);
        }
        
        this.setTransactionState(TransactionState.COMMITED);
        ((AbstractDataSource) this.getDataSource()).removeTransaction(this);
    }
    
    @Override
    public void rollback() throws QueryExecutionException
    {
        assert (m_state == TransactionState.ACTIVE);
        this.setTransactionState(TransactionState.ABORTED);
        
        // Drop the temporary schema objects
        for (String name : new ArrayList<String>(m_temp_schema_objects.keySet()))
        {
            this.removeTemporarySchemaObject(name);
        }
        
        ((AbstractDataSource) this.getDataSource()).removeTransaction(this);
    }
    
    @Override
    public boolean hasTemporarySchemaObject(String name)
    {
        return m_temp_schema_objects.containsKey(name);
    }
    
    @Override
    public SchemaObject getTemporarySchemaObject(String name) throws QueryExecutionException
    {
        assert (name != null);
        
        if (!m_temp_schema_objects.containsKey(name))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_SCHEMA_OBJ_NAME, name,
                                                      this.getDataSource().getMetaData().getName());
            
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.GET_SCHEMA_OBJ_ERROR, cause, name,
                                              this.getDataSource().getMetaData().getName());
        }
        
        return m_temp_schema_objects.get(name);
    }
    
    @Override
    public Collection<SchemaObject> getTemporarySchemaObjects()
    {
        return Collections.unmodifiableCollection(m_temp_schema_objects.values());
    }
    
    /**
     * Adds a temporary schema object to the connection.
     * 
     * @param schema_obj
     *            the temporary schema object to add.
     * @exception QueryExecutionException
     *                if there is an existing temporary schema object with the same name.
     */
    public void addTemporarySchemaObject(SchemaObject schema_obj) throws QueryExecutionException
    {
        assert (schema_obj != null);
        assert (schema_obj.getScope() == SchemaObjectScope.TEMPORARY);
        
        if (this.hasTemporarySchemaObject(schema_obj.getName()))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.INVALID_NEW_SCHEMA_OBJ_NAME,
                                                      schema_obj.getName(), this.getDataSource().getMetaData().getName());
            
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.CREATE_SCHEMA_OBJ_ERROR, cause, schema_obj.getName(),
                                              this.getDataSource().getMetaData().getName());
        }
        
        log.trace("Created transient schema object: " + this.getDataSource().getMetaData().getName() + "." + schema_obj.getName());
        
        m_temp_schema_objects.put(schema_obj.getName(), schema_obj);
    }
    
    @Override
    public SchemaObject removeTemporarySchemaObject(String name) throws QueryExecutionException
    {
        assert (name != null);
        
        if (!m_temp_schema_objects.containsKey(name))
        {
            Throwable cause = new DataSourceException(ExceptionMessages.DataSource.UNKNOWN_SCHEMA_OBJ_NAME, name,
                                                      this.getDataSource().getMetaData().getName());
            
            // Chain the exception
            throw new QueryExecutionException(QueryExecution.DROP_SCHEMA_OBJ_ERROR, cause, name,
                                              this.getDataSource().getMetaData().getName());
        }
        
        log.trace("Dropped transient schema object: " + this.getDataSource().getMetaData().getName() + "." + name);
        
        return m_temp_schema_objects.remove(name);
    }
    
}
