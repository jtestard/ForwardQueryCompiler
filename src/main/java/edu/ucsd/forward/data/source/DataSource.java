package edu.ucsd.forward.data.source;

import java.util.Collection;
import java.util.List;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.index.KeyRange;
import edu.ucsd.forward.data.source.SchemaObject.SchemaObjectScope;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryResult;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.function.external.ExternalFunction;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;
import edu.ucsd.forward.query.physical.plan.PhysicalPlan;

/**
 * A data source stores schema objects and corresponding data objects and supports transactions on them.
 * 
 * @author Michalis Petropoulos
 * 
 */
public interface DataSource
{
    /**
     * The mediator data source is automatically created when a UAS is created.
     */
    public static final String MEDIATOR           = "mediator";
    
    /**
     * The client-side mediator data source is automatically created when a UAS is created.
     */
    public static final String CLIENT             = "client";
    
    /**
     * An implicit created database source.
     */
    public static final String DB                 = "db";
    
    /**
     * The FPL local scope data source is used during FPL code compilation to resolve attribute references, and during runtime to
     * evaluate local variables. The FPL data source is automatically created when a UAS is created.
     */
    public static final String FPL_LOCAL_SCOPE    = "fpl_local_scope";
    
    /**
     * A special context data object in FPL_LOCAL_SCOPE
     */
    public static final String CONTEXT            = "context";
    
    /**
     * The page data source.
     */
    public static final String PAGE               = "__page";
    
    /**
     * The visual data source.
     */
    public static final String VISUAL             = "__visual";
    
    /**
     * An event data source
     */
    public static final String EVENT              = "event";
    
    /**
     * The source that stores the temp data objects created from the assign operators configured on a plan. The temp source for
     * assign operators is automatically created when a UAS is created.
     */
    public static final String TEMP_ASSIGN_SOURCE = "temp_assign_source";
    
    /**
     * An enumeration of data source states.
     */
    public enum DataSourceState
    {
        OPEN, CLOSED;
    }
    
    /**
     * Gets the data source meta data.
     * 
     * @return the data source meta data.
     */
    public DataSourceMetaData getMetaData();
    
    /**
     * Clears up all the schema objects and data objects.
     * 
     * @throws QueryExecutionException
     *             if deleting schema/data objects fails.
     */
    public void clear() throws QueryExecutionException;
    
    /**
     * Allocates any resources needed by the data source.
     * 
     * @exception QueryExecutionException
     *                if opening the data source raises an exception.
     */
    public void open() throws QueryExecutionException;
    
    /**
     * Releases any resources allocated for the data source and rolls back any active transactions. After this method is called, the
     * data source can be reopened and reused.
     * 
     * @exception QueryExecutionException
     *                if closing the data source raises an exception.
     */
    public void close() throws QueryExecutionException;
    
    /**
     * Gets the state of the data source.
     * 
     * @return the state of the data source.
     */
    public DataSourceState getState();
    
    /**
     * Begins a new transaction for the data source.
     * 
     * @return a new transaction.
     * @exception QueryExecutionException
     *                if beginning a new transaction raises an exception.
     */
    public DataSourceTransaction beginTransaction() throws QueryExecutionException;
    
    /**
     * Creates a schema object in the data source, either persistent or temporary.
     * 
     * @param name
     *            the name of the schema object
     * @param scope
     *            the scope of the schema object.
     * @param schema
     *            the schema tree of the schema object.
     * @param estimate
     *            the cardinality estimation of the data object.
     * @param transaction
     *            the transaction to use for the operation.
     * @return the schema object created.
     * @exception QueryExecutionException
     *                if there is an existing schema object with the same name.
     */
    public SchemaObject createSchemaObject(String name, SchemaObjectScope scope, SchemaTree schema, Size estimate,
            DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Creates a persistent schema object in the data source using an isolated transaction.
     * 
     * @param name
     *            the name of the schema object.
     * @param schema
     *            the schema tree of the schema object.
     * @param estimate
     *            the cardinality estimation of the data object.
     * @return the schema object created.
     * @exception QueryExecutionException
     *                if there is an existing schema object with the same name.
     */
    public SchemaObject createSchemaObject(String name, SchemaTree schema, Size estimate) throws QueryExecutionException;
    
    /**
     * Retrieves all schema objects in the data source that are not temporary.
     * 
     * @return a collection of schema objects.
     */
    public Collection<SchemaObject> getSchemaObjects();
    
    /**
     * Determines whether the data source has a schema object with the given name, either persistent or temporary.
     * 
     * @param name
     *            the name of the schema object.
     * @param transaction
     *            the transaction to use for the operation.
     * @return true, if the data source has a schema object with the given name; false, otherwise.
     */
    public boolean hasSchemaObject(String name, DataSourceTransaction transaction);
    
    /**
     * Determines whether the data source has a persistent schema object with the given name.
     * 
     * @param name
     *            the name of the schema object.
     * @return true, if the data source has a schema object with the given name; false, otherwise.
     */
    public boolean hasSchemaObject(String name);
    
    /**
     * Retrieves a schema object, either persistent or temporary.
     * 
     * @param name
     *            the name of the schema object.
     * @param transaction
     *            the transaction to use for the operation.
     * @return a schema object.
     * @exception QueryExecutionException
     *                if a schema object with the given name does not exist in the data source.
     */
    public SchemaObject getSchemaObject(String name, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Retrieves a persistent schema object.
     * 
     * @param name
     *            the name of the schema object.
     * @return a schema object.
     * @exception QueryExecutionException
     *                if a schema object with the given name does not exist in the data source.
     * @exception QueryExecutionException
     *                if beginning a new transaction raises an exception.
     */
    public SchemaObject getSchemaObject(String name) throws QueryExecutionException;
    
    /**
     * Drops a schema object from the data source, either persistent or temporary. If there is a corresponding data object, it will
     * be deleted as well.
     * 
     * @param name
     *            the name of the schema object to drop.
     * @param transaction
     *            the transaction to use for the operation.
     * @return the dropped schema object.
     * @exception QueryExecutionException
     *                if a schema object with the given name does not exist.
     */
    public SchemaObject dropSchemaObject(String name, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Drops a persistent schema object from the data source using an isolated transaction. If there is a corresponding data object,
     * it will be deleted as well.
     * 
     * @param name
     *            the name of the schema object to drop.
     * @return the dropped schema object.
     * @exception QueryExecutionException
     *                if a transaction fails.
     */
    public SchemaObject dropSchemaObject(String name) throws QueryExecutionException;
    
    /**
     * Retrieves the data tree of a data object, either persistent or temporary.
     * 
     * @param name
     *            the name of the data object.
     * @param transaction
     *            the transaction to use for the operation.
     * @return a data tree, or null if one does not exist.
     * @exception QueryExecutionException
     *                if a data object with the given name does not exist in the data source.
     */
    public DataTree getDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Retrieves a persistent data object using an isolated transaction.
     * 
     * @param name
     *            the name of the data object.
     * @return a data tree, or null if one does not exist.
     * @exception QueryExecutionException
     *                if a data object with the given name does not exist in the data source.
     */
    public DataTree getDataObject(String name) throws QueryExecutionException;
    
    /**
     * Creates an Index on a specific collection of a given data object.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param name
     *            the name of the index name.
     * @param collection_path
     *            the path to the collection
     * @param key_paths
     *            the list of paths to the attributes as the key
     * @param unique
     *            indicates if the duplicates value of a key is allowed.
     * @param method
     *            the index method.
     * @param transaction
     *            the transaction to use for the operation
     * @exception QueryExecutionException
     *                if anything wrong happens during the index creation.
     * 
     */
    public void createIndex(String data_obj_name, String name, SchemaPath collection_path, List<SchemaPath> key_paths,
            boolean unique, IndexMethod method, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Creates an Index on a specific collection of a given data object.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param name
     *            the name of the index.
     * @param collection_path
     *            the path to the collection
     * @param key_paths
     *            the list of paths to the attributes as the key
     * @param unique
     *            indicates if the duplicates value of a key is allowed.
     * @param method
     *            the index method.
     * @exception QueryExecutionException
     *                if anything wrong happens during the index creation.
     */
    public void createIndex(String data_obj_name, String name, SchemaPath collection_path, List<SchemaPath> key_paths,
            boolean unique, IndexMethod method) throws QueryExecutionException;
    
    /**
     * Deletes an Index from a specific collection of a given data object.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param name
     *            the name of the index.
     * @param collection_path
     *            the path to the collection
     * @param transaction
     *            the transaction to use for the operation
     * @exception QueryExecutionException
     *                if anything wrong happens during the index deletion.
     */
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path, DataSourceTransaction transaction)
            throws QueryExecutionException;
    
    /**
     * Deletes an Index from a specific collection of a given data object.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param name
     *            the name of the index.
     * @param collection_path
     *            the path to the collection
     * @exception QueryExecutionException
     *                if anything wrong happens during the index deletion.
     */
    public void deleteIndex(String data_obj_name, String name, SchemaPath collection_path) throws QueryExecutionException;
    
    /**
     * Retrieves records from the the index.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param collection_path
     *            the path to the collection
     * @param name
     *            the name of the index name.
     * @param ranges
     *            the range of the keys
     * @param transaction
     *            the transaction to use for the operation
     * @return the retrieved records from the index.
     * @exception QueryExecutionException
     *                if anything wrong happens during the index operation.
     */
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges,
            DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Retrieves records from the the index.
     * 
     * @param data_obj_name
     *            the name of the data object.
     * @param collection_path
     *            the path to the collection
     * @param name
     *            the name of the index name.
     * @param ranges
     *            the range of the keys
     * @return the retrieved records from the index.
     * @exception QueryExecutionException
     *                if anything wrong happens during the index operation.
     */
    public List<TupleValue> getFromIndex(String data_obj_name, SchemaPath collection_path, String name, List<KeyRange> ranges)
            throws QueryExecutionException;
    
    /**
     * Sets the data tree of a data object, either persistent or temporary. If there is an existing data object, it will be
     * overwritten.
     * 
     * @param name
     *            the name of the data object.
     * @param data_tree
     *            the data tree to set.
     * @param transaction
     *            the transaction to use for the operation.
     * @exception QueryExecutionException
     *                if a data object with the given name does not exist in the data source.
     */
    public void setDataObject(String name, DataTree data_tree, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Sets the data tree of a persistent data object using an isolated transaction. If there is an existing data object, it will be
     * overwritten.
     * 
     * @param name
     *            the name of the data object.
     * @param data_tree
     *            the data tree to set.
     * @exception QueryExecutionException
     *                if a data object with the given name does not exist in the data source.
     */
    public void setDataObject(String name, DataTree data_tree) throws QueryExecutionException;
    
    /**
     * Deletes the data tree of a data object, either persistent or temporary.
     * 
     * @param name
     *            the name of the data object.
     * @param transaction
     *            the transaction to use for the operation.
     * @exception QueryExecutionException
     *                if a data object with the given name does not exist in the data source.
     */
    public void deleteDataObject(String name, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Deletes the data tree of a persistent data object using an isolated transaction.
     * 
     * @param name
     *            the name of the data object.
     * @exception QueryExecutionException
     *                if a data object with the given name does not exist in the data source.
     */
    public void deleteDataObject(String name) throws QueryExecutionException;
    
    /**
     * Adds one external function to the data source.
     * 
     * @param function
     *            the external function to add to the data source.
     */
    public void addExternalFunction(ExternalFunction function);
    
    /**
     * Gets the external function with the specific name.
     * 
     * @param name
     *            the external function name.
     * @return the specified external function.
     * @throws FunctionRegistryException
     *             if the external function name cannot be resolved.
     */
    public ExternalFunction getExternalFunction(String name) throws FunctionRegistryException;
    
    /**
     * Executes a physical query plan lazily and returns a query result that allows the retrieval of the resulting data tree in a
     * pipelined fashion.
     * 
     * @param physical_plan
     *            the physical plan to execute.
     * @param transaction
     *            the transaction to use for the operation.
     * @return a pipeline.
     * @exception QueryExecutionException
     *                if the execution of the input physical query plan raises an exception.
     */
    public QueryResult execute(PhysicalPlan physical_plan, DataSourceTransaction transaction) throws QueryExecutionException;
    
    /**
     * Executes a physical query plan lazily and returns a query result that allows the retrieval of the resulting data tree in a
     * pipelined fashion.
     * 
     * @param physical_plan
     *            the physical plan to execute.
     * @return a pipeline.
     * @exception QueryExecutionException
     *                if the execution of the input physical query plan raises an exception.
     */
    public QueryResult execute(PhysicalPlan physical_plan) throws QueryExecutionException;
    
}
