/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.source.DataSourceException;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.source.DataSourceMetaData.StorageSystem;
import edu.ucsd.forward.data.source.SchemaObject;
import edu.ucsd.forward.data.source.SchemaObjectHandle;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.QueryExecutionException;
import edu.ucsd.forward.query.QueryProcessorFactory;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.util.NameGenerator;

/**
 * This class implements absolute variables for the logical plan, that is, variables that denote a specific data source and data
 * object in the unified application state.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class AbsoluteVariable extends AbstractVariable
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbsoluteVariable.class);
    
    private SchemaObjectHandle  m_handle;
    
    /**
     * Constructor.
     * 
     * @param data_source_name
     *            the name of the data source.
     * @param schema_object_name
     *            the name of the schema object.
     */
    public AbsoluteVariable(String data_source_name, String schema_object_name)
    {
        super(data_source_name + NameGenerator.NAME_PREFIX + schema_object_name, schema_object_name);
        
        m_handle = new SchemaObjectHandle(data_source_name, schema_object_name);
    }
    
    /**
     * Default constructor.
     */
    public AbsoluteVariable()
    {
        
    }
    
    /**
     * Returns the data source name of the variable.
     * 
     * @return the data source name of the variable.
     */
    public String getDataSourceName()
    {
        return m_handle.getDataSourceName();
    }
    
    /**
     * Returns the schema object name of the variable.
     * 
     * @return the schema object name of the variable.
     */
    public String getSchemaObjectName()
    {
        return m_handle.getSchemaObjectName();
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        if (this.getType() != null) return this.getType();
        
        DataSource data_source = null;
        SchemaObject schema_obj = null;
        try
        {
            // Retrieve the schema object
            data_source = QueryProcessorFactory.getInstance().getDataSourceAccess(m_handle.getDataSourceName()).getDataSource();
            schema_obj = data_source.getSchemaObject(m_handle.getSchemaObjectName());
        }
        catch (QueryExecutionException e)
        {
            // Chain the exception
            throw new QueryCompilationException(QueryCompilation.INVALID_ACCESS, this.getLocation(), e);
        }
        
        this.setType(schema_obj.getSchemaTree().getRootType());
        
        return this.getType();
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        // Same name
        if (metadata.getName().equals(m_handle.getDataSourceName()))
        {
            return true;
        }
        
        // Retrieve the data source name metadata
        DataSourceMetaData var_metadata = null;
        try
        {
            var_metadata = QueryProcessorFactory.getInstance().getDataSourceMetaData(m_handle.getDataSourceName());
        }
        catch (DataSourceException e)
        {
            assert (false);
        }
        
        // Different name but the variable targets an in-memory data source
        if (var_metadata.getStorageSystem() == StorageSystem.INMEMORY)
        {
            return true;
        }
        
        return false;
    }
    
    @Override
    public Term copy()
    {
        AbsoluteVariable copy = new AbsoluteVariable(m_handle.getDataSourceName(), m_handle.getSchemaObjectName());
        super.copy(copy);
        
        return copy;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.query.logical.term.AbstractTerm#copyWithoutType()
     */
    @Override
    public Term copyWithoutType()
    {
        return new AbsoluteVariable(getDataSourceName(), getSchemaObjectName());
    }
}
