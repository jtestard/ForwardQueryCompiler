/**
 * 
 */
package edu.ucsd.forward.query.logical.ddl;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * The operator that drops a data object.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class DropDataObject extends AbstractDdlOperator
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DropDataObject.class);
    
    private String              m_schema_name;
    
    private String              m_data_source_name;
    
    /**
     * Constructs the operator with the schema name, and data source name.
     * 
     * @param schema_name
     *            the name of the schema
     * @param data_source_name
     *            the name of the data source where the schema object to be dropped.
     */
    public DropDataObject(String schema_name, String data_source_name)
    {
        super();
        
        assert schema_name != null;
        assert data_source_name != null;
        
        m_schema_name = schema_name;
        m_data_source_name = data_source_name;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private DropDataObject()
    {
        
    }
    
    /**
     * Gets the schema name.
     * 
     * @return the schema name
     */
    public String getSchemaName()
    {
        return m_schema_name;
    }
    
    /**
     * Gets the name of the data source.
     * 
     * @return the name of the data source.
     */
    public String getDataSourceName()
    {
        return m_data_source_name;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitDropSchemaObject(this);
    }
    
    @Override
    public Operator copy()
    {
        DropDataObject copy = new DropDataObject(m_schema_name, m_data_source_name);
        super.copy(copy);
        
        return copy;
    }
    
}
