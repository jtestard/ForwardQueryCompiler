/**
 * 
 */
package edu.ucsd.forward.query.ast.ddl;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AbstractQueryConstruct;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The DROP statement that removes a schema object from the specified data source.
 * 
 * @author Yupeng
 * 
 */
public class DropStatement extends AbstractQueryConstruct implements DdlStatement
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DropStatement.class);
    
    private String              m_target_name;
    
    private String              m_data_source_name;
    
    /**
     * Default constructor.
     * 
     * @param target_name
     *            the name of the schema object that to be removed
     * @param data_source_name
     *            the name of the data source from which the schema object is removed
     * @param location
     *            a location.
     */
    public DropStatement(String target_name, String data_source_name, Location location)
    {
        super(location);
        
        assert target_name != null;
        assert data_source_name != null;
        
        m_target_name = target_name;
        m_data_source_name = data_source_name;
    }
    
    /**
     * Gets the target schema name.
     * 
     * @return the target schema name
     */
    public String getTargetName()
    {
        return m_target_name;
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
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitDropStatement(this);
    }
    
    @Override
    public AstNode copy()
    {
        DropStatement copy = new DropStatement(m_target_name, m_data_source_name, this.getLocation());
        
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append("DROP " + m_target_name + " FROM " + m_data_source_name);
    }
}
