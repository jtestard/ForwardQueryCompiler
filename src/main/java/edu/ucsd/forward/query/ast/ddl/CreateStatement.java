/**
 * 
 */
package edu.ucsd.forward.query.ast.ddl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.source.DataSource;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.ast.AbstractQueryConstruct;
import edu.ucsd.forward.query.ast.AstNode;
import edu.ucsd.forward.query.ast.ValueExpression;
import edu.ucsd.forward.query.ast.visitors.AstNodeVisitor;

/**
 * The CREATE statement that adds a schema object to the specified data source.
 * 
 * @author Yupeng
 * 
 */
public class CreateStatement extends AbstractQueryConstruct implements DdlStatement
{
    @SuppressWarnings("unused")
    private static final Logger        log = Logger.getLogger(CreateStatement.class);
    
    private SchemaTree                 m_schema_tree;
    
    private String                     m_schema_name;
    
    private String                     m_data_source_name;
    
    private Map<Type, ValueExpression> m_type_defaults;
    
    /**
     * Default constructor.
     * 
     * @param schema_name
     *            the name of the schema
     * @param schema_tree
     *            the schema tree of the schema object
     * @param data_source_name
     *            the name of the data source where the schema object to be created.
     * @param location
     *            a location.
     */
    public CreateStatement(String schema_name, SchemaTree schema_tree, String data_source_name, Location location)
    {
        this(schema_name, data_source_name, location);
        assert schema_tree != null;
        m_schema_tree = schema_tree;
    }
    
    /**
     * Construct with the schema name and data source name.
     * 
     * @param schema_name
     *            the name of the schema
     * @param data_source_name
     *            the name of the data source where the schema object to be created.
     * @param location
     *            a location.
     */
    public CreateStatement(String schema_name, String data_source_name, Location location)
    {
        super(location);
        
        assert schema_name != null;
        assert data_source_name != null;
        
        m_schema_name = schema_name;
        m_data_source_name = data_source_name;
        
        m_type_defaults = new LinkedHashMap<Type, ValueExpression>();
    }
    
    /**
     * Update the registered type, when a parent type is cloned to a new type.
     * 
     * @param old_type
     *            the old type
     * @param new_type
     *            the new type.
     */
    public void updateType(Type old_type, Type new_type)
    {
        Collection<Type> descendents_seft = old_type.getDescendantsAndSelf();
        for (Type default_type : new ArrayList<Type>(getTypesToSetDefault()))
        {
            if (!descendents_seft.contains(default_type)) continue;
            ValueExpression expr = getDefaultValueExpression(default_type);
            m_type_defaults.remove(default_type);
            SchemaPath path = new SchemaPath(old_type, default_type);
            Type new_default_type = path.find(new_type);
            assert new_default_type != null;
            m_type_defaults.put(new_default_type, expr);
        }
    }
    
    /**
     * Adds the value expression for evaluating the default value of a given type.
     * 
     * @param type
     *            the type
     * @param expression
     *            the value expression
     */
    public void addDefaultExpression(Type type, ValueExpression expression)
    {
        assert type != null;
        assert expression != null;
        m_type_defaults.put(type, expression);
    }
    
    /**
     * Gets the set of types to set default value.
     * 
     * @return the set of types to set default value.
     */
    public Set<Type> getTypesToSetDefault()
    {
        return m_type_defaults.keySet();
    }
    
    /**
     * Gets the default value expression of a given type.
     * 
     * @param type
     *            the type
     * @return the default value expression
     */
    public ValueExpression getDefaultValueExpression(Type type)
    {
        return m_type_defaults.get(type);
    }
    
    /**
     * Sets the schema tree.
     * 
     * @param schema_tree
     *            the schema tree of the schema object
     */
    public void setSchemaTree(SchemaTree schema_tree)
    {
        assert schema_tree != null;
        m_schema_tree = schema_tree;
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
    
    /**
     * Gets the schema tree.
     * 
     * @return the schema tree.
     */
    public SchemaTree getSchemaTree()
    {
        return m_schema_tree;
    }
    
    @Override
    public Object accept(AstNodeVisitor visitor) throws QueryCompilationException
    {
        return visitor.visitCreateStatement(this);
    }
    
    @Override
    public AstNode copy()
    {
        SchemaTree schema_copy = TypeUtil.clone(m_schema_tree);
        CreateStatement copy = new CreateStatement(m_schema_name, schema_copy, m_data_source_name, this.getLocation());
        for (Type type : getTypesToSetDefault())
        {
            SchemaPath path = new SchemaPath(type);
            Type type_copy = path.find(schema_copy.getRootType());
            copy.addDefaultExpression(type_copy, (ValueExpression) getDefaultValueExpression(type).copy());
        }
        super.copy(copy);
        
        return copy;
    }
    
    @Override
    public void toQueryString(StringBuilder sb, int tabs, DataSource data_source)
    {
        sb.append(this.getIndent(tabs));
        
        sb.append("CREATE SCHEMA " + m_schema_name + " ");
        m_schema_tree.toQueryString(sb, tabs, data_source);
        sb.append(" INTO " + m_data_source_name);
    }
}
