package edu.ucsd.forward.data.source;

import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.query.logical.CardinalityEstimate.Size;

/**
 * A schema object has a name, a schema tree, and is hosted by a data source.
 * 
 * @author Michalis Petropoulos
 * 
 */
public class SchemaObject
{
    public static final String     PAGE_SCHEMA_OBJECT    = "page";
    
    public static final String     VISUAL_SCHEMA_OBJECT  = "visual";
    
    public static final String     MUTABLE_SCHEMA_OBJECT = "mutable";
    
    /**
     * FORWARD_SCHEMA_OBJECT and IS_INIT are used to determine whether a carry over at root level should return the default value or
     * the current value. Written to UAS by PageEvaluator and PageCompiler and read by the query produced by PageQueryGenerator
     */
    public static final String     IS_INIT               = "__is_init";
    
    public static final String     FORWARD_SCHEMA_NAME   = "__forward";
    
    public static final SchemaTree FORWARD_SCHEMA;
    
    static
    {
        TupleType forward_tuple = new TupleType();
        forward_tuple.setAttribute(IS_INIT, new BooleanType());
        FORWARD_SCHEMA = new SchemaTree(forward_tuple);
    }
    
    /**
     * The name of the schema object.
     */
    private String                 m_name;
    
    /**
     * An enumeration of the schema object scopes. In case of PERSISTENT, the schema object is present in a data source for the
     * lifetime of the data source or if it is explicitly dropped. In case of TEMPORARY, the schema object is present in a data
     * source for the lifetime of transaction or if it is explicitly dropped.
     */
    public enum SchemaObjectScope
    {
        PERSISTENT, TEMPORARY;
    }
    
    /**
     * The scope of the schema object.
     */
    private SchemaObjectScope m_scope;
    
    /**
     * The schema tree of the schema object.
     */
    private SchemaTree        m_schema;
    
    /**
     * The cardinality estimation of the data object.
     */
    private Size              m_estimate;
    
    /**
     * Constructs an instance of a schema object.
     * 
     * @param name
     *            name of the schema object.
     * @param scope
     *            scope of the schema object.
     * @param schema
     *            schema tree of the schema object.
     * @param estimate
     *            the cardinality estimation of the data object.
     */
    public SchemaObject(String name, SchemaObjectScope scope, SchemaTree schema, Size estimate)
    {
        assert (name != null);
        assert (scope != null);
        assert (schema != null);
        assert (estimate != null);
        
        m_name = name;
        m_scope = scope;
        m_schema = schema;
        m_estimate = estimate;
    }
    
    /**
     * Returns the name of the schema object.
     * 
     * @return the schema object name.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the scope of the schema object.
     * 
     * @return the schema object scope.
     */
    public SchemaObjectScope getScope()
    {
        return m_scope;
    }
    
    /**
     * Returns the schema tree of the schema object.
     * 
     * @return the schema tree of the schema object.
     */
    public SchemaTree getSchemaTree()
    {
        return m_schema;
    }
    
    /**
     * Returns the cardinality estimation of the data object.
     * 
     * @return the cardinality estimation of the data object.
     */
    public Size getCardinalityEstimate()
    {
        return m_estimate;
    }
}
