/**
 * 
 */
package edu.ucsd.forward.data.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.ScalarType;

/**
 * An declaration of the index.
 * 
 * @author Yupeng
 * 
 */
@SuppressWarnings("serial")
public class IndexDeclaration implements Serializable
{
    
    private CollectionType     m_collection_type;
    
    private List<ScalarType>   m_keys;
    
    private String             m_name;
    
    private boolean            m_unique;
    
    public static final String SEPARATION = "@";
    
    private IndexMethod        m_method;
    
    /**
     * Constructs the index declaration.
     * 
     * @param name
     *            the name of the index
     * @param collection_type
     *            the collection that the index is declared on
     * @param keys
     *            the keys of the index
     * @param unique
     *            indicates if duplicate values are allowed in the index.
     */
    public IndexDeclaration(String name, CollectionType collection_type, List<ScalarType> keys, boolean unique, IndexMethod method)
    {
        assert name != null;
        assert collection_type != null;
        assert keys != null;
        assert !keys.isEmpty();
        assert method != null;
        
        m_name = name;
        m_collection_type = collection_type;
        m_keys = keys;
        m_unique = unique;
        m_method = method;
    }
    
    /**
     * Constructs the index declaration.
     * 
     * @param name
     *            the name of the index
     * @param collection_type
     *            the collection that the index is declared on
     * @param keys
     *            the keys of the index
     */
    public IndexDeclaration(String name, CollectionType collection_type, List<ScalarType> keys)
    {
        this(name, collection_type, keys, false, IndexMethod.BTREE);
    }
    
    /**
     * Default constructor.
     */
    protected IndexDeclaration()
    {
        
    }
    
    /**
     * Gets the index method.
     * 
     * @return the index method.
     */
    public IndexMethod getMethod()
    {
        return m_method;
    }
    
    /**
     * Gets the name of the index.
     * 
     * @return the name of the index.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the collection type.
     * 
     * @return the collection type.
     */
    public CollectionType getCollectionType()
    {
        return m_collection_type;
    }
    
    /**
     * Returns the keys.
     * 
     * @return the keys.
     */
    public List<ScalarType> getKeys()
    {
        return m_keys;
    }
    
    /**
     * Returns the collection type schema path, computed on demand (not cached).
     * 
     * @return the collection type schema path.
     */
    public SchemaPath getCollectionTypeSchemaPath()
    {
        return new SchemaPath(m_collection_type);
    }
    
    /**
     * Checks if duplicate values are allowed in the index.
     * 
     * @return <code>true</code> if duplicate values are allowed in the index, <code>false</code> otherwise.
     */
    public boolean isUnique()
    {
        return m_unique;
    }
    
    /**
     * Returns the relative schema paths of the attributes.
     * 
     * @return the relative schema paths of the attributes.
     */
    public List<SchemaPath> getKeyPaths()
    {
        List<SchemaPath> paths = new ArrayList<SchemaPath>();
        for (ScalarType key : m_keys)
        {
            paths.add(new SchemaPath(m_collection_type, key));
        }
        return paths;
    }
    
    /**
     * Gets the full name of the index including the schema path to the collection.
     * 
     * @return the full name of the index including the schema path to the collection.
     */
    public String getFullName()
    {
        String schema_part = new SchemaPath(m_collection_type).getString();
        return schema_part + SEPARATION + getName();
    }
    
    @Override
    public String toString()
    {
        String s = "Index:\n";
        s += "Collection: " + getCollectionTypeSchemaPath() + m_collection_type;
        s += "\nKeys:{ ";
        for (ScalarType key : m_keys)
        {
            s += new SchemaPath(m_collection_type, key) + ":" + key + ";";
        }
        s += "}\n";
        return s;
    }
}
