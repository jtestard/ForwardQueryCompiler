/**
 * 
 */
package edu.ucsd.forward.data.index;

import java.util.ArrayList;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.TupleValue;

/**
 * The abstract implementation of index.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractIndex implements Index
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractIndex.class);
    
    private IndexDeclaration    m_declaration;
    
    /**
     * Constructs the index with the declaration.
     * 
     * @param declaration
     *            the declaration.
     */
    AbstractIndex(IndexDeclaration declaration)
    {
        assert declaration != null;
        m_declaration = declaration;
    }
    
    @Override
    public IndexDeclaration getDeclaration()
    {
        return m_declaration;
    }
    
    @Override
    public void load(CollectionValue collection)
    {
        clear();
        for(TupleValue tuple:collection.getTuples())
        {
            insert(tuple);
        }
    }
    
    /**
     * Calculates the keys of the given tuple.
     * 
     * @param tuple
     *            a given tuple value.
     * @return the list calculated keys.
     */
    protected List<ScalarValue> computeKeys(TupleValue tuple)
    {
        List<ScalarValue> keys = new ArrayList<ScalarValue>();
        for (SchemaPath path : getDeclaration().getKeyPaths())
        {
            String attr_name = path.getLastPathStep();
            ScalarValue key = (ScalarValue) tuple.getAttribute(attr_name);
            keys.add(key);
        }
        return keys;
    }
}
