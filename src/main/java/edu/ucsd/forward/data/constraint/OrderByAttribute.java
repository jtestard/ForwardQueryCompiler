/**
 * 
 */
package edu.ucsd.forward.data.constraint;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;

/**
 * The attribute that specifies the order of the tuples in the relation.
 * 
 * @author Yupeng
 * 
 */
public class OrderByAttribute
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(OrderByAttribute.class);
    
    private Type                m_attribute;
    
    private Spec                m_spec;
    
    private Nulls               m_nulls;
    
    /**
     * Constructs with the attribute type and the order.
     * 
     * @param attribute
     *            the attribute type
     * @param spec
     *            the specification of the order
     * @param nulls
     *            the nulls ordering
     */
    public OrderByAttribute(Type attribute, Spec spec, Nulls nulls)
    {
        assert attribute != null;
        assert spec != null;
        assert nulls != null;
        m_attribute = attribute;
        m_spec = spec;
        m_nulls = nulls;
    }
    
    /**
     * Gets the attribute type.
     * 
     * @return the attribute type
     */
    public Type getAttribute()
    {
        return m_attribute;
    }
    
    /**
     * Gets the specification of the order.
     * 
     * @return the order
     */
    public Spec getSpec()
    {
        return m_spec;
    }
    
    /**
     * Gets the nulls ordering.
     * 
     * @return the nulls ordering
     */
    public Nulls getNulls()
    {
        return m_nulls;
    }
}
