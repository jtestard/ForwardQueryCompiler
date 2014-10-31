/**
 * 
 */
package edu.ucsd.forward.data.value;

import com.google.gwt.xml.client.Node;

import edu.ucsd.forward.data.type.XhtmlType;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * The XHTML value.
 * 
 * @author Yupeng
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class XhtmlValue extends AbstractPrimitiveValue<XhtmlType, String>
{
    
    private String m_xhtml_string;
    
    private Node   m_xhtml;
    
    /**
     * Constructs a XHTML value.
     * 
     * @param xhtml_string
     *            the XHTML in string format.
     */
    public XhtmlValue(String xhtml_string)
    {
        m_xhtml = null;
        m_xhtml_string = xhtml_string;
    }
    
    /**
     * Constructs a XHTML value.
     * 
     * @param node
     *            a DOM node.
     */
    public XhtmlValue(Node node)
    {
        assert (node != null);
        m_xhtml = node;
        m_xhtml_string = XmlUtil.serializeDomToString(m_xhtml, true, false);
    }
    
    /**
     * Default constructor.
     */
    protected XhtmlValue()
    {
        
    }
    
    @Override
    public Class<XhtmlType> getTypeClass()
    {
        return XhtmlType.class;
    }
    
    @Override
    public String getObject()
    {
        return m_xhtml_string;
    }
    
    @Override
    public String toString()
    {
        return m_xhtml_string;
    }
    
}
