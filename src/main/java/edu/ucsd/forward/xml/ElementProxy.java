/**
 * 
 */
package edu.ucsd.forward.xml;

import com.google.gwt.xml.client.Attr;
import com.google.gwt.xml.client.NodeList;

/**
 * A wrapper of GWT's DOM element to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class ElementProxy extends NodeProxy implements com.google.gwt.xml.client.Element
{
    
    /**
     * Default constructor.
     */
    protected ElementProxy()
    {
        super();
    }
    
    /**
     * Constructs a GWT element from a W3C element.
     * 
     * @param e
     *            the W3C element
     * @return the constructed GWT element.
     */
    public static ElementProxy makeGwtElement(org.w3c.dom.Element e)
    {
        if (e == null)
        {
            return null;
        }
        ElementProxy e1 = new ElementProxy();
        e1.n = e;
        return (e1);
    }
    
    @Override
    public String getAttribute(String name)
    {
        return ((org.w3c.dom.Element) n).getAttribute(name);
    }
    
    @Override
    public Attr getAttributeNode(String name)
    {
        org.w3c.dom.Attr attr = ((org.w3c.dom.Element) n).getAttributeNode(name);
        return AttrProxy.makeGwtAttr(attr);
    }
    
    @Override
    public NodeList getElementsByTagName(String name)
    {
        return NodeListProxy.makeGwtNodeList(((org.w3c.dom.Element) n).getElementsByTagName(name));
    }
    
    @Override
    public String getTagName()
    {
        return ((org.w3c.dom.Element) n).getTagName();
    }
    
    @Override
    public boolean hasAttribute(String name)
    {
        return ((org.w3c.dom.Element) n).hasAttribute(name);
    }
    
    @Override
    public void removeAttribute(String name)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public void setAttribute(String name, String value)
    {
        ((org.w3c.dom.Element) n).setAttribute(name, value);
    }
    
}
