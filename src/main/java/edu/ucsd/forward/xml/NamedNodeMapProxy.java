/**
 * 
 */
package edu.ucsd.forward.xml;

import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A wrapper of GWT's DOM named node map to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class NamedNodeMapProxy implements com.google.gwt.xml.client.NamedNodeMap
{
    @SuppressWarnings("unused")
    private static final Logger      log = Logger.getLogger(NamedNodeMapProxy.class);
    
    private org.w3c.dom.NamedNodeMap n;
    
    /**
     * Default constructor.
     */
    protected NamedNodeMapProxy()
    {
        super();
    }
    
    /**
     * Constructs a GWT named node map from a W3C named node map.
     * 
     * @param m
     *            the W3C named node map
     * @return the constructed GWT named node map.
     */
    public static NamedNodeMapProxy makeGwtNamedNodeMap(org.w3c.dom.NamedNodeMap m)
    {
        if (m == null)
        {
            return null;
        }
        NamedNodeMapProxy m1 = new NamedNodeMapProxy();
        m1.n = m;
        return (m1);
    }
    
    @Override
    public int getLength()
    {
        return n.getLength();
    }
    
    @Override
    public Node getNamedItem(String name)
    {
        org.w3c.dom.Node node = n.getNamedItem(name);
        return NodeProxy.makeGwtNode(node);
    }
    
    @Override
    public Node item(int index)
    {
        org.w3c.dom.Node node = n.item(index);
        return NodeProxy.makeGwtNode(node);
    }
}
