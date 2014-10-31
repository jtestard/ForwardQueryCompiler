/**
 * 
 */
package edu.ucsd.forward.xml;

import com.google.gwt.xml.client.Node;

/**
 * A wrapper of GWT's DOM node list to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class NodeListProxy implements com.google.gwt.xml.client.NodeList
{
    /**
     * Default constructor.
     */
    protected NodeListProxy()
    {
        super();
    }
    
    protected org.w3c.dom.NodeList n;
    
    /**
     * Constructs the GWT node list from the W3C node list.
     * 
     * @param n
     *            the W3C node list
     * @return the constructed GWT node list.
     */
    public static NodeListProxy makeGwtNodeList(org.w3c.dom.NodeList n)
    {
        if (n == null)
        {
            return null;
        }
        NodeListProxy n1 = new NodeListProxy();
        n1.n = n;
        return (n1);
    }
    
    @Override
    public int getLength()
    {
        return n.getLength();
    }
    
    @Override
    public Node item(int index)
    {
        return NodeProxy.makeGwtNode(n.item(index));
    }
}
