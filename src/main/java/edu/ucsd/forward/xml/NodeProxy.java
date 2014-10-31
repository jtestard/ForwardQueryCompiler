/**
 * 
 */
package edu.ucsd.forward.xml;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.NamedNodeMap;
import com.google.gwt.xml.client.Node;

/**
 * A wrapper of GWT's DOM node to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class NodeProxy implements com.google.gwt.xml.client.Node
{
    public org.w3c.dom.Node n;
    
    /**
     * Default constructor.
     */
    protected NodeProxy()
    {
        super();
    }
    
    /**
     * Makes a GWT node from a W3C node.
     * 
     * @param n
     *            the W3C DOM node.
     * @return the constructed GWT DOM node.
     */
    public static Node makeGwtNode(org.w3c.dom.Node n)
    {
        if (n == null)
        {
            return null;
        }
        NodeProxy node;
        if (n instanceof org.w3c.dom.Element)
        {
            node = new ElementProxy();
        }
        else if (n instanceof org.w3c.dom.Document)
        {
            node = new DocumentProxy();
        }
        else if (n instanceof org.w3c.dom.Text)
        {
            node = new TextProxy();
        }
        else if (n instanceof org.w3c.dom.Comment)
        {
            node = new CommentProxy();
        }
        else if (n instanceof org.w3c.dom.CharacterData)
        {
            node = new CharacterDataProxy();
        }
        else if (n instanceof org.w3c.dom.DocumentFragment)
        {
            node = new DocumentFragmentProxy();
        }
        else if (n instanceof org.w3c.dom.Attr)
        {
            node = new AttrProxy();
        }
        else
        {
            node = new NodeProxy();
        }
        node.n = n;
        return (node);
    }
    
    @Override
    public Node appendChild(Node new_child)
    {
        n.appendChild(((NodeProxy) new_child).n);
        return (new_child);
    }
    
    @Override
    public Node cloneNode(boolean deep)
    {
        return makeGwtNode(n.cloneNode(deep));
    }
    
    @Override
    public NamedNodeMap getAttributes()
    {
        return NamedNodeMapProxy.makeGwtNamedNodeMap(n.getAttributes());
    }
    
    @Override
    public com.google.gwt.xml.client.NodeList getChildNodes()
    {
        return NodeListProxy.makeGwtNodeList(n.getChildNodes());
    }
    
    @Override
    public Node getFirstChild()
    {
        return makeGwtNode(n.getFirstChild());
    }
    
    @Override
    public Node getLastChild()
    {
        return makeGwtNode(n.getLastChild());
    }
    
    @Override
    public String getNamespaceURI()
    {
        return n.getNamespaceURI();
    }
    
    @Override
    public Node getNextSibling()
    {
        return makeGwtNode(n.getNextSibling());
    }
    
    @Override
    public String getNodeName()
    {
        return n.getNodeName();
    }
    
    @Override
    public short getNodeType()
    {
        return n.getNodeType();
    }
    
    @Override
    public String getNodeValue()
    {
        return n.getNodeValue();
    }
    
    @Override
    public Document getOwnerDocument()
    {
        return DocumentProxy.makeGwtDocument(n.getOwnerDocument());
    }
    
    @Override
    public Node getParentNode()
    {
        org.w3c.dom.Node parent = n.getParentNode();
        return makeGwtNode(parent);
    }
    
    @Override
    public String getPrefix()
    {
        return n.getPrefix();
    }
    
    @Override
    public Node getPreviousSibling()
    {
        return makeGwtNode(n.getPreviousSibling());
    }
    
    @Override
    public boolean hasAttributes()
    {
        return n.hasAttributes();
    }
    
    @Override
    public boolean hasChildNodes()
    {
        return n.hasChildNodes();
    }
    
    @Override
    public Node insertBefore(Node new_child, Node ref_child)
    {
        return makeGwtNode(n.insertBefore(((NodeProxy) new_child).n, ((NodeProxy) ref_child).n));
    }
    
    @Override
    public void normalize()
    {
        n.normalize();
    }
    
    @Override
    public Node removeChild(Node oldChild)
    {
        return makeGwtNode(n.removeChild(((NodeProxy) oldChild).n));
    }
    
    @Override
    public Node replaceChild(Node newChild, Node oldChild)
    {
        return makeGwtNode(n.replaceChild(((NodeProxy) newChild).n, ((NodeProxy) oldChild).n));
    }
    
    @Override
    public void setNodeValue(String nodeValue)
    {
        n.setNodeValue(nodeValue);
    }
    
    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof NodeProxy)) return false;
        return n.equals(((NodeProxy) other).n);
    }
    
    @Override
    public int hashCode()
    {
        return n.hashCode();
    }
}
