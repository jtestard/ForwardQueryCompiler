/**
 * 
 */
package edu.ucsd.forward.xml;

import com.google.gwt.xml.client.CDATASection;
import com.google.gwt.xml.client.Comment;
import com.google.gwt.xml.client.DocumentFragment;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.ProcessingInstruction;
import com.google.gwt.xml.client.Text;

/**
 * A wrapper of GWT's DOM document to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class DocumentProxy extends NodeProxy implements com.google.gwt.xml.client.Document
{
    /**
     * Default constructor.
     */
    protected DocumentProxy()
    {
        super();
    }
    
    /**
     * Gets the w3c DOM document.
     * 
     * @return the w3c DOM document.
     */
    private org.w3c.dom.Document doc()
    {
        return (org.w3c.dom.Document) n;
    }
    
    /**
     * Makes a GWT document from w3c document.
     * 
     * @param d
     *            the w3c document
     * @return the constructed GWT document.
     */
    public static DocumentProxy makeGwtDocument(org.w3c.dom.Document d)
    {
        if (d == null)
        {
            return null;
        }
        DocumentProxy d1 = new DocumentProxy();
        d1.n = d;
        return (d1);
    }
    
    @Override
    public CDATASection createCDATASection(String data)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Comment createComment(String data)
    {
        return (CommentProxy) NodeProxy.makeGwtNode(doc().createComment(data));
    }
    
    @Override
    public DocumentFragment createDocumentFragment()
    {
        return DocumentFragmentProxy.makeGwtDocumentFragment((doc().createDocumentFragment()));
    }
    
    @Override
    public ElementProxy createElement(String tag_name)
    {
        return ElementProxy.makeGwtElement(doc().createElement(tag_name));
    }
    
    @Override
    public ProcessingInstruction createProcessingInstruction(String target, String data)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Text createTextNode(String data)
    {
        return (TextProxy) makeGwtNode(doc().createTextNode(data));
    }
    
    @Override
    public ElementProxy getDocumentElement()
    {
        return ElementProxy.makeGwtElement(doc().getDocumentElement());
    }
    
    @Override
    public Element getElementById(String element_id)
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public NodeListProxy getElementsByTagName(String tagname)
    {
        return NodeListProxy.makeGwtNodeList(doc().getElementsByTagName(tagname));
    }
    
    @Override
    public Node importNode(Node imported_node, boolean deep)
    {
        org.w3c.dom.Node node = ((org.w3c.dom.Document) n).importNode(((NodeProxy) imported_node).n, deep);
        return NodeProxy.makeGwtNode(node);
    }
    
}
