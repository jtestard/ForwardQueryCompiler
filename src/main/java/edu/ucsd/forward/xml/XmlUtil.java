/**
 * 
 */
package edu.ucsd.forward.xml;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.DocumentFragment;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.NamedNodeMap;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.NodeList;
import com.google.gwt.xml.client.Text;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A utility class that provides XML relevant utility methods that are shared between server and client (GWT project).
 * 
 * @author Yupeng
 * 
 */
public final class XmlUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(XmlUtil.class);
    
    /**
     * Hidden constructor.
     */
    private XmlUtil()
    {
    }
    
    /*
     * ==========================================================================
     * =================================================== Methods for parsing XML
     * ======================================================================
     * =======================================================
     */
    
    /**
     * Parse an XML string into an XML DOM node. The method accommodates strings that starts with an XML declaration, as well as
     * strings that are simply a literal without any XML element. In the former case, the document element is returned. In the
     * latter case, the text node is returned.
     * 
     * @param xml_string
     *            the XML string.
     * @param document_builder
     *            the document builder that is used to parse the string.
     * @return the XML DOM node.
     */
    public static Node parseDomNode(String xml_string)
    {
        assert (xml_string != null);
        
        // Wrap the string with a dummy element to accommodate literals and
        // forests
        String wrapped_str = null;
        boolean wrapped = false;
        
        if (!xml_string.startsWith("<?"))
        {
            wrapped_str = "<dummy>" + xml_string + "</dummy>";
            wrapped = true;
        }
        else
        {
            wrapped_str = xml_string;
        }
        
        // Create a DOM builder and parse the template
        Document doc = DomApiProxy.parse(wrapped_str);
        
        // Remove whitespace nodes
        removeWhitespaceNodes(doc.getDocumentElement());
        
        NodeList children = doc.getDocumentElement().getChildNodes();
        
        if (!wrapped) return doc.getDocumentElement();
        
        // If the dummy element has a single child, then return it
        if (children.getLength() == 1)
        {
            return children.item(0);
        }
        // Otherwise return a document fragment
        else
        {
            // Create the document fragment node to hold the new nodes
            DocumentFragment doc_fragment = doc.createDocumentFragment();
            
            // Move the nodes into the fragment
            while (doc.getDocumentElement().hasChildNodes())
            {
                doc_fragment.appendChild(doc.getDocumentElement().removeChild(doc.getDocumentElement().getFirstChild()));
            }
            return doc_fragment;
        }
    }
    
    /**
     * Removes whitespace only text nodes in the DOM graph.
     * 
     * @param node
     *            the node
     */
    protected static void removeWhitespaceNodes(Node node)
    {
        NodeList children = node.getChildNodes();
        for (int i = children.getLength() - 1; i >= 0; i--)
        {
            Node child = children.item(i);
            if (child instanceof Text && ((Text) child).getData().trim().length() == 0)
            {
                node.removeChild(child);
            }
            else if (child instanceof Element)
            {
                removeWhitespaceNodes((Element) child);
            }
        }
    }
    
    /*
     * ==========================================================================
     * =================================================== Methods for creating XML documents
     * ============================================================
     * =================================================================
     */
    
    /**
     * Creates an empty XML document.
     * 
     * @return an empty XML document.
     */
    public static Document createDocument()
    {
        return DomApiProxy.makeDocument();
    }
    
    /*
     * ==========================================================================
     * =================================================== Methods for creating new DOM nodes
     * ============================================================
     * =================================================================
     */
    
    /**
     * Creates the root element of a document and adds it to the document. The document must be empty.
     * 
     * @param document
     *            the empty document.
     * @param tag
     *            the tag of the root element.
     * @return the root element.
     */
    public static Element createRootElement(Document document, String tag)
    {
        assert (document != null);
        assert (!document.hasChildNodes());
        assert (tag != null);
        
        Element element = document.createElement(tag);
        document.appendChild(element);
        return element;
    }
    
    /**
     * Creates a child element within a parent node.
     * 
     * @param parent_node
     *            the parent node.
     * @param tag
     *            the tag of the child element.
     * @return the child element.
     */
    public static Element createChildElement(Node parent_node, String tag)
    {
        assert (parent_node != null);
        assert (tag != null);
        
        Document document = (parent_node instanceof Document) ? (Document) parent_node : parent_node.getOwnerDocument();
        
        Element child_element = document.createElement(tag);
        parent_node.appendChild(child_element);
        return child_element;
    }
    
    /**
     * Creates a leaf element with the given tag name and text value, and appends it to the parent node.
     * 
     * @param tag_name
     *            the tag name of the leaf element.
     * @param value
     *            the value of the leaf element.
     * @param parent_node
     *            the parent node.
     * @return the leaf element.
     */
    public static Element createLeafElement(String tag_name, String value, Node parent_node)
    {
        Document document = parent_node.getOwnerDocument();
        Element node = document.createElement(tag_name);
        parent_node.appendChild(node);
        if (value != null)
        {
            Text name_text = document.createTextNode(value);
            node.appendChild(name_text);
        }
        
        return node;
    }
    
    /**
     * Creates a text node in the context of the given document.
     * 
     * @param document
     *            the document to create the node in the context of
     * @param text
     *            text of the text node
     * @return a text node with the given text
     */
    public static Node createTextNode(Document document, String text)
    {
        assert (document != null);
        assert (text != null);
        
        Node text_node = document.createTextNode(text);
        return text_node;
    }
    
    /**
     * Creates a text node within a parent element.
     * 
     * @param parent_node
     *            the parent node.
     * @param text
     *            the text content.
     * @return the text node.
     */
    public static Node createTextNode(Node parent_node, String text)
    {
        
        assert (parent_node != null);
        assert (text != null);
        
        Document document = (parent_node instanceof Document) ? (Document) parent_node : parent_node.getOwnerDocument();
        
        Node text_node = document.createTextNode(text);
        parent_node.appendChild(text_node);
        return text_node;
    };
    
    /*
     * ==========================================================================
     * =================================================== Methods for mutating existing DOM nodes
     * ========================================================
     * =====================================================================
     */
    
    /**
     * Removes a node from its parent.
     * 
     * @param node
     *            the node.
     */
    public static void removeNode(Node node)
    {
        assert (node != null);
        node.getParentNode().removeChild(node);
    }
    
    /**
     * Replaces an old node with a new node.
     * 
     * @param old_node
     *            the old node.
     * @param new_node
     *            the new node.
     */
    public static void replaceNode(Node old_node, Node new_node)
    {
        assert (old_node != null);
        assert (new_node != null);
        old_node.getParentNode().replaceChild(new_node, old_node);
    }
    
    /*
     * ==========================================================================
     * =================================================== Methods for accessing DOM nodes
     * ================================================================
     * =============================================================
     */
    
    /**
     * Returns the list of child nodes.
     * 
     * @param parent
     *            the parent element.
     * @return the list of child nodes.
     */
    public static List<Node> getChildNodes(Node parent)
    {
        List<Node> list = new ArrayList<Node>();
        NodeList nl = parent.getChildNodes();
        if (nl == null) return list;
        
        for (int i = 0; i < nl.getLength(); i++)
        {
            Node node = nl.item(i);
            list.add(node);
        }
        return list;
    }
    
    /**
     * Serializes a DOM tree into string format.
     * 
     * @param node
     *            the root of the DOM tree to serialize.
     * @param omit_xml_declaration
     *            flag determining whether to omit the XML declaration.
     * @param indent
     *            flag determining whether to indent the XML elements.
     * @return the serialized string representation.
     */
    public static String serializeDomToString(Node node, boolean omit_xml_declaration, boolean indent)
    {
        return DomApiProxy.serializeDomToString(node, omit_xml_declaration, indent);
    }
    
    /**
     * Serializes DOM tree into string format without indentation.
     * 
     * @param node
     *            the root of the DOM tree to serialize.
     * @param omit_xml_declaration
     *            flag determining whether to omit the XML declaration.
     * @return the serialized string representation.
     */
    public static String serializeDomToString(Node node, boolean omit_xml_declaration)
    {
        return serializeDomToString(node, omit_xml_declaration, true);
    }
    
    /**
     * Returns the list of child elements.
     * 
     * @param parent
     *            the parent element.
     * @return the list of child elements.
     */
    public static List<Element> getChildElements(Node parent)
    {
        List<Element> list = new ArrayList<Element>();
        NodeList nl = parent.getChildNodes();
        if (nl == null) return list;
        
        for (int i = 0; i < nl.getLength(); i++)
        {
            Node node = nl.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE) continue;
            
            list.add((Element) node);
        }
        return list;
    }
    
    /**
     * Returns the list of child elements with a given tag name.
     * 
     * @param parent
     *            the parent element.
     * @param tag
     *            the tag name.
     * @return the list of child elements.
     */
    public static List<Element> getChildElements(Element parent, String tag)
    {
        List<Element> list = new ArrayList<Element>();
        for (Element child : getChildElements(parent))
        {
            if (child.getTagName().equals(tag)) list.add(child);
        }
        return list;
    }
    
    /**
     * Returns the only child element. An error occurs if there is not exactly one child element.
     * 
     * @param parent
     *            the parent element.
     * @return the only child element.
     */
    public static Element getOnlyChildElement(Element parent)
    {
        List<Element> children = getChildElements(parent);
        assert children.size() == 1;
        return children.get(0);
    }
    
    /**
     * Returns the only child element with a given tag name. An error occurs if there is not exactly one such child element.
     * 
     * @param parent
     *            the parent element.
     * @param tag
     *            the tag name.
     * @return the only child element with the given tag name.
     */
    public static Element getOnlyChildElement(Element parent, String tag)
    {
        List<Element> children = getChildElements(parent, tag);
        assert children.size() == 1;
        return children.get(0);
    }
    
    /**
     * Returns an optional child element. An error occurs if there are more than one child element.
     * 
     * @param parent
     *            the parent element.
     * @return the optional child element if it exists; <code>null</code> otherwise.
     */
    public static Element getOptionalChildElement(Element parent)
    {
        List<Element> children = getChildElements(parent);
        assert children.size() == 0 || children.size() == 1;
        return (children.size() == 1) ? children.get(0) : null;
    }
    
    /**
     * Returns an optional child element with a given tag name. An error occurs if there are more than one such child element.
     * 
     * @param parent
     *            the parent element.
     * @param tag
     *            the tag name.
     * @return the optional child element with the given tag name if it exists; <code>null</code> otherwise.
     */
    public static Element getOptionalChildElement(Element parent, String tag)
    {
        List<Element> children = getChildElements(parent, tag);
        assert children.size() == 0 || children.size() == 1;
        return (children.size() == 1) ? children.get(0) : null;
    }
    
    /**
     * Returns the attribute of the given name. An error occurs if the attribute is not set.
     * 
     * @param element
     *            the element.
     * @param attribute_name
     *            the attribute name.
     * @return the attribute of the given name.
     */
    public static String getNonEmptyAttribute(Element element, String attribute_name)
    {
        String attribute = element.getAttribute(attribute_name);
        assert (!attribute.isEmpty());
        return attribute;
    }
    
    /**
     * Returns the attribute of the given name. If the attribute is not set, this method returns <code>null</code>. This is unlike
     * the DOM API, which returns the empty string.
     * 
     * @param element
     *            the element.
     * @param attribute_name
     *            the attribute name.
     * @return the attribute of the given name if it is set; <code>null</code> otherwise.
     */
    public static String getAttribute(Element element, String attribute_name)
    {
        return (element.hasAttribute(attribute_name)) ? element.getAttribute(attribute_name) : null;
    }
    
    /**
     * Returns the names of all the attributes in an element.
     * 
     * @param element
     *            the element
     * @return the names of the element's attributes
     */
    public static List<String> getAttributeNames(Element element)
    {
        NamedNodeMap map = element.getAttributes();
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < map.getLength(); i++)
        {
            names.add(map.item(i).getNodeName());
        }
        return names;
    }
    
    /**
     * Returns a boolean indicating whether the given Node is a TextNode.
     * 
     * @param node
     * @return a boolean indicating whether the given Node is a TextNode
     */
    public static boolean isTextNode(Node node)
    {
        return node.getNodeType() == Node.TEXT_NODE;
    }
    
    /**
     * Checks if a Node equals to another.
     * 
     * @param left
     *            given node
     * @param right
     *            given node
     * @return <code>true</code> if the nodes equal, <code>false</code>otherwise
     */
    public static boolean equals(Node left, Node right)
    {
        if (left == null && right == null) return true;
        if (left == null || right == null) return false;
        return left.equals(right);
    }
}
