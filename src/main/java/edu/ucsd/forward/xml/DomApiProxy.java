/**
 * 
 */
package edu.ucsd.forward.xml;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.google.gwt.xml.client.Attr;
import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;
import com.google.gwt.xml.client.NodeList;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * The proxy API used for XML Util. This class is shared with GWT project. And its client-side emulation is via gwt.xml.* classes.
 * 
 * @author Yupeng
 * 
 */
public final class DomApiProxy
{
    @SuppressWarnings("unused")
    private static final Logger                       log                      = Logger.getLogger(DomApiProxy.class);
    
    // Cache a factory object, as instantiating one needs an expensive lookup of available implementations
    private static final DocumentBuilderFactory       DOCUMENT_BUILDER_FACTORY = DocumentBuilderFactory.newInstance();
    
    // Cache a factory object, as instantiating one needs an expensive lookup of available implementations
    private static final TransformerFactory           TRANSFORMER_FACTORY      = TransformerFactory.newInstance();
    
    /**
     * A thread pool for the document builders.
     */
    private static final ThreadLocal<DocumentBuilder> DB_INSTANCE              = new ThreadLocal<DocumentBuilder>();
    
    /**
     * Private constructor.
     */
    private DomApiProxy()
    {
        
    }
    
    /**
     * Constructs a GWT document.
     * 
     * @return the constructed GWT document.
     */
    public static Document makeDocument()
    {
        org.w3c.dom.Document doc;
        try
        {
            DOCUMENT_BUILDER_FACTORY.setIgnoringElementContentWhitespace(true);
            DOCUMENT_BUILDER_FACTORY.setCoalescing(true);
            doc = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder().newDocument();
        }
        catch (ParserConfigurationException e)
        {
            throw new AssertionError();
        }
        return DocumentProxy.makeGwtDocument(doc);
    }
    
    /**
     * Gets the node value of a given element. The XMLParser in GWT has strange behavior, it regards list-node value as one more
     * node and requires getting child nodes for it.
     * 
     * @param element
     *            the given element
     * @return the node value of the given element.
     */
    public static String getNodeValue(Element element)
    {
        return ((ElementProxy) element).n.getTextContent();
    }
    
    /**
     * Sets the text content of this node and its descendants.
     * 
     * @param node
     *            the node whose content to set.
     * @param text
     *            the text of the content
     */
    public static void setTextContent(Node node, String text)
    {
        ((NodeProxy) node).n.setTextContent(text);
    }
    
    /**
     * Gets the owner element of the attr.
     * 
     * @param attr
     *            the attr
     * @return the owner element.
     */
    public static Element getOwnerElement(Attr attr)
    {
        org.w3c.dom.Attr w3c_attr = (org.w3c.dom.Attr) ((AttrProxy) attr).n;
        return ElementProxy.makeGwtElement(w3c_attr.getOwnerElement());
    }
    
    /**
     * Returns the list of DOM nodes found when executing the input XPath using the input DOM node as context.
     * 
     * @param context
     *            the context DOM node.
     * @param xpath
     *            the XPath expression.
     * @return a list of DOM nodes.
     */
    public static List<Node> getXpathNodes(Node context, String xpath)
    {
        assert (context != null && xpath != null);
        
        List<Node> list = new ArrayList<Node>();
        
        XPathFactory factory = XPathFactory.newInstance();
        XPath expr = factory.newXPath();
        Object result;
        try
        {
            result = expr.evaluate(xpath, ((NodeProxy) context).n, XPathConstants.NODESET);
            NodeList nl = NodeListProxy.makeGwtNodeList((org.w3c.dom.NodeList) result);
            for (int i = 0; i < nl.getLength(); i++)
            {
                Node node = nl.item(i);
                list.add(node);
            }
            
            return list;
        }
        catch (XPathExpressionException e)
        {
            throw new AssertionError();
        }
    }
    
    /**
     * Parses a new document from the supplied string.
     * 
     * @param content
     *            the String to be parsed into a Document.
     * @return the newly created Document
     */
    public static Document parse(String content)
    {
        DocumentBuilder builder = DB_INSTANCE.get();
        if (builder == null)
        {
            try
            {
                DOCUMENT_BUILDER_FACTORY.setIgnoringElementContentWhitespace(true);
                DOCUMENT_BUILDER_FACTORY.setCoalescing(true);
                builder = DOCUMENT_BUILDER_FACTORY.newDocumentBuilder();
                DB_INSTANCE.set(builder);
            }
            catch (ParserConfigurationException e)
            {
                throw new RuntimeException(e);
            }
            
        }
        
        try
        {
            // Reset document builder to original configuration
            org.w3c.dom.Document doc = builder.parse(new InputSource(new StringReader(content)));
            return DocumentProxy.makeGwtDocument(doc);
        }
        catch (SAXException e)
        {
            throw new RuntimeException(e);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Checks if an attribute in element is empty.
     * 
     * @param attribute
     *            the attribute to check.
     * @return <code>true</code> if the attribute is empty;<code>false</code> otherwise.
     */
    public static boolean isEmpty(String attribute)
    {
        return attribute.isEmpty();
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
        try
        {
            Transformer transformer = TRANSFORMER_FACTORY.newTransformer();
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
            transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
            
            if (indent)
            {
                transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            }
            
            StringWriter sw = new StringWriter();
            transformer.transform(new DOMSource(((NodeProxy) node).n), new StreamResult(sw));
            
            if (!omit_xml_declaration)
            {
                /*
                 * HACK:
                 * 
                 * XALAN has an inconsistent behavior of omitting the newline after the XML declaration, even when INDENT is on.
                 * This inconsistent behavior was absent in 2.7.0, but present in 2.7.1.
                 * 
                 * More details at: http://www.mail-archive.com/xalan-j-users@xml.apache.org/msg05702.html
                 * 
                 * Workaround the issue by manually adding the XML declaration.
                 */
                return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" + sw.toString();
            }
            
            return sw.toString();
        }
        catch (TransformerConfigurationException e)
        {
            throw new RuntimeException(e);
        }
        catch (TransformerException e)
        {
            throw new RuntimeException(e);
        }
    }
}
