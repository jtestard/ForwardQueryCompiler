/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.text.ParseException;
import java.util.List;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.diff.DeleteDiff;
import edu.ucsd.forward.data.diff.Diff;
import edu.ucsd.forward.data.diff.InsertDiff;
import edu.ucsd.forward.data.diff.ReplaceDiff;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * A parser for the XML representation of data diffs.
 * 
 * @author Kian Win Ong
 * 
 */
public final class DataDiffXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log               = Logger.getLogger(DataDiffXmlParser.class);
    
    private static final String INSERT_OPERATION  = "insert";
    
    private static final String REPLACE_OPERATION = "replace";
    
    private static final String DELETE_OPERATION  = "delete";
    
    /**
     * Private constructor.
     */
    private DataDiffXmlParser()
    {
        
    }
    
    /**
     * Parse a data diff from an element. The payload is represented by a data path to a data tree.
     * 
     * @param element
     *            the element.
     * @param data_tree
     *            the data tree.
     * @return a data diff.
     * @throws XmlParserException
     *             if an error occurs during XML parsing.
     */
    public static Diff parse(Element element, DataTree data_tree) throws XmlParserException
    {
        assert (element != null);
        assert (data_tree != null);
        
        // Parse the operation
        String operation = parseOperation(element);
        
        // Parse the context
        DataPath context = parseContext(element);
        
        // Construct the diff
        if (operation.equals(INSERT_OPERATION))
        {
            Value payload = context.find(data_tree);
            return new InsertDiff(context, payload);
        }
        else if (operation.equals(REPLACE_OPERATION))
        {
            Value payload = context.find(data_tree);
            return new ReplaceDiff(context, payload);
        }
        else
        {
            assert (operation.equals(DELETE_OPERATION));
            return new DeleteDiff(context);
        }
    }
    
    /**
     * Parse a data diff from an element. The payload is represented by a value tree.
     * 
     * @param element
     *            the element.
     * @param schema_tree
     *            the schema tree providing the types for the value tree.
     * @return a data diff.
     * @throws XmlParserException
     *             if an error occurs during XML parsing.
     */
    public static Diff parse(Element element, SchemaTree schema_tree) throws XmlParserException
    {
        assert (element != null);
        assert (schema_tree != null);
        
        // Parse the operation
        String operation = parseOperation(element);
        
        // Parse the context
        DataPath context = parseContext(element);
        
        // Check that <payload> occurs either once or not at all
        List<Element> payload_elements = XmlUtil.getChildElements(element, "payload");
        if ((operation.equals(INSERT_OPERATION) || operation.equals(REPLACE_OPERATION)) && payload_elements.size() != 1)
        {
            throw new XmlParserException(StringUtil.format("<%s> must have exactly one payload child element", operation));
        }
        else if (operation.equals(DELETE_OPERATION) && payload_elements.size() != 0)
        {
            throw new XmlParserException(StringUtil.format("<%s> must not have a payload child element", operation));
        }
        
        // Parse the payload
        Value payload = null;
        if (payload_elements.size() == 1)
        {
            Element payload_element = payload_elements.get(0);
            
            SchemaPath schema_path = context.toSchemaPath();
            Type type = schema_path.find(schema_tree);
            
            payload = ValueXmlParser.parseValue(payload_element, type);
        }
        
        // Construct the diff
        if (operation.equals(INSERT_OPERATION))
        {
            return new InsertDiff(context, payload);
        }
        else if (operation.equals(REPLACE_OPERATION))
        {
            return new ReplaceDiff(context, payload);
        }
        else
        {
            assert (operation.equals(DELETE_OPERATION));
            return new DeleteDiff(context);
        }
    }
    
    /**
     * Parse an operation from an element.
     * 
     * @param element
     *            the element.
     * @return the string representation of an operation.
     * @throws XmlParserException
     *             if an error occurs during XML parsing.
     */
    private static String parseOperation(Element element) throws XmlParserException
    {
        assert (element != null);
        
        String element_tag = element.getTagName();
        if (!element_tag.equals(INSERT_OPERATION) && !element_tag.equals(REPLACE_OPERATION)
                && !element_tag.equals(DELETE_OPERATION))
        {
            throw new XmlParserException(StringUtil.format("<%s> is not a valid diff", element_tag));
        }
        
        return element_tag;
    }
    
    /**
     * Parse a context from an element.
     * 
     * @param element
     *            the element.
     * @return the context.
     * @throws XmlParserException
     *             if an error occurs during XML parsing.
     */
    private static DataPath parseContext(Element element) throws XmlParserException
    {
        assert (element != null);
        
        String context_string = element.getAttribute("context");
        if ("".equals(context_string))
        {
            String msg = "<%s> must have a \"context\" attribute";
            throw new XmlParserException(StringUtil.format(msg, element.getTagName()));
        }
        DataPath context = null;
        try
        {
            context = new DataPath(context_string);
        }
        catch (ParseException e)
        {
            String msg = "<%s> has invalid \"context\" attribute \"%s\"";
            throw new XmlParserException(StringUtil.format(msg, element.getTagName(), context_string), e);
        }
        
        return context;
    }
}
