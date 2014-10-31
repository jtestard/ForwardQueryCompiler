/**
 * 
 */
package edu.ucsd.forward.data.xml;

import static edu.ucsd.forward.xml.XmlUtil.createChildElement;

import java.io.Serializable;

import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;
import edu.ucsd.forward.data.value.TupleValue.AttributeValueEntry;
import edu.ucsd.forward.xml.AbstractXmlSerializer;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Utility class to serialize a value or a data tree to an XML DOM tree.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public final class ValueXmlSerializer extends AbstractXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger   log           = Logger.getLogger(ValueXmlSerializer.class);
    
    public static final String    DATA_TREE_ELM = "data_tree";
    
    public static final String    ROOT_ELM      = "root";
    
    protected static final String NULL_ATTR     = "null";
    
    protected static final String TUPLE_ELM     = "tuple";
    
    protected static final String VALUE_ELEMENT = "element";
    
    protected static final String ORDERED = "ordered";
    
    /**
     * Hidden constructor.
     */
    private ValueXmlSerializer()
    {
    }
    
    /**
     * Serializes a data tree to an XML DOM element.
     * 
     * @param data_tree
     *            a data tree to serialize.
     * @param parent_node
     *            the parent XML node.
     */
    public static void serializeDataTree(DataTree data_tree, Node parent_node)
    {
        assert (parent_node != null);
        
        Element data_tree_elm = createChildElement(parent_node, DATA_TREE_ELM);
        
        serializeValue(data_tree.getRootValue(), ROOT_ELM, data_tree_elm);
    }
    
    /**
     * Serializes a value to an XML DOM element.
     * 
     * @param value
     *            the value to serialize.
     * @param tag_name
     *            the tag name to be used when serializing the input value.
     * @param parent_node
     *            the parent XML node.
     */
    public static Element serializeValue(Value value, String tag_name, Node parent_node)
    {
        return serializeValue(value, tag_name, parent_node, false);
    }
    
    /**
     * Serializes a value to an XML DOM element.
     * 
     * @param value
     *            the value to serialize.
     * @param tag_name
     *            the tag name to be used when serializing the input value.
     * @param parent_node
     *            the parent XML node.
     * @param add_predicate_attribute
     *            for tuple values within a collection value, specify whether the resulting element should have an attribute that
     *            stores the predicate that is calculated based on the keys
     */
    public static Element serializeValue(Value value, String tag_name, Node parent_node, boolean add_predicate_attribute)
    {
        Element value_element = createChildElement(parent_node, tag_name);
        
        if (value instanceof TupleValue)
        {
            TupleValue tuple_value = (TupleValue) value;
            for (AttributeValueEntry entry : tuple_value)
            {
                Value attribute_value = entry.getValue();
                serializeValue(attribute_value, entry.getName(), value_element, add_predicate_attribute);
            }
            
            if (add_predicate_attribute && value.getParent() instanceof CollectionValue)
            {
                // Sets the value of the predicate as an attribute on the element.
                // This is an optimization so that the client runtime doesn't need to recompute
                // the predicate from the keys if the predicate is already provided by the
                // server.
                DataPath data_path = new DataPath(value);
                String predicate = data_path.getLastPathStep().toString();
                // Remove the "tuple" token so that only the [...] remains
                predicate = predicate.replaceFirst("tuple", "");
                value_element.setAttribute("predicate", predicate);
            }
        }
        else if (value instanceof CollectionValue)
        {
            CollectionValue collection_value = (CollectionValue) value;
            if(collection_value.isOrdered())
            {
                value_element.setAttribute(ORDERED, String.valueOf(true));
            }
            for (Value child : collection_value.getValues())
            {
                serializeValue(child, VALUE_ELEMENT, value_element, add_predicate_attribute);
            }
        }
        else if (value instanceof SwitchValue)
        {
            SwitchValue switch_value = (SwitchValue) value;
            Value case_value = switch_value.getCase();
            serializeValue(case_value, switch_value.getCaseName(), value_element, add_predicate_attribute);
        }
        else if (value instanceof XhtmlValue)
        {
            value_element.setAttribute("xmlns", "http://www.w3.org/1999/xhtml");
            
            XhtmlValue xhtml_value = (XhtmlValue) value;
            Node xhtml_node = XmlUtil.parseDomNode(xhtml_value.getObject());
            value_element.appendChild(parent_node.getOwnerDocument().importNode(xhtml_node, true));
        }
        else if (value instanceof JsonValue)
        {
            JsonValue json_value = (JsonValue) value;
            String element_str = json_value.getElement().toString();
            XmlUtil.createTextNode(value_element, element_str);
        }
        else if (value instanceof ScalarValue)
        {
            ScalarValue scalar_value = (ScalarValue) value;
            String value_string = scalar_value.toString();
            XmlUtil.createTextNode(value_element, value_string);
        }
        else
        {
            assert (value instanceof NullValue);
            value_element.setAttribute(NULL_ATTR, "true");
        }
        
        return value_element;
    }
    
}
