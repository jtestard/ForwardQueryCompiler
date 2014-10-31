/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.DateFormat;
import java.util.List;
import java.util.Locale;

import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.app2you.util.collection.Pair;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.ValueTypeMapUtil;
import edu.ucsd.forward.data.type.BooleanType;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.DateType;
import edu.ucsd.forward.data.type.DecimalType;
import edu.ucsd.forward.data.type.DoubleType;
import edu.ucsd.forward.data.type.FloatType;
import edu.ucsd.forward.data.type.IntegerType;
import edu.ucsd.forward.data.type.JsonType;
import edu.ucsd.forward.data.type.LongType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.StringType;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TimestampType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.XhtmlType;
import edu.ucsd.forward.data.value.BooleanValue;
import edu.ucsd.forward.data.value.CollectionValue;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.DateValue;
import edu.ucsd.forward.data.value.DecimalValue;
import edu.ucsd.forward.data.value.DoubleValue;
import edu.ucsd.forward.data.value.FloatValue;
import edu.ucsd.forward.data.value.IntegerValue;
import edu.ucsd.forward.data.value.JsonValue;
import edu.ucsd.forward.data.value.LongValue;
import edu.ucsd.forward.data.value.NullValue;
import edu.ucsd.forward.data.value.StringValue;
import edu.ucsd.forward.data.value.SwitchValue;
import edu.ucsd.forward.data.value.TimestampValue;
import edu.ucsd.forward.data.value.TupleValue;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.data.value.XhtmlValue;
import edu.ucsd.forward.xml.AbstractXmlParser;
import edu.ucsd.forward.xml.DomApiProxy;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Utility class to parse a value or a data tree from an XML DOM tree.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public final class ValueXmlParser extends AbstractXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ValueXmlParser.class);
    
    /**
     * Hidden constructor.
     */
    private ValueXmlParser()
    {
    }
    
    /**
     * Parse a value and a type from the given element. Foreign key constraints are ignored. Schema is NOT mapped to value.
     * 
     * @param elm
     *            the input element
     * @return the pair of value and type
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static Pair<Value, Type> parseValueAndType(Element elm) throws XmlParserException
    {
        // Build schema tree
        Element type_elm = edu.ucsd.forward.xml.XmlUtil.getOnlyChildElement(elm, "type");
        Type type = TypeXmlParser.parseType(type_elm);
        
        // Build data tree
        Element value_elm = XmlUtil.getOnlyChildElement(elm, "value");
        Value value = parseValue(value_elm, type);
        
        return new Pair<Value, Type>(value, type);
    }
    
    /**
     * Parses a data tree.
     * 
     * @param data_tree_element
     *            a data tree element of an XML DOM.
     * @param schema_tree
     *            the expected schmea_tree of the value.
     * @return the parsed data tree.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static DataTree parseDataTree(Element data_tree_element, SchemaTree schema_tree) throws XmlParserException
    {
        Element root = XmlUtil.getOptionalChildElement(data_tree_element, ValueXmlSerializer.ROOT_ELM);
        
        // Check for nonexistent data tree
        if (root == null)
        {
            return null;
        }
        
        Value root_value = parseValue(root, schema_tree.getRootType());
        DataTree data_tree = new DataTree(root_value);
        
        data_tree.setType(schema_tree);
        
        // Map the value nodes to the type nodes
        ValueTypeMapUtil.map(data_tree, schema_tree);
        
        // Check that the data tree is type and constraint consistent
        if (!data_tree.checkConsistent())
        {
            String s = StringUtil.format("Data tree is not consistent w.r.t. its schema tree: <%s>", data_tree.toString());
            throw new XmlParserException(s);
        }
        
        return data_tree;
    }
    
    /**
     * Parses a value from an element.
     * 
     * @param value_element
     *            a value element of an XML DOM.
     * @param type
     *            the expected type of the value.
     * @return the parsed value.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static Value parseValue(Element value_element, Type type) throws XmlParserException
    {
        assert (type != null);
        assert (value_element != null);
        
        if (type instanceof ScalarType)
        {
            ScalarType scalar_type = (ScalarType) type;
            
            return parseScalarValue(scalar_type, value_element);
        }
        else if (type instanceof JsonType)
        {
            String text_content = DomApiProxy.getNodeValue(value_element);
            return JsonValue.parse(text_content);
        }
        else if (type instanceof TupleType)
        {
            String null_attr = value_element.getAttribute(ValueXmlSerializer.NULL_ATTR);
            
            if (!DomApiProxy.isEmpty(null_attr))
            {
                return new NullValue(TupleType.class);
            }
            
            TupleValue tuple_value = new TupleValue();
            TupleType tuple_type = (TupleType) type;
            
            for (Element child_element : XmlUtil.getChildElements(value_element))
            {
                String attribute_name = child_element.getTagName();
                Type attribute_type = tuple_type.getAttribute(attribute_name);
                if (attribute_type == null)
                {
                    String s = StringUtil.format("<%s> is not a valid attribute of <%s>", child_element.getTagName(),
                                                 value_element.getTagName());
                    throw new XmlParserException(s);
                }
                tuple_value.setAttribute(attribute_name, parseValue(child_element, attribute_type));
            }
            
            return tuple_value;
        }
        else if (type instanceof CollectionType)
        {
            CollectionValue collection_value = new CollectionValue();
            CollectionType collection_type = (CollectionType) type;
            boolean type_ordered = collection_type.isOrdered();
            Type children_type = collection_type.getChildrenType();
            String ordered = XmlUtil.getAttribute(value_element, ValueXmlSerializer.ORDERED);
            if (ordered == null) ordered = "false";
            assert (type_ordered == Boolean.valueOf(ordered));
            collection_value.setOrdered(type_ordered);
            for (Element child_element : XmlUtil.getChildElements(value_element))
            {
                Value child_value = parseValue(child_element, children_type);
                collection_value.add(child_value);
            }
            
            return collection_value;
        }
        else if (type instanceof SwitchType)
        {
            String null_attr = value_element.getAttribute(ValueXmlSerializer.NULL_ATTR);
            
            if (!DomApiProxy.isEmpty(null_attr))
            {
                return new NullValue(SwitchType.class);
            }
            
            SwitchValue switch_value = new SwitchValue();
            SwitchType switch_type = (SwitchType) type;
            
            List<Element> child_elements = XmlUtil.getChildElements(value_element);
            if (child_elements.size() != 1)
            {
                String s = StringUtil.format("Expecting exactly one case in <%s>; found %s instead", value_element.getTagName(),
                                             (new Integer(child_elements.size())).toString());
                throw new XmlParserException(s);
            }
            
            Element child_element = child_elements.get(0);
            String case_name = child_element.getTagName();
            Type case_type = switch_type.getCase(case_name);
            if (case_type == null)
            {
                String s = StringUtil.format("<%s> is not a valid case of <%s>", child_element.getTagName(),
                                             value_element.getTagName());
                throw new XmlParserException(s);
            }
            
            Value case_value = (Value) parseValue(child_element, case_type);
            switch_value.setCase(case_name, case_value);
            
            return switch_value;
        }
        else
        {
            String s = StringUtil.format("Encountered unknown type <%s>", type.getClass().getName());
            throw new XmlParserException(s);
        }
    }
    
    /**
     * Parses a scalar value from an element.
     * 
     * @param scalar_type
     *            the expected scalar type.
     * @param scalar_element
     *            the element.
     * @return the scalar value.
     * @throws XmlParserException
     *             if an error occurs during parsing.
     */
    private static Value parseScalarValue(ScalarType scalar_type, Element scalar_element) throws XmlParserException
    {
        String text_content = DomApiProxy.getNodeValue(scalar_element);
        String null_attr = scalar_element.getAttribute(ValueXmlSerializer.NULL_ATTR);
        
        // A null value is represented with a null="true" attribute
        if (!DomApiProxy.isEmpty(null_attr))
        {
            return new NullValue(scalar_type.getClass());
        }
        // XhtmlType needs the XML node and not the text content
        else if (scalar_type instanceof XhtmlType)
        {
            if (scalar_element.getChildNodes().getLength() == 1)
            {
                // The single HTML child element is the data
                return new XhtmlValue(scalar_element.getChildNodes().item(0));
            }
            else
            {
                StringBuilder sb = new StringBuilder();
                for (Node node : XmlUtil.getChildNodes(scalar_element))
                {
                    sb.append(XmlUtil.serializeDomToString(node, true, false));
                }
                return new XhtmlValue(sb.toString());
            }
        }
        // All other scalar values
        else
        {
            return parseScalarValue(scalar_type, text_content);
        }
        
    }
    
    /**
     * Parses a scalar value (or a scalar null value) of a given scalar type from a string literal.
     * 
     * @param type
     *            The class representing the scalar type to parse.
     * @param literal
     *            The literal to parse.
     * @return The scalar value, or a null scalar value if the literal is null.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static Value parseScalarValue(ScalarType type, String literal) throws XmlParserException
    {
        // Null literals are converted to null scalar values
        if (literal == null)
        {
            return new NullValue(type.getClass());
        }
        
        // Convert the other types
        if (type.getClass() == StringType.class)
        {
            return new StringValue(literal);
        }
        else if (type.getClass() == IntegerType.class)
        {
            try
            {
                return new IntegerValue(Integer.parseInt(literal));
            }
            catch (NumberFormatException e)
            {
                String s = StringUtil.format("Expecting integer value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == DoubleType.class)
        {
            try
            {
                return new DoubleValue(Double.parseDouble(literal));
            }
            catch (NumberFormatException e)
            {
                String s = StringUtil.format("Expecting double value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == FloatType.class)
        {
            try
            {
                return new FloatValue(Float.parseFloat(literal));
            }
            catch (NumberFormatException e)
            {
                String s = StringUtil.format("Expecting float value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == LongType.class)
        {
            try
            {
                return new LongValue(Long.parseLong(literal));
            }
            catch (NumberFormatException e)
            {
                String s = StringUtil.format("Expecting long value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == DecimalType.class)
        {
            try
            {
                return new DecimalValue(new BigDecimal(literal));
            }
            catch (NumberFormatException e)
            {
                String s = StringUtil.format("Expecting decimal value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == DateType.class)
        {
            try
            {
                long millisecs = DateFormat.getDateInstance(DateFormat.MEDIUM, Locale.US).parse(literal).getTime();
                return new DateValue(new Date(millisecs));
            }
            catch (Exception e)
            {
                String s = StringUtil.format("Expecting date value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == TimestampType.class)
        {
            try
            {
                return TimestampValue.parse(literal);
            }
            catch (Exception e)
            {
                String s = StringUtil.format("Expecting date time value; found %s instead", literal);
                throw new XmlParserException(s, e);
            }
        }
        else if (type.getClass() == BooleanType.class)
        {
            String trim = literal.trim();
            if (trim.equalsIgnoreCase("true"))
            {
                return new BooleanValue(true);
            }
            else if (trim.equalsIgnoreCase("false"))
            {
                return new BooleanValue(false);
            }
            else
            {
                String s = StringUtil.format("Expecting boolean value; found %s instead", literal);
                throw new XmlParserException(s);
            }
        }
        else if (type.getClass() == XhtmlType.class)
        {
            return new XhtmlValue(literal);
        }
        
        throw new AssertionError();
    }
    
}
