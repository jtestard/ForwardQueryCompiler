/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.StringUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.NonNullConstraint;
import edu.ucsd.forward.data.constraint.OrderByAttribute;
import edu.ucsd.forward.data.constraint.OrderConstraint;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.index.IndexMethod;
import edu.ucsd.forward.data.source.DataSourceException;
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
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.UnknownType;
import edu.ucsd.forward.data.type.XhtmlType;
import edu.ucsd.forward.data.value.ScalarValue;
import edu.ucsd.forward.query.ast.OrderByItem.Nulls;
import edu.ucsd.forward.query.ast.OrderByItem.Spec;
import edu.ucsd.forward.xml.AbstractXmlParser;
import edu.ucsd.forward.xml.DomApiProxy;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Utility class to parse a type or a schema tree from an XML DOM tree.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public final class TypeXmlParser extends AbstractXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger  log = Logger.getLogger(TypeXmlParser.class);
    
    /**
     * Collects the foreign key constraints to be parsed after all collection types have been parsed.
     */
    private static List<Element> s_foreign_key_constraint_elms;
    
    /**
     * Hidden constructor.
     */
    private TypeXmlParser()
    {
    }
    
    /**
     * Parses a schema tree from an XML DOM element in string .
     * 
     * @param schema_tree_str
     *            a schema tree element of an XML DOM in string.
     * @return the parsed schema tree.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static SchemaTree parseSchemaTree(String schema_tree_str) throws XmlParserException
    {
        Element schema_tree_element = (Element) XmlUtil.parseDomNode(schema_tree_str);
        return parseSchemaTree(schema_tree_element);
    }
    
    /**
     * Parses a schema tree from an XML DOM element. Foreign keys are ignored.
     * 
     * @param schema_tree_element
     *            a schema tree element of an XML DOM.
     * @return the parsed schema tree.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static SchemaTree parseSchemaTree(Element schema_tree_element) throws XmlParserException
    {
        Element root = XmlUtil.getOnlyChildElement(schema_tree_element, TypeXmlSerializer.ROOT_ELM);
        s_foreign_key_constraint_elms = new ArrayList<Element>();
        
        Type root_type = parseType(root);
        SchemaTree schema_tree = new SchemaTree(root_type);
        
        assert (TypeUtil.checkConstraintConsistent(schema_tree));
        
        return schema_tree;
    }
    
    /**
     * Parses a schema tree from an XML DOM element and accumulates the foreign key constraint DOM elements encountered.
     * 
     * @param schema_tree_element
     *            a schema tree element of an XML DOM.
     * @param foreign_key_constraint_elms
     *            key constraint elements encountered during parsing will be added to this collection.
     * @return the parsed schema tree.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static SchemaTree parseSchemaTree(Element schema_tree_element, Collection<Element> foreign_key_constraint_elms)
            throws XmlParserException
    {
        Element root = XmlUtil.getOnlyChildElement(schema_tree_element, TypeXmlSerializer.ROOT_ELM);
        s_foreign_key_constraint_elms = new ArrayList<Element>();
        
        Type root_type = parseType(root);
        SchemaTree schema_tree = new SchemaTree(root_type);
        
        foreign_key_constraint_elms.addAll(s_foreign_key_constraint_elms);
        
        assert (TypeUtil.checkConstraintConsistent(schema_tree));
        
        return schema_tree;
    }
    
    /**
     * Parses a type from an XML DOM element.
     * 
     * @param type_element
     *            a type element of an XML DOM.
     * @return the parsed type.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static Type parseType(Element type_element) throws XmlParserException
    {
        assert (type_element != null);
        
        String type_name = type_element.getAttribute(TypeXmlSerializer.TYPE_ATTR);
        Type type = null;
        
        if (type_name.equals(TypeEnum.TUPLE.getName()))
        {
            TupleType tuple_type = new TupleType();
            
            for (Element child_element : XmlUtil.getChildElements(type_element))
            {
                String attribute_name = child_element.getTagName();
                if (attribute_name.equals(TypeXmlSerializer.CONSTRAINTS_ELM)) continue;
                tuple_type.setAttribute(attribute_name, parseType(child_element));
            }
            
            type = tuple_type;
        }
        else if (type_name.equals(TypeEnum.COLLECTION.getName()))
        {
            CollectionType collection_type = new CollectionType();
            String ordered = type_element.getAttribute(TypeXmlSerializer.ORDERED);
            if(ordered != null)
            {
                collection_type.setOrdered(Boolean.valueOf(ordered));
            }
            Element child_element = XmlUtil.getOnlyChildElement(type_element, TypeXmlSerializer.TYPE_ELEMENT);
            Type child_type = parseType(child_element);
            collection_type.setChildrenType(child_type);
            
            for (Element idx_element : XmlUtil.getChildElements(type_element, TypeXmlSerializer.INDEX_ELM))
            {
                IndexDeclaration declaration = parseIndexDeclaration(idx_element, collection_type);
                try
                {
                    collection_type.addIndex(declaration);
                }
                catch (DataSourceException e)
                {
                    throw new XmlParserException("Error adding index", e);
                }
            }
            
            type = collection_type;
        }
        else if (type_name.equals(TypeEnum.SWITCH.getName()))
        {
            SwitchType switch_type = new SwitchType();
            
            List<Element> child_elements = XmlUtil.getChildElements(type_element);
            if (child_elements.size() == 0)
            {
                String s = StringUtil.format("Expecting at least one case in <%s>; found 0 instead", type_element.getTagName());
                throw new XmlParserException(s);
            }
            
            for (Element child : child_elements)
            {
                String case_name = child.getTagName();
                Type case_type = parseType(child);
                
                switch_type.setCase(case_name, case_type);
            }
            
            type = switch_type;
        }
        else if (type_name.equals(TypeEnum.JSON.getName()))
        {
            type = new JsonType();
        }
        else if (type_name.equals(TypeEnum.UNKNOWN.getName()))
        {
            type = new UnknownType();
        }
        else
        {
            type = parseScalarType(type_element);
        }
        
        Element constaints = XmlUtil.getOptionalChildElement(type_element, TypeXmlSerializer.CONSTRAINTS_ELM);
        if (constaints != null)
        {
            parseConstraints(constaints, type);
        }
        
        return type;
    }
    
    /**
     * Parses index declaration.
     * 
     * @param idx_element
     *            the index element.
     * @param collection_type
     *            the collection type where the index to add
     * @return the created index declaration.
     * @throws XmlParserException
     *             if anything wrong with the index parsing.
     */
    private static IndexDeclaration parseIndexDeclaration(Element idx_element, CollectionType collection_type)
            throws XmlParserException
    {
        String name = idx_element.getAttribute(TypeXmlSerializer.INDEX_NAME_ATTR);
        String unique_attr = idx_element.getAttribute(TypeXmlSerializer.INDEX_UNIQUE_ATTR);
        String method_attr = idx_element.getAttribute(TypeXmlSerializer.INDEX_METHOD_ATTR);
        IndexMethod method = !DomApiProxy.isEmpty(method_attr) ? IndexMethod.valueOf(method_attr.toUpperCase()) : IndexMethod.BTREE;
        boolean unique = !DomApiProxy.isEmpty(unique_attr) ? Boolean.parseBoolean(unique_attr) : false;
        List<ScalarType> keys = new ArrayList<ScalarType>();
        for (Element key_element : XmlUtil.getChildElements(idx_element, TypeXmlSerializer.INDEX_KEY_ELM))
        {
            String path = key_element.getAttribute(TypeXmlSerializer.INDEX_PATH_ATTR);
            SchemaPath schema_path = null;
            try
            {
                schema_path = new SchemaPath(path);
            }
            catch (ParseException e)
            {
                throw new XmlParserException("Error creating schema path", e);
            }
            ScalarType key = (ScalarType) schema_path.find(collection_type);
            assert key != null;
            keys.add(key);
        }
        IndexDeclaration declaration = new IndexDeclaration(name, collection_type, keys, unique, method);
        return declaration;
    }
    
    /**
     * Parses a scalar type from an element.
     * 
     * @param type_element
     *            the scalar type element.
     * @return the scalar type.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    private static ScalarType parseScalarType(Element type_element) throws XmlParserException
    {
        String type_name = type_element.getAttribute(TypeXmlSerializer.TYPE_ATTR);
        ScalarType scalar_type;
        
        if (type_name.equals(TypeEnum.STRING.getName()))
        {
            scalar_type = new StringType();
        }
        else if (type_name.equals(TypeEnum.DECIMAL.getName()))
        {
            scalar_type = new DecimalType();
        }
        else if (type_name.equals(TypeEnum.INTEGER.getName()))
        {
            scalar_type = new IntegerType();
        }
        else if (type_name.equals(TypeEnum.LONG.getName()))
        {
            scalar_type = new LongType();
        }
        else if (type_name.equals(TypeEnum.FLOAT.getName()))
        {
            scalar_type = new FloatType();
        }
        else if (type_name.equals(TypeEnum.DOUBLE.getName()))
        {
            scalar_type = new DoubleType();
        }
        else if (type_name.equals(TypeEnum.DATE.getName()))
        {
            scalar_type = new DateType();
        }
        else if (type_name.equals(TypeEnum.TIMESTAMP.getName()))
        {
            scalar_type = new TimestampType();
        }
        else if (type_name.equals(TypeEnum.BOOLEAN.getName()))
        {
            scalar_type = new BooleanType();
        }
        else if (type_name.equals(TypeEnum.XHTML.getName()))
        {
            scalar_type = new XhtmlType();
        }
        else
        {
            String s = StringUtil.format("<%s> has an unknown type <%s>", type_element.getTagName(), type_name);
            throw new XmlParserException(s);
        }
        
        if (type_element.hasAttribute(TypeXmlSerializer.DEFAULT_ATTR))
        {
            // Get default value
            String default_value = type_element.getAttribute(TypeXmlSerializer.DEFAULT_ATTR);
            scalar_type.setDefaultValue((ScalarValue) ValueXmlParser.parseScalarValue(scalar_type, default_value));
        }
        
        return scalar_type;
    }
    
    /**
     * Parses type constraints.
     * 
     * @param constraints_element
     *            a constraints element of an XML DOM.
     * @param type
     *            the type to add the constraints
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    private static void parseConstraints(Element constraints_element, Type type) throws XmlParserException
    {
        for (Element constraint_elm : XmlUtil.getChildElements(constraints_element))
        {
            if (constraint_elm.getTagName().equals(TypeXmlSerializer.LOCAL_KEY_ELM))
            {
                assert (type instanceof CollectionType);
                parseLocalKeyConstraint(constraint_elm, (CollectionType) type);
            }
            else if (constraint_elm.getTagName().equals(TypeXmlSerializer.ORDER_ELM))
            {
                assert (type instanceof CollectionType);
                parseOrderConstraint(constraint_elm, (CollectionType) type);
            }
            else if (constraint_elm.getTagName().equals(TypeXmlSerializer.NON_NULL_ELM))
            {
                parseNonNullConstraint(constraint_elm, type);
            }
            else if (constraint_elm.getTagName().equals(TypeXmlSerializer.FOREIGN_KEY_ELM))
            {
                assert (type instanceof CollectionType);
                // Collect the foreign key constraints and parse them after all collection types have been parsed.
                s_foreign_key_constraint_elms.add(constraint_elm);
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }
    }
    
    private static void parseOrderConstraint(Element constraint_element, CollectionType collection_type) throws XmlParserException
    {
        // Retrieve the paths to the key attributes of scalar type
        List<OrderByAttribute> attributes = new ArrayList<OrderByAttribute>();
        for (Element child : XmlUtil.getChildElements(constraint_element, TypeXmlSerializer.ATTRIBUTE_ELM))
        {
            if (!child.hasAttribute(TypeXmlSerializer.PATH_ATTR))
            {
                String s = StringUtil.format("Element <%s> needs to have an attribute <%s>", TypeXmlSerializer.ATTRIBUTE_ELM,
                                             TypeXmlSerializer.PATH_ATTR);
                throw new XmlParserException(s);
            }
            
            String attribute_path = child.getAttribute(TypeXmlSerializer.PATH_ATTR);
            SchemaPath path;
            try
            {
                path = new SchemaPath(attribute_path);
            }
            catch (ParseException e)
            {
                String s = StringUtil.format("Invalid schema path: <%s>", attribute_path);
                throw new XmlParserException(s, e);
            }
            Type attr_type = path.find(collection_type);
            
            if (attr_type == null)
            {
                String s = StringUtil.format("No type found at path: <%s>", attribute_path);
                throw new XmlParserException(s);
            }
            
            if (!child.hasAttribute(TypeXmlSerializer.ORDER_SPEC_ATTR))
            {
                String s = StringUtil.format("Element <%s> needs to have an attribute <%s>", TypeXmlSerializer.ATTRIBUTE_ELM,
                                             TypeXmlSerializer.ORDER_SPEC_ATTR);
                throw new XmlParserException(s);
            }
            
            if (!child.hasAttribute(TypeXmlSerializer.ORDER_NULLS_ATTR))
            {
                String s = StringUtil.format("Element <%s> needs to have an attribute <%s>", TypeXmlSerializer.ATTRIBUTE_ELM,
                                             TypeXmlSerializer.ORDER_NULLS_ATTR);
                throw new XmlParserException(s);
            }
            Spec spec = Spec.valueOf(child.getAttribute(TypeXmlSerializer.ORDER_SPEC_ATTR));
            Nulls nulls = Nulls.valueOf(child.getAttribute(TypeXmlSerializer.ORDER_NULLS_ATTR));
            OrderByAttribute attr = new OrderByAttribute(attr_type, spec, nulls);
            attributes.add(attr);
        }
        
        OrderConstraint constraint = new OrderConstraint(collection_type, attributes);
        collection_type.addConstraint(constraint);
    }
    
    /**
     * Parses a local key constraint, which comes with a collection type.
     * 
     * @param constraint_element
     *            a local key constraint element of an XML DOM.
     * @param collection_type
     *            the collection type to add the constraint.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    private static void parseLocalKeyConstraint(Element constraint_element, CollectionType collection_type)
            throws XmlParserException
    {
        // Retrieve the paths to the key attributes of scalar type
        List<Type> attributes = new ArrayList<Type>();
        for (Element child : XmlUtil.getChildElements(constraint_element, TypeXmlSerializer.ATTRIBUTE_ELM))
        {
            if (!child.hasAttribute(TypeXmlSerializer.PATH_ATTR))
            {
                String s = StringUtil.format("Element <%s> needs to have an attribute <%s>", TypeXmlSerializer.ATTRIBUTE_ELM,
                                             TypeXmlSerializer.PATH_ATTR);
                throw new XmlParserException(s);
            }
            
            String attribute_path = child.getAttribute(TypeXmlSerializer.PATH_ATTR);
            SchemaPath path;
            try
            {
                path = new SchemaPath(attribute_path);
            }
            catch (ParseException e)
            {
                String s = StringUtil.format("Invalid schema path: <%s>", attribute_path);
                throw new XmlParserException(s, e);
            }
            Type attr_type = path.find(collection_type);
            
            if (attr_type == null || !(attr_type instanceof ScalarType))
            {
                String s = StringUtil.format("No scalar type found at path: <%s>", attribute_path);
                throw new XmlParserException(s);
            }
            
            attributes.add((ScalarType) attr_type);
        }
        
        if (attributes.size() == 0)
        {
            String s = StringUtil.format("Element <%s> needs to have at least one <%s> subelement",
                                         TypeXmlSerializer.LOCAL_KEY_ELM, TypeXmlSerializer.ATTRIBUTE_ELM);
            throw new XmlParserException(s);
        }
        
        // Check that the closest ancestor collection type of all key attributes is the same one
        for (Type type : attributes)
        {
            CollectionType closest_relation_type = type.getClosestAncestor(CollectionType.class);
            if (closest_relation_type == null || closest_relation_type != collection_type)
            {
                // Could not display more information regarding which collection it is because at this point the collection type is
                // detached.
                String s = StringUtil.format("The attributes of <%s> do not have the same closest ancestor collection type",
                                             TypeXmlSerializer.LOCAL_KEY_ELM);
                throw new XmlParserException(s);
            }
        }
        
        new LocalPrimaryKeyConstraint(collection_type, attributes);
    }
    
    /**
     * Parses a non null constraint, which comes with a type.
     * 
     * @param constraint_element
     *            a non null constraint element of an XML DOM.
     * @param type
     *            the type to add the constraint.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    private static void parseNonNullConstraint(Element constraint_element, Type type) throws XmlParserException
    {
        new NonNullConstraint(type);
    }
    
}
