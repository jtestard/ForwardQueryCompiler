/**
 * 
 */
package edu.ucsd.forward.data.xml;

import static edu.ucsd.forward.xml.XmlUtil.createChildElement;

import com.google.gwt.xml.client.Document;
import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.SchemaPath;
import edu.ucsd.forward.data.constraint.Constraint;
import edu.ucsd.forward.data.constraint.ForeignKeyConstraint;
import edu.ucsd.forward.data.constraint.LocalPrimaryKeyConstraint;
import edu.ucsd.forward.data.constraint.NonNullConstraint;
import edu.ucsd.forward.data.constraint.OrderByAttribute;
import edu.ucsd.forward.data.constraint.OrderConstraint;
import edu.ucsd.forward.data.constraint.SingletonConstraint;
import edu.ucsd.forward.data.index.IndexDeclaration;
import edu.ucsd.forward.data.source.DataSourceXmlParser;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.ScalarType;
import edu.ucsd.forward.data.type.SchemaTree;
import edu.ucsd.forward.data.type.SwitchType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.TupleType.AttributeEntry;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.data.type.UnknownType;
import edu.ucsd.forward.xml.AbstractXmlSerializer;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * Utility class to serialize a type or a schema tree to an XML DOM tree.
 * 
 * @author Kian Win
 * @author Yupeng
 * @author Michalis Petropoulos
 */
public final class TypeXmlSerializer extends AbstractXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger   log                      = Logger.getLogger(TypeXmlSerializer.class);
    
    public static final String    SCHEMA_TREE_ELM          = "schema_tree";
    
    public static final String    ROOT_ELM                 = "root";
    
    protected static final String TYPE_ATTR                = "type";
    
    protected static final String DEFAULT_ATTR             = "default";
    
    protected static final String CONSTRAINTS_ELM          = "constraints";
    
    protected static final String LOCAL_KEY_ELM            = "local-key";
    
    protected static final String ORDER_ELM                = "order";
    
    protected static final String ORDER_SPEC_ATTR          = "spec";
    
    protected static final String ORDER_NULLS_ATTR         = "nulls";
    
    protected static final String COLLECTION_ATTR          = "collection";
    
    public static final String    ATTRIBUTE_ELM            = "attribute";
    
    public static final String    PATH_ATTR                = "path";
    
    protected static final String NON_NULL_ELM             = "non-null";
    
    protected static final String FOREIGN_KEY_ELM          = "foreign-key";
    
    public static final String    FOREIGN_ELM              = "foreign";
    
    public static final String    PRIMARY_ELM              = "primary";
    
    private static final String   SINGLETON_CONSTRAINT_ELM = "singleton";
    
    protected static final String INDEX_ELM                = "index";
    
    protected static final String INDEX_NAME_ATTR          = "name";
    
    protected static final String INDEX_UNIQUE_ATTR        = "unique";
    
    protected static final String INDEX_KEY_ELM            = "key";
    
    protected static final String INDEX_PATH_ATTR          = "path";
    
    protected static final String INDEX_METHOD_ATTR        = "method";
    
    protected static final String TYPE_ELEMENT             = "element";
    
    protected static final String ORDERED                  = "ordered";
    
    /**
     * Hidden constructor.
     */
    private TypeXmlSerializer()
    {
    }
    
    /**
     * Serializes a schema tree to an XML in string.
     * 
     * @param schema_tree
     *            a schema tree to serialize.
     * @return the XML in string format.
     */
    public static String serializeSchemaTree(SchemaTree schema_tree)
    {
        Document document = XmlUtil.createDocument();
        Element schema_tree_elm = XmlUtil.createRootElement(document, SCHEMA_TREE_ELM);
        serializeType(schema_tree.getRootType(), ROOT_ELM, schema_tree_elm);
        return XmlUtil.serializeDomToString(schema_tree_elm, true, true);
    }
    
    /**
     * Serializes a schema tree to an XML DOM element.
     * 
     * @param schema_tree
     *            a schema tree to serialize.
     * @param parent_node
     *            the parent XML node.
     */
    public static void serializeSchemaTree(SchemaTree schema_tree, Node parent_node)
    {
        assert (parent_node != null);
        
        Element schema_tree_elm = createChildElement(parent_node, SCHEMA_TREE_ELM);
        
        serializeType(schema_tree.getRootType(), ROOT_ELM, schema_tree_elm);
    }
    
    /**
     * Serializes a type to an XML DOM element.
     * 
     * @param type
     *            the type to serialize.
     * @param tag_name
     *            the tag name to be used when serializing the input type.
     * @param parent_node
     *            the parent XML node.
     */
    public static void serializeType(Type type, String tag_name, Node parent_node)
    {
        assert (parent_node != null);
        assert (type != null);
        assert (tag_name != null);
        
        Element type_element = createChildElement(parent_node, tag_name);
        
        type_element.setAttribute(TYPE_ATTR, TypeEnum.getName(type));
        
        // Recurse
        
        if (type instanceof TupleType)
        {
            TupleType tuple_type = (TupleType) type;
            for (AttributeEntry entry : tuple_type)
            {
                Type attribute_type = entry.getType();
                serializeType(attribute_type, entry.getName(), type_element);
            }
        }
        else if (type instanceof CollectionType)
        {
            CollectionType collection_type = (CollectionType) type;
            if(collection_type.isOrdered())
            {
                type_element.setAttribute(ORDERED, String.valueOf(true));
            }
            Type child_type = collection_type.getChildrenType();
            serializeType(child_type, TYPE_ELEMENT, type_element);
            serializeIndex(collection_type, type_element);
        }
        else if (type instanceof SwitchType)
        {
            SwitchType switch_type = (SwitchType) type;
            for (String case_name : switch_type.getCaseNames())
            {
                Type case_type = switch_type.getCase(case_name);
                serializeType(case_type, case_name, type_element);
            }
        }
        else if (type instanceof NullType)
        {
            // Do nothing
        }
        else if (type instanceof ScalarType)
        {
            // Do nothing
        }
        else
        {
            assert (type instanceof UnknownType);
        }
        
        // Set the default value
        // type_element.setAttribute(DEFAULT_ATTR, type.getDefaultValue().toString());
        
        serializeConstraints(type, type_element);
    }
    
    /**
     * Serializes all the indices in the collection.
     * 
     * @param collection_type
     *            the collection that holds indices.
     * @param type_element
     *            the serialized type element.
     */
    private static void serializeIndex(CollectionType collection_type, Element type_element)
    {
        for (IndexDeclaration declaration : collection_type.getIndexDeclarations())
        {
            Element idx_element = createChildElement(type_element, INDEX_ELM);
            idx_element.setAttribute(INDEX_NAME_ATTR, declaration.getName());
            idx_element.setAttribute(INDEX_UNIQUE_ATTR, new Boolean(declaration.isUnique()).toString());
            idx_element.setAttribute(INDEX_METHOD_ATTR, declaration.getMethod().name());
            for (SchemaPath path : declaration.getKeyPaths())
            {
                Element key_element = createChildElement(idx_element, INDEX_KEY_ELM);
                key_element.setAttribute(INDEX_PATH_ATTR, path.toString());
            }
        }
    }
    
    /**
     * Serializes the constraints of the given type.
     * 
     * @param type
     *            a type with a list of constraints.
     * @param parent_node
     *            parent node to attach to.
     */
    public static void serializeConstraints(Type type, Node parent_node)
    {
        Element constraints_element = null;
        if (!type.getConstraints().isEmpty())
        {
            constraints_element = createChildElement(parent_node, CONSTRAINTS_ELM);
        }
        for (Constraint c : type.getConstraints())
        {
            serializeConstraint(type, c, constraints_element);
        }
    }
    
    /**
     * Serializes a constraint to an XML DOM element.
     * 
     * @param type
     *            the type to which the constraint is attached.
     * @param constraint
     *            the constraint to serialize.
     * @param parent_node
     *            the parent node.
     */
    private static void serializeConstraint(Type type, Constraint constraint, Node parent_node)
    {
        if (constraint instanceof LocalPrimaryKeyConstraint)
        {
            serializeLocalPrimaryKeyConstraint(parent_node, (LocalPrimaryKeyConstraint) constraint);
        }
        else if (constraint instanceof OrderConstraint)
        {
            serializeOrderConstraint(parent_node, (OrderConstraint) constraint);
        }
        else if (constraint instanceof SingletonConstraint)
        {
            serializeSingletonConstraint(parent_node, (SingletonConstraint) constraint);
        }
        else if (constraint instanceof NonNullConstraint)
        {
            serializeNonNullConstraint(parent_node, (NonNullConstraint) constraint);
        }
        else if (constraint instanceof ForeignKeyConstraint)
        {
            ForeignKeyConstraint foreign = (ForeignKeyConstraint) constraint;
            // Serialize only if the given type is the foreign one
            if (type == foreign.getForeignCollectionType())
            {
                serializeForeignKeyConstraint(parent_node, (ForeignKeyConstraint) constraint);
            }
        }
        else
        {
            throw new UnsupportedOperationException();
        }
    }
    
    /**
     * Serialize a singleton constraint to an XML DOM element.
     * 
     * @param parent_node
     *            the parent node
     * @param constraint
     *            the singleton constraint
     */
    private static void serializeSingletonConstraint(Node parent_node, SingletonConstraint constraint)
    {
        createChildElement(parent_node, SINGLETON_CONSTRAINT_ELM);
    }
    
    /**
     * Serializes a local primary key constraint to an XML DOM element.
     * 
     * @param parent_node
     *            the parent node
     * @param local_key_constraint
     *            the local primary key constraint to serialize.
     */
    private static void serializeLocalPrimaryKeyConstraint(Node parent_node, LocalPrimaryKeyConstraint local_key_constraint)
    {
        Element local_key_element = createChildElement(parent_node, LOCAL_KEY_ELM);
        for (ScalarType key_type : local_key_constraint.getAttributes())
        {
            SchemaPath path = new SchemaPath(local_key_constraint.getCollectionType(), key_type);
            Element attr_element = createChildElement(local_key_element, ATTRIBUTE_ELM);
            attr_element.setAttribute(PATH_ATTR, path.getString());
        }
    }
    
    /**
     * Serializes an order constraint to an XML DOM element.
     * 
     * @param parent_node
     *            the parent node
     * @param order_constraint
     *            the order constraint to serialize
     */
    private static void serializeOrderConstraint(Node parent_node, OrderConstraint order_constraint)
    {
        Element order_constraint_element = createChildElement(parent_node, ORDER_ELM);
        for (OrderByAttribute attr : order_constraint.getOrderByAttributes())
        {
            SchemaPath path = new SchemaPath(order_constraint.getCollectionType(), attr.getAttribute());
            Element attr_element = createChildElement(order_constraint_element, ATTRIBUTE_ELM);
            attr_element.setAttribute(PATH_ATTR, path.getString());
            attr_element.setAttribute(ORDER_SPEC_ATTR, attr.getSpec().name());
            attr_element.setAttribute(ORDER_NULLS_ATTR, attr.getNulls().name());
        }
    }
    
    /**
     * Serializes a non null constraint to an XML DOM element.
     * 
     * @param parent_node
     *            the parent node
     * @param non_null_constraint
     *            the non null constraint to serialize.
     */
    private static void serializeNonNullConstraint(Node parent_node, NonNullConstraint non_null_constraint)
    {
        createChildElement(parent_node, NON_NULL_ELM);
    }
    
    /**
     * Serializes a foreign key constraint to an XML DOM element.
     * 
     * @param parent_node
     *            the parent node
     * @param foreign_key_constraint
     *            the foreign key constraint to serialize.
     */
    private static void serializeForeignKeyConstraint(Node parent_node, ForeignKeyConstraint foreign_key_constraint)
    {
        SchemaPath path;
        
        Element foreign_key_elm = createChildElement(parent_node, FOREIGN_KEY_ELM);
        
        Element foreign_elm = createChildElement(foreign_key_elm, FOREIGN_ELM);
        foreign_elm.setAttribute(DataSourceXmlParser.DATA_SOURCE_ELM,
                                 foreign_key_constraint.getForeignHandle().getDataSourceName());
        foreign_elm.setAttribute(DataSourceXmlParser.DATA_OBJECT_ELM,
                                 foreign_key_constraint.getForeignHandle().getSchemaObjectName());
        path = new SchemaPath(foreign_key_constraint.getForeignCollectionType());
        foreign_elm.setAttribute(PATH_ATTR, path.getString());
        for (ScalarType key_type : foreign_key_constraint.getForeignAttributes())
        {
            path = new SchemaPath(foreign_key_constraint.getForeignCollectionType(), key_type);
            Element attr_element = createChildElement(foreign_elm, ATTRIBUTE_ELM);
            attr_element.setAttribute(PATH_ATTR, path.getString());
        }
        
        Element primary_elm = createChildElement(foreign_key_elm, PRIMARY_ELM);
        primary_elm.setAttribute(DataSourceXmlParser.DATA_SOURCE_ELM,
                                 foreign_key_constraint.getPrimaryHandle().getDataSourceName());
        primary_elm.setAttribute(DataSourceXmlParser.DATA_OBJECT_ELM,
                                 foreign_key_constraint.getPrimaryHandle().getSchemaObjectName());
        path = new SchemaPath(foreign_key_constraint.getPrimaryCollectionType());
        primary_elm.setAttribute(PATH_ATTR, path.getString());
        for (ScalarType key_type : foreign_key_constraint.getPrimaryAttributes())
        {
            path = new SchemaPath(foreign_key_constraint.getPrimaryCollectionType(), key_type);
            Element attr_element = createChildElement(primary_elm, ATTRIBUTE_ELM);
            attr_element.setAttribute(PATH_ATTR, path.getString());
        }
    }
    
}
