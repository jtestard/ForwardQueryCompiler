/**
 * 
 */
package edu.ucsd.forward.data.mapping;

import static edu.ucsd.forward.xml.XmlUtil.createChildElement;

import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;

/**
 * Serializes the mapping.
 * 
 * @author Yupeng Fu
 * 
 */
public final class MappingXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger   log               = Logger.getLogger(MappingXmlSerializer.class);
    
    public static final String    MAPPING_GROUP_ELM = "mapping_group";
    
    public static final String    MAPPING_ELM       = "mapping";
    
    protected static final String SOURCE_PATH_ATTR  = "source_path";
    
    protected static final String TARGET_PATH_ATTR  = "target_path";
    
    protected static final String PREDICATES_ELM    = "predicates";
    
    protected static final String PREDICATE_ELM     = "predicate";
    
    protected static final String PARAMETER_ATTR    = "parameter";
    
    /**
     * Hidden constructor.
     */
    private MappingXmlSerializer()
    {
        
    }
    
    /**
     * Serializes a mapping group to an XML DOM element.
     * 
     * @param group
     *            a mapping group to serialize.
     * @param parent_node
     *            the parent XML node.
     */
    public static void serializeMappingGroup(MappingGroup group, Node parent_node)
    {
        assert (parent_node != null);
        
        Element mapping_group_elm = createChildElement(parent_node, MAPPING_GROUP_ELM);
        
        for (Mapping mapping : group.getMappings())
        {
            Element mapping_elm = createChildElement(mapping_group_elm, MAPPING_ELM);
            mapping_group_elm.setAttribute(SOURCE_PATH_ATTR, mapping.getSourcePath().toString());
            mapping_group_elm.setAttribute(TARGET_PATH_ATTR, mapping.getTargetPathString());
            Element predicates_elm = createChildElement(mapping_elm, PREDICATES_ELM);
            for (DataPath parameter : mapping.getParameters())
            {
                Element predicate_elm = createChildElement(predicates_elm, PREDICATE_ELM);
                predicate_elm.setAttribute(PARAMETER_ATTR, parameter.getString());
            }
        }
    }
}
