/**
 * 
 */
package edu.ucsd.forward.data.mapping;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.google.gwt.xml.client.Element;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.xml.XmlParserException;
import edu.ucsd.forward.xml.XmlUtil;

/**
 * 
 * Deserializes the mapping.
 * 
 * @author Yupeng Fu
 * 
 */
public final class MappingXmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(MappingXmlParser.class);
    
    /**
     * Hidden constructor.
     */
    private MappingXmlParser()
    {
        
    }
    
    /**
     * Parses a mapping group.
     * 
     * @param mapping_group_element
     *            a mapping group element of an XML DOM.
     * @return the parsed mapping group.
     * @throws XmlParserException
     *             if an error occurs during conversion.
     */
    public static MappingGroup parseMappingGroup(Element mapping_group_element) throws XmlParserException
    {
        MappingGroup group = new MappingGroup();
        
        for (Element mapping_elm : XmlUtil.getChildElements(mapping_group_element, MappingXmlSerializer.MAPPING_ELM))
        {
            try
            {
                String source_path = XmlUtil.getAttribute(mapping_elm, MappingXmlSerializer.SOURCE_PATH_ATTR);
                String target_path = XmlUtil.getAttribute(mapping_elm, MappingXmlSerializer.TARGET_PATH_ATTR);
                List<DataPath> predicates = new ArrayList<DataPath>();
                Element predicates_elm = XmlUtil.getOnlyChildElement(mapping_elm, MappingXmlSerializer.PREDICATES_ELM);
                for (Element predicate_elm : XmlUtil.getChildElements(predicates_elm, MappingXmlSerializer.PREDICATE_ELM))
                {
                    String parameter = XmlUtil.getAttribute(predicate_elm, MappingXmlSerializer.PARAMETER_ATTR);
                    
                    predicates.add(new DataPath(parameter));
                }
                
                Mapping mapping = new Mapping(new DataPath(source_path), target_path, predicates);
                group.addMapping(mapping);
            }
            catch (ParseException e)
            {
                throw new XmlParserException(e.getMessage());
            }
        }
        return group;
    }
}
