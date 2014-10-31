/**
 * 
 */
package edu.ucsd.forward.xml;

import static edu.ucsd.forward.xml.XmlUtil.createDocument;

import com.google.gwt.xml.client.Document;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Abstract class for implementing XML serializers.
 * 
 * @author Yupeng
 * 
 */
public class AbstractXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractXmlSerializer.class);
    
    private Document            m_document;
    
    /**
     * Constructs the converter.
     */
    public AbstractXmlSerializer()
    {
        m_document = createDocument();
    }
    
    /**
     * Constructs with an existing document.
     */
    public AbstractXmlSerializer(Document document)
    {
        assert document != null;
        m_document = document;
    }
    
    /**
     * Get the DOM document.
     * 
     * @return the DOM document
     */
    protected Document getDomDocument()
    {
        return m_document;
    }
}
