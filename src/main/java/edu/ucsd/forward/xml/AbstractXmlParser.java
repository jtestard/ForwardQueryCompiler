/**
 * 
 */
package edu.ucsd.forward.xml;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Abstract class for implementing XML parsers.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractXmlParser implements XmlParser
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractXmlParser.class);
    
    /**
     * Constructs the parser.
     */
    public AbstractXmlParser()
    {
    }
}
