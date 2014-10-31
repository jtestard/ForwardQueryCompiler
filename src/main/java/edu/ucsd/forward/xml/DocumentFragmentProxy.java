/**
 * 
 */
package edu.ucsd.forward.xml;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A wrapper of GWT's DOM document fragment to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class DocumentFragmentProxy extends NodeProxy implements com.google.gwt.xml.client.DocumentFragment
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DocumentFragmentProxy.class);
    
    /**
     * Default constructor.
     */
    protected DocumentFragmentProxy()
    {
        super();
    }
    
    /**
     * Constructs a GWT document fragment from a W3C document fragment.
     * 
     * @param d
     *            the W3C document fragment
     * @return the constructed GWT document fragment.
     */
    public static DocumentFragmentProxy makeGwtDocumentFragment(org.w3c.dom.DocumentFragment d)
    {
        if (d == null)
        {
            return null;
        }
        DocumentFragmentProxy d1 = new DocumentFragmentProxy();
        d1.n = d;
        return (d1);
    }
}
