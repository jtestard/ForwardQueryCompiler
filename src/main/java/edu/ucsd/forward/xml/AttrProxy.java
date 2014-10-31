/**
 * 
 */
package edu.ucsd.forward.xml;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A wrapper of GWT's DOM attr to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class AttrProxy extends NodeProxy implements com.google.gwt.xml.client.Attr
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AttrProxy.class);
    
    /**
     * Default constructor.
     */
    protected AttrProxy()
    {
        super();
    }
    
    /**
     * Constructs a GWT attr from a W3C attr.
     * 
     * @param e
     *            the W3C attr
     * @return the constructed GWT attr.
     */
    public static AttrProxy makeGwtAttr(org.w3c.dom.Attr a)
    {
        if (a == null)
        {
            return null;
        }
        AttrProxy a1 = new AttrProxy();
        a1.n = a;
        return (a1);
    }
    
    @Override
    public String getName()
    {
        return ((org.w3c.dom.Attr) n).getName();
    }
    
    @Override
    public boolean getSpecified()
    {
        return ((org.w3c.dom.Attr) n).getSpecified();
    }
    
    @Override
    public String getValue()
    {
        return ((org.w3c.dom.Attr) n).getValue();
    }
}
