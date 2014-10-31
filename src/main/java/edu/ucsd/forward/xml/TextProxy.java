/**
 * 
 */
package edu.ucsd.forward.xml;

import com.google.gwt.xml.client.Text;

/**
 * A wrapper of GWT's DOM node list to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class TextProxy extends CharacterDataProxy implements com.google.gwt.xml.client.Text
{
    
    @Override
    public Text splitText(int offset)
    {
        return (TextProxy) makeGwtNode(((org.w3c.dom.Text) n).splitText(offset));
    }
    
}
