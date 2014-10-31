/**
 * 
 */
package edu.ucsd.forward.xml;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A wrapper of GWT's DOM comment to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class CommentProxy extends CharacterDataProxy implements com.google.gwt.xml.client.Comment
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(CommentProxy.class);
}
