/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.util.List;

import com.google.gwt.xml.client.Element;
import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.SystemUtil;
import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.diff.Diff;
import edu.ucsd.forward.xml.AbstractXmlSerializer;

/**
 * Utility class to serialize service configuration into a DOM Document.
 * 
 * @author Hongping Lim
 * 
 */
public class DataDiffXmlSerializer extends AbstractXmlSerializer
{
    @SuppressWarnings("unused")
    private static final Logger log         = Logger.getLogger(DataDiffXmlSerializer.class);
    
    private static final String CONTEXT_TAG = "context";
    
    private static final String DIFFS_TAG   = "diffs";
    
    private static final String PAYLOAD_TAG = "payload";
    
    /**
     * Converts a diff into XML node.
     * 
     * @param diff
     *            diff
     * @return XML node
     */
    public Node serialize(Diff diff)
    {
        String diff_type = SystemUtil.getSimpleName(diff.getClass());
        Element root = getDomDocument().createElement(diff_type);
        root.setAttribute(CONTEXT_TAG, diff.getContext().toString());
        if (diff.getPayload() != null)
        {
            ValueXmlSerializer.serializeValue(diff.getPayload(), PAYLOAD_TAG, (Node) root);
        }
        return root;
    }
    
    /**
     * Converts a list of diffs into XML node.
     * 
     * @param diffs
     *            diffs
     * @return XML node
     */
    public Node serialize(List<Diff> diffs)
    {
        Element root = getDomDocument().createElement(DIFFS_TAG);
        for (Diff diff : diffs)
        {
            Node diff_node = serialize(diff);
            root.appendChild(diff_node);
        }
        return root;
    }
}
