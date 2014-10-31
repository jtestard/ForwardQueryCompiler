/**
 * 
 */
package edu.ucsd.forward.xml;

import java.util.ArrayList;
import java.util.List;

import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A range of consecutive nodes. Both sides of the range are inclusive. The start node and end node may be the same node.
 * 
 * @author Yupeng
 * 
 */
public class NodeRange
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(NodeRange.class);
    
    private Node                m_start_node;
    
    private Node                m_end_node;
    
    /**
     * Constructs a node range.
     * 
     * @param start_node
     *            the start node.
     * @param end_node
     *            the end node.
     */
    public NodeRange(Node start_node, Node end_node)
    {
        assert (start_node != null);
        assert (end_node != null);
        
        // Both nodes must be within the same parent
        Node s_parent = start_node.getParentNode();
        Node e_parent = end_node.getParentNode();
        assert (s_parent == null && e_parent == null) || s_parent.equals(e_parent);
        
        m_start_node = start_node;
        m_end_node = end_node;
    }
    
    /**
     * Returns the start node.
     * 
     * @return the start node.
     */
    public Node getStartNode()
    {
        return m_start_node;
    }
    
    /**
     * Returns the end node.
     * 
     * @return the end node.
     */
    public Node getEndNode()
    {
        return m_end_node;
    }
    
    /**
     * Returns a list comprising all nodes between the start node and end node.
     * 
     * @return a list of all nodes between the start node and end node.
     */
    public List<Node> getAllNodes()
    {
        List<Node> nodes = new ArrayList<Node>();
        for (Node node = m_start_node; !node.equals(m_end_node); node = node.getNextSibling())
        {
            assert (node != null);
            nodes.add(node);
        }
        nodes.add(m_end_node);
        return nodes;
    }
}
