/**
 * 
 */
package edu.ucsd.forward.xml;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import com.google.gwt.xml.client.Node;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * Helper class for tracking node positions in a document.
 * 
 * @author Yupeng
 * 
 */
public class NodePositionCache
{
    @SuppressWarnings("unused")
    private static final Logger                   log = Logger.getLogger(NodePositionCache.class);
    
    /**
     * Map from parent node, to a map from child nodes to their indices
     */
    private HashMap<Node, HashMap<Node, Integer>> m_map;
    
    /**
     * Constructor.
     */
    public NodePositionCache()
    {
        m_map = new LinkedHashMap<Node, HashMap<Node, Integer>>();
    }
    
    /**
     * Adds all the nodes in the given tree.
     * 
     * @param root
     *            root
     */
    public void addAll(Node root)
    {
        addParentNode(root);
        List<Node> nodes = XmlUtil.getChildNodes(root);
        for (int i = 0; i < nodes.size(); i++)
        {
            Node node = nodes.get(i);
            
            addAll(node);
        }
    }
    
    /**
     * Adds a parent node to the cache, building an index from the child nodes to their indices.
     * 
     * @param parent_node
     *            parent node
     * 
     */
    public void addParentNode(Node parent_node)
    {
        HashMap<Node, Integer> map = new LinkedHashMap<Node, Integer>();
        List<Node> nodes = XmlUtil.getChildNodes(parent_node);
        for (int i = 0; i < nodes.size(); i++)
        {
            Node node = nodes.get(i);
            map.put(node, i);
        }
        m_map.put(parent_node, map);
    }
    
    /**
     * Invalidates any previous entry for the given parent node.
     * 
     * @param parent_node
     *            parent node
     */
    public void invalidateParentNode(Node parent_node)
    {
        if (!m_map.containsKey(parent_node))
        {
            return;
        }
        m_map.remove(parent_node);
    }
    
    /**
     * Invalidates the node and descendants.
     * 
     * @param root
     *            root
     */
    public void invalidateAll(Node root)
    {
        invalidateParentNode(root);
        List<Node> nodes = XmlUtil.getChildNodes(root);
        for (int i = 0; i < nodes.size(); i++)
        {
            Node node = nodes.get(i);
            invalidateAll(node);
        }
    }
    
    /**
     * Returns the index of the node in the list of child nodes of its parent. Returns 0 if the node does not have a parent. Throws
     * an exception if the index for the parent has not being built.
     * 
     * @param node
     *            node
     * @return index
     */
    public int getPosition(Node node)
    {
        Node parent = node.getParentNode();
        if (parent == null)
        {
            return 0;
        }
        if (!m_map.containsKey(parent))
        {
            throw new UnsupportedOperationException();
        }
        HashMap<Node, Integer> map = m_map.get(parent);
        assert map != null;
        return map.get(node);
    }
}
