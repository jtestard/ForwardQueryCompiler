/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.util.HashMap;
import java.util.Map;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * @author AdityaAvinash
 * 
 */
public class BlockDirectory
{
    @SuppressWarnings("unused")
    private static final Logger              log               = Logger.getLogger(BlockDirectory.class);
    
    private static final Map<Integer, Block> m_block_directory = new HashMap<Integer, Block>();
    
    private static int                       m_block_id        = 0;
    
    /**
     * It return a value which is unique for a BlockDirectory
     */
    public static int getUniqueBlockID()
    {
        return m_block_id++;
    }
    
    public static void putBlock(Integer block_id, Block block)
    {
        m_block_directory.put(block_id, block);
    }
    
    public static Block getBlock(Integer block_id)
    {
        return m_block_directory.get(block_id);
    }
}
