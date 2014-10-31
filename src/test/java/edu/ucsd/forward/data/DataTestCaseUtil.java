/**
 * 
 */
package edu.ucsd.forward.data;

import java.text.ParseException;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.value.DataTree;
import edu.ucsd.forward.data.value.Value;
import edu.ucsd.forward.exception.UncheckedException;

/**
 * Utility methods for writing test cases that manipulate data.
 * 
 * @author Kian Win Ong
 * 
 */
public final class DataTestCaseUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DataTestCaseUtil.class);
    
    /**
     * Private constructor.
     */
    private DataTestCaseUtil()
    {
        
    }
    
    /**
     * Finds the matching value in a data tree.
     * 
     * @param data_tree
     *            the data tree.
     * @param data_path_string
     *            the data path in string format.
     * @return the matching value if it exists; <code>null</code> otherwise.
     */
    public static Value find(DataTree data_tree, String data_path_string)
    {
        try
        {
            return new DataPath(data_path_string).find(data_tree);
        }
        catch (ParseException e)
        {
            throw new UncheckedException(e);
        }
    }
    
    /**
     * Finds the matching value in a data tree.
     * 
     * @param value
     *            the value to start matching from.
     * @param data_path_string
     *            the data path in string format.
     * @return the matching value if it exists; <code>null</code> otherwise.
     */
    public static Value find(Value value, String data_path_string)
    {
        try
        {
            return new DataPath(data_path_string).find(value);
        }
        catch (ParseException e)
        {
            throw new UncheckedException(e);
        }
    }
}
