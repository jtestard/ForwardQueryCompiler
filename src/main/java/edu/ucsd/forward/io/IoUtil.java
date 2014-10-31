/**
 * 
 */
package edu.ucsd.forward.io;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * @author Yupeng
 * 
 */
public class IoUtil
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(IoUtil.class);
    
    private IoUtil()
    {
        
    }
    
    /**
     * Returns the contents of a resource file on the classpath. An error occurs if the resource file is not found.
     * 
     * @param clazz
     *            the class for which we use the corresponding class loader.
     * @param relative_file_name
     *            the file name, which will be treated as relative to the package name of the class.
     * @return the contents.
     */
    public static String getResourceAsString(Class<?> clazz, String relative_file_name)
    {
        return edu.ucsd.app2you.util.IoUtil.getResourceAsString(clazz, relative_file_name);
    }
}
