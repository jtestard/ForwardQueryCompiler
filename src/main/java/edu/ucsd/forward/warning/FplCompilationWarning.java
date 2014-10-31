/**
 * 
 */
package edu.ucsd.forward.warning;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.exception.Location;
import edu.ucsd.forward.warning.WarningMessages.ActionCompilation;

/**
 * Represents a warning generated during action compilation.
 * 
 * @author Yupeng
 * 
 */
public class FplCompilationWarning extends Warning
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(FplCompilationWarning.class);
    
    /**
     * Constructs a warning with a given message, possibly parameterized, for a given location in an input file.
     * 
     * @param message
     *            the warning message
     * @param location
     *            the file location associated with this warning
     * @param params
     *            the parameters that the message is expecting
     */
    public FplCompilationWarning(ActionCompilation message, Location location, Object... params)
    {
        super(message, location, params);
    }
}
