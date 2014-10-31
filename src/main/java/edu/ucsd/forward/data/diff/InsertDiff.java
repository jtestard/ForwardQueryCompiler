/**
 * 
 */
package edu.ucsd.forward.data.diff;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.ValueUtil;
import edu.ucsd.forward.data.value.Value;

/**
 * An insert diff.
 * 
 * @author Kian Win Ong
 * 
 */
public class InsertDiff extends AbstractDiff
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(InsertDiff.class);
    
    /**
     * Constructs the diff to obtain a target value. The value's context is used as the diff's context, whereas a clone of the value
     * (and its descendants) will be used as the diff's payload.
     * 
     * @param target_value
     *            the target value.
     */
    public InsertDiff(Value target_value)
    {
        this(new DataPath(target_value), target_value);
    }
    
    /**
     * Constructs an insert diff.
     * 
     * @param context
     *            the context
     * @param payload
     *            the payload
     */
    public InsertDiff(DataPath context, Value payload)
    {
        super(context, payload);
    }
    
    /**
     * 
     * <b>Inherited JavaDoc:</b><br> {@inheritDoc} <br>
     * <br>
     * <b>See original method below.</b> <br>
     * 
     * @see edu.ucsd.forward.data.new_diff.AbstractDiff#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("InsertDiff {");
        sb.append("context : ");
        sb.append(getContext());
        sb.append("\n");
        sb.append("payload : ");
        sb.append(getPayload());
        sb.append("}");
        return sb.toString();
    }
    
}
