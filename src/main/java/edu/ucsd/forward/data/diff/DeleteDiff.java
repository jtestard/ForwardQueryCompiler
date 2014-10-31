/**
 * 
 */
package edu.ucsd.forward.data.diff;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.DataPath;
import edu.ucsd.forward.data.value.Value;

/**
 * A delete diff.
 * 
 * @author Kian Win Ong
 * 
 */
public class DeleteDiff extends AbstractDiff
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(DeleteDiff.class);
    
    /**
     * Constructs the diff from a source value. The value's context is used as the diff's context.
     * 
     * @param source_value
     *            the source value.
     */
    public DeleteDiff(Value source_value)
    {
        this(new DataPath(source_value));
    }
    
    /**
     * Constructs a delete diff.
     * 
     * @param context
     *            the context
     */
    public DeleteDiff(DataPath context)
    {
        super(context, null);
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
        sb.append("DeleteDiff {");
        sb.append("context : ");
        sb.append(getContext());
        sb.append("}");
        return sb.toString();
    }
    
}
