/**
 * 
 */
package edu.ucsd.forward.fpl.plan;

import java.util.HashSet;
import java.util.Set;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * The abstract implementation of instruction.
 * 
 * @author Yupeng
 * 
 */
public abstract class AbstractInstruction implements Instruction
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(AbstractInstruction.class);
    
    private Set<String>         m_labels;
    
    /**
     * Default constructor.
     */
    protected AbstractInstruction()
    {
        m_labels = new HashSet<String>();
    }
    
    @Override
    public String getName()
    {
        // return this.getClass().getSimpleName();
        String class_name = this.getClass().getName();
        
        return class_name.substring(class_name.lastIndexOf('.') + 1, class_name.length());
    }
    
    @Override
    public void addLabel(String label)
    {
        assert label != null;
        m_labels.add(label);
    }
    
    @Override
    public Set<String> getLabels()
    {
        return new HashSet<String>(m_labels);
    }
    
    @Override
    public String toString()
    {
        return toExplainString();
    }
    
    @Override
    public String toExplainString()
    {
        String str = "";
        boolean first = true;
        for (String label : m_labels)
        {
            if (first) first = false;
            else str += ",";
            str += label;
        }
        str += ":";
        return str;
    }
}
