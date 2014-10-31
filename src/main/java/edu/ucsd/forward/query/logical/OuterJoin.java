package edu.ucsd.forward.query.logical;

import edu.ucsd.forward.query.logical.term.Term;
import edu.ucsd.forward.query.logical.visitors.OperatorVisitor;

/**
 * Represents the outer join logical operator.
 * 
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class OuterJoin extends InnerJoin
{
    /**
     * An enumeration of the outer join variations.
     */
    public enum Variation
    {
        LEFT, RIGHT, FULL;
    }
    
    /**
     * Outer join variation.
     */
    private Variation m_variation;
    
    /**
     * Initializes an instance of the operator.
     * 
     * @param variation
     *            the outer join variation.
     */
    public OuterJoin(Variation variation)
    {
        super();
        
        assert (variation != null);
        m_variation = variation;
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private OuterJoin()
    {
        
    }
    
    /**
     * Returns the outer join variation.
     * 
     * @return a outer join variation.
     */
    public Variation getOuterJoinVariation()
    {
        return m_variation;
    }
    
    @Override
    public String toExplainString()
    {
        String str = super.toExplainString() + " ";
        str += this.getOuterJoinVariation().name().toLowerCase();
        
        return str;
    }
    
    @Override
    public Operator accept(OperatorVisitor visitor)
    {
        return visitor.visitOuterJoin(this);
    }
    
    @Override
    public Operator copy()
    {
        OuterJoin copy = new OuterJoin(m_variation);
        super.copy(copy);
        
        for (Term term : this.getConditions())
            copy.addCondition(term.copy());
        
        return copy;
    }
    
    @Override
    public OuterJoin copyWithoutType()
    {
        OuterJoin copy = new OuterJoin(m_variation);
        
        for (Term term : this.getConditions())
            copy.addCondition(term.copyWithoutType());
        
        return copy;
    }
}
