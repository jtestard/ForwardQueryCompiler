/**
 * 
 */
package edu.ucsd.forward.query.logical.term;

import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.source.DataSourceMetaData;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.exception.ExceptionMessages.QueryCompilation;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.logical.Operator;

/**
 * Implementation of the variable interface for relative variables, that is, variables that do not access a data sources.
 * 
 * @author Michalis Petropoulos
 * 
 */
@SuppressWarnings("serial")
public class RelativeVariable extends AbstractVariable
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(RelativeVariable.class);
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the variable.
     */
    public RelativeVariable(String name)
    {
        super(name, name);
    }
    
    /**
     * Default constructor.
     */
    @SuppressWarnings("unused")
    private RelativeVariable()
    {
        
    }
    
    /**
     * Constructor.
     * 
     * @param name
     *            the name of the variable.
     * @param default_alias
     *            the default alias of the variable.
     */
    public RelativeVariable(String name, String default_alias)
    {
        super(name, default_alias);
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        if (this.getType() != null) return this.getType();
        
        for (Operator child : operators)
        {
            Variable in_var = null;
            if (child.getOutputInfo().getVariables().contains(this))
            {
                in_var = child.getOutputInfo().getVariable(this.getName());  
                this.setType(in_var.getType());
                this.setDefaultProjectAlias(in_var.getDefaultProjectAlias());
                
                return this.getType();
            }
            else if(child.getOutputInfo().getProvenanceVariables().contains(this))
            {
                in_var = child.getOutputInfo().getProvenanceVariable(this.getName());   
                this.setType(in_var.getType());
                this.setDefaultProjectAlias(in_var.getDefaultProjectAlias());
                
                return this.getType();
            }

        }
        
        throw new QueryCompilationException(QueryCompilation.UNKNOWN_ATTRIBUTE, this.getLocation(), this.getName());
    }
    
    @Override
    public boolean isDataSourceCompliant(DataSourceMetaData metadata)
    {
        return true;
    }
    
    @Override
    public Term copy()
    {
        RelativeVariable copy = new RelativeVariable(this.getName(), this.getDefaultProjectAlias());
        super.copy(copy);
        
        return copy;
    }
    
    /**
     * <b>Inherited JavaDoc:</b><br>
     * {@inheritDoc}
     * <br><br>
     * <b>See original method below.</b>
     * <br>
     * @see edu.ucsd.forward.query.logical.term.AbstractTerm#copyWithoutType()
     */
    @Override
    public RelativeVariable copyWithoutType()
    {
        return new RelativeVariable(getName());
    }
}
