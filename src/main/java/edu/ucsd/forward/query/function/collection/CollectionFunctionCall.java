package edu.ucsd.forward.query.function.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.data.TypeUtil;
import edu.ucsd.forward.data.type.CollectionType;
import edu.ucsd.forward.data.type.TupleType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.query.QueryCompilationException;
import edu.ucsd.forward.query.function.AbstractFunctionCall;
import edu.ucsd.forward.query.function.FunctionRegistry;
import edu.ucsd.forward.query.function.FunctionRegistryException;
import edu.ucsd.forward.query.logical.Operator;
import edu.ucsd.forward.query.logical.term.Term;

/**
 * A collection function call, which has a collection function definition and a list of arguments. The arguments are terms, which
 * have to be function calls.
 * 
 * @author Romain Vernoux
 */
@SuppressWarnings("serial")
public class CollectionFunctionCall extends AbstractFunctionCall<CollectionFunction>
{
    @SuppressWarnings("unused")
    private static final Logger log       = Logger.getLogger(CollectionFunctionCall.class);
    
    private boolean             m_ordered = false;
    
    /**
     * Constructs the collection function call for a given list of arguments. The constructor copies the arguments into an internal
     * list.
     * 
     * @param arguments
     *            the function arguments.
     */
    public CollectionFunctionCall(List<Term> arguments)
    {
        super();
        
        assert (arguments != null);
        
        try
        {
            this.setFunction((CollectionFunction) FunctionRegistry.getInstance().getFunction(CollectionFunction.NAME));
        }
        catch (FunctionRegistryException e)
        {
            // This should never happen
            assert (false);
        }
        
        for (Term arg : arguments)
            this.addArgument(arg);
    }
    
    /**
     * Constructs the collection function call for a given variable number of arguments. The constructor copies the arguments into
     * an internal list. This constructor is mainly used in test cases.
     * 
     * @param arguments
     *            the function arguments.
     */
    public CollectionFunctionCall(Term... arguments)
    {
        this(Arrays.asList(arguments));
    }
    
    public CollectionFunctionCall()
    {
        this(new ArrayList<Term>());
    }
    
    /**
     * Gets the flag telling if this function corresponds to a BAG call or a LIST call.
     * 
     * @return <code>true</code> if this node corresponds to a LIST call, <code>false</code> otherwise.
     */
    public boolean isOrdered()
    {
        return m_ordered;
    }
    
    /**
     * Sets the flag telling if this function corresponds to a BAG call or a LIST call.
     * 
     * @param flag
     *            should be if this node corresponds to a LIST call, <code>false</code> otherwise.
     */
    public void setOrdered(boolean flag)
    {
        m_ordered = flag;
    }
    
    @Override
    public Type inferType(List<Operator> operators) throws QueryCompilationException
    {
        CollectionType coll_type = new CollectionType();
        
        Iterator<Term> iter_args = this.getArguments().iterator();
        
        Type t = null;
        
        while (iter_args.hasNext())
        {
            // check that all the arguments have the same (tuple) type
            Term arg = iter_args.next();
            Type type = arg.inferType(operators);
            type = TypeUtil.cloneNoParent(type);
            
            if (t == null)
            {
                t = type;
            }
            else
            {
                assert (TypeUtil.deepEqualsByIsomorphism(t, type));
            }
        }
        
        if (t == null)
        {
            // FIXME put Any here
            coll_type.setChildrenType(new TupleType());
        }
        else
        {
            coll_type.setChildrenType(t);
        }
        
        coll_type.setOrdered(m_ordered);
        
        this.setType(coll_type);
        
        return this.getType();
    }
    
    @Override
    public String toExplainString()
    {
        String str = "";
        
        str += CollectionFunction.NAME;
        str += " (";
        
        for (Term argument : this.getArguments())
            str += argument.toExplainString() + ", ";
        if (!this.getArguments().isEmpty()) str = str.substring(0, str.length() - 2);
        str += ")";
        
        return str;
    }
    
    @Override
    public Term copy()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copy());
        }
        CollectionFunctionCall copy = new CollectionFunctionCall(copies);
        super.copy(copy);
        
        copy.setOrdered(m_ordered);
        
        return copy;
    }
    
    @Override
    public Term copyWithoutType()
    {
        List<Term> copies = new ArrayList<Term>();
        for (Term arg : this.getArguments())
        {
            copies.add(arg.copyWithoutType());
        }
        CollectionFunctionCall copy = new CollectionFunctionCall(copies);
        
        return copy;
    }
}
