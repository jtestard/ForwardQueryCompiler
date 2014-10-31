/**
 * 
 */
package edu.ucsd.forward.query.function;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import edu.ucsd.forward.data.type.NullType;
import edu.ucsd.forward.data.type.Type;
import edu.ucsd.forward.data.type.TypeEnum;
import edu.ucsd.forward.query.explain.ExplanationPrinter;

/**
 * A function signature.
 * 
 * @author Michalis Petropoulos
 */
@SuppressWarnings("serial")
public class FunctionSignature implements ExplanationPrinter, Serializable
{
    /**
     * The name of the function signature.
     */
    private String         m_name;
    
    /**
     * The function signature arguments.
     */
    private List<Argument> m_arguments;
    
    /**
     * An enumeration of argument occurrence constraints.
     */
    public enum Occurrence
    {
        ONE, MULTIPLE;
    }
    
    /**
     * The return type of the function.
     */
    private Type m_return_type;
    
    /**
     * Constructor.
     * 
     * @param name
     *            The name of the function.
     * @param return_type
     *            the return type of the function.
     * @param arguments
     *            the arguments of the function.
     * 
     */
    public FunctionSignature(String name, Type return_type, Argument... arguments)
    {
        assert (name != null && !name.isEmpty());
        
        m_name = name;
        m_arguments = (arguments.length == 0) ? new ArrayList<Argument>() : Arrays.asList(arguments);
        m_return_type = return_type;
    }
    
    /**
     * Constructions a function signature with no return type.
     * 
     * @param name
     *            The name of the function.
     * @param arguments
     *            the arguments of the function.
     * 
     */
    public FunctionSignature(String name, Argument... arguments)
    {
        this(name, new NullType(), arguments);
    }
    
    @SuppressWarnings("unused")
    private FunctionSignature()
    {
        
    }
    
    /**
     * Returns the name of the function signature.
     * 
     * @return the name of the function signature.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Returns the arguments of the function signature.
     * 
     * @return the arguments of the function signature.
     */
    public List<Argument> getArguments()
    {
        return Collections.unmodifiableList(m_arguments);
    }
    
    /**
     * Adds an argument to the function signature.
     * 
     * @param name
     *            the name of the argument.
     * @param type
     *            the type of the argument.
     */
    public void addArgument(String name, Type type)
    {
        m_arguments.add(new Argument(name, type, Occurrence.ONE));
    }
    
    /**
     * Adds an argument to the function signature.
     * 
     * @param name
     *            the name of the argument.
     * @param type
     *            the type of the argument.
     * @param occurrence
     *            the occurrence constraint of the argument.
     */
    public void addArgument(String name, Type type, Occurrence occurrence)
    {
        m_arguments.add(new Argument(name, type, occurrence));
    }
    
    /**
     * Returns the number of arguments in the function signature.
     * 
     * @return the number of arguments.
     */
    public int size()
    {
        return m_arguments.size();
    }
    
    /**
     * Returns the return type of the function signature.
     * 
     * @return the return type of the function signature.
     */
    public Type getReturnType()
    {
        return m_return_type;
    }
    
    @Override
    public String toExplainString()
    {
        String str = m_name + "(";
        
        for (Argument arg : m_arguments)
        {
            str += TypeEnum.getName(arg.getType()) + " " + arg.getName() + ", ";
        }
        if (!m_arguments.isEmpty()) str = str.substring(0, str.length() - 2);
        
        str += ")";
        
        if (m_return_type != null) str = TypeEnum.getName(m_return_type) + " " + str;
        
        return str;
    }
    
    @Override
    public String toString()
    {
        return this.toExplainString();
    }
    
    /**
     * A function argument.
     */
    public static class Argument implements Serializable
    {
        /**
         * The name of the argument.
         */
        private String     m_name;
        
        /**
         * The type of the argument.
         */
        private Type       m_type;
        
        /**
         * The occurrence constraint.
         */
        private Occurrence m_occurrence;
        
        /**
         * Constructor.
         * 
         * @param name
         *            the name of the argument.
         * @param type
         *            the type of the argument.
         * @param occurrence
         *            the occurrence constraint.
         */
        public Argument(String name, Type type, Occurrence occurrence)
        {
            assert (name != null);
            assert (type != null);
            assert (occurrence != null);
            
            m_name = name;
            m_type = type;
            m_occurrence = occurrence;
        }
        
        @SuppressWarnings("unused")
        private Argument()
        {
            
        }
        
        /**
         * Returns the name of the argument.
         * 
         * @return the name of the argument.
         */
        public String getName()
        {
            return m_name;
        }
        
        /**
         * Returns the type of the argument.
         * 
         * @return the type of the argument.
         */
        public Type getType()
        {
            return m_type;
        }
        
        /**
         * Returns the occurrence constraint of the argument.
         * 
         * @return the occurrence constraint of the argument.
         */
        public Occurrence getOccurrence()
        {
            return m_occurrence;
        }
    }
    
}
