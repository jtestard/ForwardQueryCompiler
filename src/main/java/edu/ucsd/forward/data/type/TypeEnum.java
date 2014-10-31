/**
 * 
 */
package edu.ucsd.forward.data.type;

import edu.ucsd.forward.exception.ExceptionMessages;

/**
 * The list of types supported by the data model.
 * 
 * @author Michalis Petropoulos
 * 
 */
public enum TypeEnum
{
    COLLECTION("collection", new CollectionType()),

    TUPLE("tuple", new TupleType()),

    SWITCH("switch", new SwitchType()),

    DECIMAL("decimal", new DecimalType()),

    DOUBLE("double", new DoubleType()),

    FLOAT("float", new FloatType()),

    LONG("long", new LongType()),

    INTEGER("integer", new IntegerType()),

    DATE("date", new DateType()),

    TIMESTAMP("timestamp", new TimestampType()),

    BOOLEAN("boolean", new BooleanType()),

    XHTML("xhtml", new XhtmlType()),

    STRING("string", new StringType()),

    NULL("unknown", new NullType()),

    JSON("json", new JsonType()),

    UNKNOWN("unknown", new UnknownType());
    
    private final String m_name;
    
    private final Type   m_type;
    
    /**
     * Constructor.
     * 
     * @param name
     *            the display name of the type.
     * @param type
     *            the type.
     */
    TypeEnum(String name, Type type)
    {
        m_name = name;
        m_type = type;
    }
    
    /**
     * Gets the name of a type.
     * 
     * @return the name of a type.
     */
    public String getName()
    {
        return m_name;
    }
    
    /**
     * Gets the type.
     * 
     * @return the type.
     */
    public Type get()
    {
        return m_type;
    }
    
    /**
     * Gets the type with the given name.
     * 
     * @param name
     *            the name of a type.
     * @return the type.
     * @throws TypeException
     *             if the type name cannot be resolved.
     */
    public static Type get(String name) throws TypeException
    {
        for (TypeEnum type : values())
            if (type.getName().toLowerCase().equals(name.toLowerCase())) return type.get();
        
        throw new TypeException(ExceptionMessages.Type.UNKNOWN_TYPE_NAME, name);
    }
    
    /**
     * Gets the type with the given class.
     * 
     * @param clazz
     *            the name of the class.
     * @return the type, or null.
     */
    public static Type get(Class<? extends Type> clazz)
    {
        for (TypeEnum type : values())
            if (type.get().getClass() == clazz) return type.get();
        
        throw new AssertionError();
    }
    
    /**
     * Gets the type class with the given name.
     * 
     * @param name
     *            the name of a type.
     * @return the type class.
     * @throws TypeException
     *             if the type name cannot be resolved.
     */
    public static Class<? extends Type> getClass(String name) throws TypeException
    {
        for (TypeEnum type : values())
            if (type.getName().toLowerCase().equals(name.toLowerCase())) return type.get().getClass();
        
        throw new TypeException(ExceptionMessages.Type.UNKNOWN_TYPE_NAME, name);
    }
    
    /**
     * Gets the name of the given type.
     * 
     * @param type
     *            the type.
     * @return the name of the type, or null.
     */
    public static String getName(Type type)
    {
        for (TypeEnum value : values())
            // FIXME Implement equals for Type class
            if (value.get().getClass().equals(type.getClass())) return value.getName();
        
        return null;
    }
    
    /**
     * Gets the name of the given type.
     * 
     * @param type
     *            the type.
     * @return the name of the type, or null.
     */
    public static String getName(Class<? extends Type> type)
    {
        for (TypeEnum value : values())
            // FIXME Implement equals for Type class
            if (value.get().getClass().equals(type)) return value.getName();
        
        return null;
    }
    
    /**
     * Gets the type enum entry with the given type class.
     * 
     * @param type_class
     *            the type class.
     * @return a type enum entry.
     */
    public static TypeEnum getEntry(Class<? extends Type> type_class)
    {
        for (TypeEnum entry : values())
            if (entry.get().getClass() == type_class) return entry;
        
        throw new AssertionError();
    }
    
    /**
     * Gets the type enum entry with the given type.
     * 
     * @param type
     *            the type.
     * @return a type enum entry.
     */
    public static TypeEnum getEntry(Type type)
    {
        for (TypeEnum entry : values())
            if (entry.get().getClass() == type.getClass()) return entry;
        
        throw new AssertionError();
    }
    
    /**
     * Gets the type enum entry with the given type name.
     * 
     * @param name
     *            the type name.
     * @return a type enum entry.
     * @throws TypeException
     *             if the type name cannot be resolved.
     */
    public static TypeEnum getEntry(String name) throws TypeException
    {
        for (TypeEnum entry : values())
            if (entry.getName().toLowerCase().equals(name.toLowerCase())) return entry;
        
        throw new TypeException(ExceptionMessages.Type.UNKNOWN_TYPE_NAME, name);
    }
    
    /**
     * Determines whether the input name is a type name.
     * 
     * @param name
     *            the name to test.
     * @return true, if the input name is a type name; otherwise, false.
     */
    public static boolean isTypeName(String name)
    {
        for (TypeEnum entry : values())
            if (entry.getName().toLowerCase().equals(name.toLowerCase())) return true;
        
        return false;
    }
    
}
