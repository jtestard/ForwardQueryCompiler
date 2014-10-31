/**
 * 
 */
package edu.ucsd.forward.data.xml;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import com.thoughtworks.xstream.core.util.Base64Encoder;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * A Java object serializer that serializes an object to string and deserializes back.
 * 
 * @author Yupeng
 * 
 */
public final class ObjectSerializer
{
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(ObjectSerializer.class);
    
    /**
     * Hidden constructor.
     */
    private ObjectSerializer()
    {
        
    }
    
    /**
     * Read the object from Base64 string.
     * 
     * @param s
     *            the string to deserialize
     * @return the deserialized object.
     */
    public static Object deserialize(String s)
    {
        byte[] data = new Base64Encoder().decode(s);
        ObjectInputStream ois;
        try
        {
            ois = new ObjectInputStream(new ByteArrayInputStream(data));
            Object o = ois.readObject();
            ois.close();
            return o;
        }
        catch (IOException e)
        {
            assert false;
        }
        catch (ClassNotFoundException e)
        {
            assert false;
        }
        return null;
    }
    
    /**
     * Write the object to a Base64 string.
     * 
     * @param o
     *            the object to serialize
     * @return the serialized string.
     */
    public static String serialize(Serializable o)
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos;
        try
        {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(o);
            oos.close();
        }
        catch (IOException e)
        {
            assert false;
        }
        
        return new String(new Base64Encoder().encode(baos.toByteArray()));
    }
}
