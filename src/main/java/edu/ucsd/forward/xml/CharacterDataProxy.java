/**
 * 
 */
package edu.ucsd.forward.xml;

/**
 * A wrapper of GWT's DOM character data to work on the server side.
 * 
 * @author Yupeng
 * 
 */
public class CharacterDataProxy extends NodeProxy implements com.google.gwt.xml.client.CharacterData
{
    
    @Override
    public void appendData(String appended_data)
    {
        ((org.w3c.dom.CharacterData) n).appendData(appended_data);
    }
    
    @Override
    public void deleteData(int offset, int count)
    {
        ((org.w3c.dom.CharacterData) n).deleteData(offset, count);
    }
    
    @Override
    public String getData()
    {
        return ((org.w3c.dom.CharacterData) n).getData();
    }
    
    @Override
    public int getLength()
    {
        return ((org.w3c.dom.CharacterData) n).getLength();
    }
    
    @Override
    public void insertData(int offset, String inserted_data)
    {
        ((org.w3c.dom.CharacterData) n).insertData(offset, inserted_data);
    }
    
    @Override
    public void replaceData(int offset, int count, String replacement_data)
    {
        ((org.w3c.dom.CharacterData) n).replaceData(offset, count, replacement_data);
    }
    
    @Override
    public void setData(String data)
    {
        ((org.w3c.dom.CharacterData) n).setData(data);
    }
    
    @Override
    public String substringData(int offset, int count)
    {
        return ((org.w3c.dom.CharacterData) n).substringData(offset, count);
    }
    
}
