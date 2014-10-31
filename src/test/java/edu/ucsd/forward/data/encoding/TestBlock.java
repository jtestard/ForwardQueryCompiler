/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.io.IOException;

import org.testng.annotations.Test;

import edu.ucsd.app2you.util.logger.Logger;
import edu.ucsd.forward.test.AbstractTestCase;

/**
 * @author Aditya Avinash
 * 
 */
@Test
public class TestBlock extends AbstractTestCase
{
    @SuppressWarnings("unused")
    private static final Logger log                   = Logger.getLogger(TestBlock.class);
    
    private static final String TEST_STRING_OF_LEN_10 = "1234567890";
    
    private static final int    TEST_INT              = 132321;
    
    private static final long   TEST_LONG             = 1232112321;
    
    private static final float  TEST_FLOAT            = 1323211323.0f;
    
    private static final double TEST_DOUBLE           = 132321132312.0;
    
    private static final int    TEST_INDEX            = 1500;
    
    /**
     * It tests following scenarios: 1. Checks if signed and unsigned ints are indeed compressed 2. Tests the encoding and decoding
     * of positive integers. 3. Tests the encoding and decoding of negative integers.
     * 
     * For signed int upto 63 takes 1 byte, for unsigned int upto 127 takes 1 byte
     * 
     * @throws IOException
     */
    public void testVarint32() throws IOException
    {
        Block b = new Block();
        
        // test unsigned ints
        int value = (int) Math.pow(2, 32) - 1; // max int val
        b.putUnInt32(value);
        assertTrue(b.getUnInt32() == value);
        b.putUnInt32(TEST_INDEX, value);
        assertTrue(b.getUnInt32(TEST_INDEX) == value);
        
        // test number of bytes used to store
        int prev_position = b.getWritePosition();
        value = (int) Math.pow(2, 7) - 1;
        b.putUnInt32(value);
        assertTrue(b.getUnInt32() == value);
        b.putUnInt32(TEST_INDEX, value);
        assertTrue(b.getUnInt32(TEST_INDEX) == value);
        int current_position = b.getWritePosition();
        int bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 1);
        
        prev_position = current_position;
        
        value = (int) Math.pow(2, 14) - 1;
        b.putUnInt32(value);
        assertTrue(b.getUnInt32() == value);
        b.putUnInt32(TEST_INDEX, value);
        assertTrue(b.getUnInt32(TEST_INDEX) == value);
        current_position = b.getWritePosition();
        bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 2);
        
        // Test positive signed integer
        prev_position = current_position;
        
        value = (int) Math.pow(2, 6) - 1;
        b.putInt32(value);
        assertTrue(b.getInt32() == value);
        b.putInt32(TEST_INDEX, value);
        assertTrue(b.getInt32(TEST_INDEX) == value);
        current_position = b.getWritePosition();
        bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 1);
        
        // Test negative integer
        prev_position = current_position;
        value = -((int) Math.pow(2, 6) - 1);
        b.putInt32(value);
        assertTrue(b.getInt32() == value);
        b.putInt32(TEST_INDEX, value);
        assertTrue(b.getInt32(TEST_INDEX) == value);
        current_position = b.getWritePosition();
        bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 1);
        
    }
    
    
    
    public void testVarint64() throws IOException
    {
        Block b = new Block();
        
        // test unsigned long
        long value = (long) Math.pow(2, 64) - 1; // max long val
        b.putUnInt64(value);
        assertTrue(b.getUnInt64() == value);
        b.putUnInt64(TEST_INDEX, value);
        assertTrue(b.getUnInt64(TEST_INDEX) == value);
        
        // test number of bytes used to store
        int prev_position = b.getWritePosition();
        value = (long) Math.pow(2, 7) - 1;
        b.putUnInt64(value);
        assertTrue(b.getUnInt64() == value);
        b.putUnInt64(TEST_INDEX, value);
        assertTrue(b.getUnInt64(TEST_INDEX) == value);
        int current_position = b.getWritePosition();
        int bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 1);
        
        prev_position = current_position;
        
        value = (long) Math.pow(2, 14) - 1;
        b.putUnInt64(value);
        assertTrue(b.getUnInt64() == value);
        b.putUnInt64(TEST_INDEX, value);
        assertTrue(b.getUnInt64(TEST_INDEX) == value);
        current_position = b.getWritePosition();
        bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 2);
        
        // Test positive signed long
        prev_position = current_position;
        
        value = (long) Math.pow(2, 6) - 1;
        b.putInt64(value);
        assertTrue(b.getInt64() == value);
        b.putInt64(TEST_INDEX, value);
        assertTrue(b.getInt64(TEST_INDEX) == value);
        current_position = b.getWritePosition();
        bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 1);
        
        // Test negative integer
        prev_position = current_position;
        value = -((long) Math.pow(2, 6) - 1);
        b.putInt64(value);
        assertTrue(b.getInt32() == value);
        b.putInt64(TEST_INDEX, value);
        assertTrue(b.getInt64(TEST_INDEX) == value);
        current_position = b.getWritePosition();
        bytes_used = current_position - prev_position;
        assertTrue(bytes_used == 1);
    }
    
    public void testStringEncodingAndDecoding() throws IOException
    {
        Block b = new Block();
        int prev_position = b.getWritePosition();
        b.putString(TEST_STRING_OF_LEN_10);
        int current_position = b.getWritePosition();
        int total_bytes = current_position - prev_position - 1; // one byte was used to store length of string
        assertTrue("Incorrect number of bytes written for the string", TEST_STRING_OF_LEN_10.length() == total_bytes);
        assertTrue("Decoded String was incorrect", TEST_STRING_OF_LEN_10.equals(b.getString()));
        
        b = new Block();
        prev_position = b.getWritePosition();
        b.putString(TEST_INDEX,TEST_STRING_OF_LEN_10);
        current_position = b.getWritePosition();
        assertTrue("Absolute String method unexpectedly modified w_write_position", prev_position==current_position);
        
        prev_position = b.getReadPosition();
        String decodedString = b.getString(TEST_INDEX);
        current_position = b.getReadPosition();
        assertTrue("Absolute String method unexpectedly modified w_read_position", prev_position==current_position);
        assertTrue("Decoded String was incorrect", TEST_STRING_OF_LEN_10.equals(decodedString));
    }
    
    public void testBooleanEncodingAndDecoding() throws IOException
    {
        Block b = new Block();
        int prev_position = b.getWritePosition();
        boolean value = true;
        b.putBoolean(value);
        int current_position = b.getWritePosition();
        int total_bytes = current_position - prev_position;
        assertTrue("Boolean was not encoded in 1 byte", total_bytes == 1);
        assertTrue("Decoded Boolean value was incorrect", value == b.getBoolean());
        value=false;
        b.putBoolean(value);
        assertTrue("Decoded Boolean value was incorrect", value == b.getBoolean());
        
        b = new Block();
        prev_position = b.getWritePosition();
        value = true;
        b.putBoolean(100, value);
        current_position = b.getWritePosition();
        total_bytes = current_position - prev_position;
        assertTrue("Absolute method should not change the position variables", total_bytes == 0);
        assertTrue("Decoded Boolean value was incorrect", value == b.getBoolean(100));
        value=false;
        b.putBoolean(200,value);
        assertTrue("Decoded Boolean value was incorrect", value == b.getBoolean(200));
    }
    
    public void testFixedSizeIntEncodingAndDecoding() throws IOException
    {
        Block b = new Block();
        int prev_position = b.getWritePosition();
        b.putFixedSizeInt(TEST_INT);
        int current_position = b.getWritePosition();
        long val = b.getFixedSizeInt();
        int total_bytes = current_position - prev_position; 
        assertTrue("Incorrect number of bytes written for fixed size int",  total_bytes == 4);
        assertTrue("Decoded int was incorrect", val == TEST_INT);
        
        b = new Block();
        prev_position = b.getWritePosition();
        b.putFixedSizeInt(TEST_INDEX, TEST_INT);
        current_position = b.getWritePosition();
        assertTrue("Absolute method unexpectedly modified w_write_position", prev_position==current_position);
        
        prev_position = b.getReadPosition();
        int decodedInt = b.getFixedSizeInt(TEST_INDEX);
        current_position = b.getReadPosition();
        assertTrue("Absolute method unexpectedly modified w_read_position", prev_position==current_position);
        assertTrue("Decoded int  was incorrect", decodedInt==TEST_INT);
    }
    
    public void testFixedSizeLongEncodingAndDecoding() throws IOException
    {
        Block b = new Block();
        int prev_position = b.getWritePosition();
        b.putFixedSizeLong(TEST_LONG);
        int current_position = b.getWritePosition();
        long val = b.getFixedSizeLong();
        int total_bytes = current_position - prev_position; 
        assertTrue("Incorrect number of bytes written for fixed size long",  total_bytes == 8);
        assertTrue("Decoded long was incorrect", val == TEST_LONG);
    
    
        b = new Block();
        prev_position = b.getWritePosition();
        b.putFixedSizeLong(TEST_INDEX, TEST_LONG);
        current_position = b.getWritePosition();
        assertTrue("Absolute method unexpectedly modified w_write_position", prev_position==current_position);
        
        prev_position = b.getReadPosition();
        long decodedLong = b.getFixedSizeLong(TEST_INDEX);
        current_position = b.getReadPosition();
        assertTrue("Absolute method unexpectedly modified w_read_position", prev_position==current_position);
        assertTrue("Decoded long  was incorrect", decodedLong==TEST_LONG);
    }
    
    public void testFloatEncodingAndDecoding() throws IOException
    {
        Block b = new Block();
        int prev_position = b.getWritePosition();
        b.putFloat(TEST_FLOAT);
        int current_position = b.getWritePosition();
        float val = b.getFloat();
        int total_bytes = current_position - prev_position; 
        assertTrue("Incorrect number of bytes written for float",  total_bytes == 4);
        assertTrue("Decoded float was incorrect", val == TEST_FLOAT);
        
        b = new Block();
        prev_position = b.getWritePosition();
        b.putFloat(TEST_INDEX, TEST_FLOAT);
        current_position = b.getWritePosition();
        assertTrue("Absolute method unexpectedly modified w_write_position", prev_position==current_position);
        
        prev_position = b.getReadPosition();
        float decodedFloat = b.getFloat(TEST_INDEX);
        current_position = b.getReadPosition();
        assertTrue("Absolute method unexpectedly modified w_read_position", prev_position==current_position);
        assertTrue("Decoded float  was incorrect", decodedFloat==TEST_FLOAT);
    }
    
    public void testDoubleEncodingAndDecoding() throws IOException
    {
        Block b = new Block();
        int prev_position = b.getWritePosition();
        b.putDouble(TEST_DOUBLE);
        int current_position = b.getWritePosition();
        double val = b.getDouble();
        int total_bytes = current_position - prev_position; 
        assertTrue("Incorrect number of bytes written for double",  total_bytes == 8);
        assertTrue("Decoded double was incorrect", val == TEST_DOUBLE);
        
        b = new Block();
        prev_position = b.getWritePosition();
        b.putDouble(TEST_INDEX, TEST_DOUBLE);
        current_position = b.getWritePosition();
        assertTrue("Absolute method unexpectedly modified w_write_position", prev_position==current_position);
        
        prev_position = b.getReadPosition();
        double decodedDouble = b.getDouble(TEST_INDEX);
        current_position = b.getReadPosition();
        assertTrue("Absolute method unexpectedly modified w_read_position", prev_position==current_position);
        assertTrue("Decoded double  was incorrect", decodedDouble==TEST_DOUBLE);
    } 
}
