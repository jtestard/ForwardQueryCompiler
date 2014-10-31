/**
 * 
 */
package edu.ucsd.forward.data.encoding;

import java.io.IOException;
import java.nio.ByteBuffer;

import edu.ucsd.app2you.util.logger.Logger;

/**
 * @author Aditya Avinash
 * 
 *         This class is responsible for the storage of encoded byte stream on the heap.
 * 
 *         1. Relative methods guarantee that the instance variables m_read_position and m_write_position do not change when these
 *         methods are called. 2. Absolute methods will instance variables modify m_read_position or m_write_position. 3. The
 *         implementation does not guarantee any deterministic value of "position" variable of ByteBuffer
 * 
 *         The varint and string encoding/decoding implementations have been adapted from Google protocol buffer library. TODO: Add
 *         Google copyright details if it is needed.
 * 
 */
public class Block
{
    @SuppressWarnings("unused")
    private static final Logger log              = Logger.getLogger(Block.class);
    
    public static final int     BLOCK_SIZE       = 32768;
    
    protected int                 m_write_position = 0;
    
    protected int                 m_read_position  = 0;
    
    protected ByteBuffer        m_byte_buffer    = ByteBuffer.allocate(BLOCK_SIZE);
    
    protected final boolean       RELATIVE_ACCESS  = true;
    
    protected final boolean       ABSOLUTE_ACCESS  = false;
    
    private final int           m_block_id;
    
    /**
     * Whenever a Block is created, it gets a unique block id and it gets registered in the block directory.
     */
    public Block()
    {
        m_block_id = BlockDirectory.getUniqueBlockID();
        BlockDirectory.putBlock(m_block_id, this);
    }
    
    public int getBlockID()
    {
        return m_block_id;
    }
    
    protected ByteBuffer getByteBuffer()
    {
        return m_byte_buffer;
    }
    
    protected boolean isFull(int index)
    {
        return index >= BLOCK_SIZE;
    }
    
    public int bytesAvailable()
    {
        return BLOCK_SIZE - m_write_position;
    }
    
    public int getWritePosition()
    {
        return m_write_position;
    }
    
    public int getReadPosition()
    {
        return m_read_position;
    }
    
    public void setReadPosition(int value)
    {
        m_read_position = value;
    }
    
    /*
     * =============================================================================================================================
     * Relative get methods : These methods modify the instance variable m_read_position
     * =============================================================================================================================
     */
    
    public byte getByte() throws IOException
    {
        return readRawByte(m_read_position, RELATIVE_ACCESS);
    }
    
    public boolean getBoolean() throws IOException
    {
        int val = readRawVarint32(m_read_position, RELATIVE_ACCESS);
        return val == 1 ? true : false;
    }
    
    public int getInt32() throws IOException
    {
        return readSInt32(m_read_position, RELATIVE_ACCESS);
    }
    
    public int getUnInt32() throws IOException
    {
        return readRawVarint32(m_read_position, RELATIVE_ACCESS);
    }
    
    public long getInt64() throws IOException
    {
        return readSInt64(m_read_position, RELATIVE_ACCESS);
    }
    
    public long getUnInt64() throws IOException
    {
        return readRawVarint64(m_read_position, RELATIVE_ACCESS);
    }
    
    public int getFixedSizeInt() throws IOException
    {
        m_byte_buffer.position(m_read_position);
        int val = m_byte_buffer.getInt();
        m_read_position = m_byte_buffer.position();
        return val;
    }
    
    public long getFixedSizeLong() throws IOException
    {
        m_byte_buffer.position(m_read_position);
        long val = m_byte_buffer.getLong();
        m_read_position = m_byte_buffer.position();
        return val;
    }
    
    public float getFloat() throws IOException
    {
        m_byte_buffer.position(m_read_position);
        float val = m_byte_buffer.getFloat();
        m_read_position = m_byte_buffer.position();
        return val;
    }
    
    public double getDouble() throws IOException
    {
        m_byte_buffer.position(m_read_position);
        double val = m_byte_buffer.getDouble();
        m_read_position = m_byte_buffer.position();
        return val;
    }
    
    public String getString() throws IOException
    {
        return getString(m_read_position, RELATIVE_ACCESS);
    }
    
    /*
     * =============================================================================================================================
     * Absolute get methods : These absolute methods do not change m_read_position
     * =============================================================================================================================
     */
    
    public byte getByte(int index) throws IOException
    {
        return readRawByte(index, ABSOLUTE_ACCESS);
    }
    
    public boolean getBoolean(int index) throws IOException
    {
        int val = readRawVarint32(index, ABSOLUTE_ACCESS);
        return val == 1 ? true : false;
    }
    
    public int getInt32(int index) throws IOException
    {
        return readSInt32(index, ABSOLUTE_ACCESS);
    }
    
    public int getUnInt32(int index) throws IOException
    {
        return readRawVarint32(index, ABSOLUTE_ACCESS);
    }
    
    public long getInt64(int index) throws IOException
    {
        return readSInt64(index, ABSOLUTE_ACCESS);
    }
    
    public long getUnInt64(int index) throws IOException
    {
        return readRawVarint64(index, ABSOLUTE_ACCESS);
    }
    
    public int getFixedSizeInt(int index) throws IOException
    {
        return m_byte_buffer.getInt(index);
    }
    
    public long getFixedSizeLong(int index) throws IOException
    {
        return m_byte_buffer.getLong(index);
    }
    
    public float getFloat(int index) throws IOException
    {
        return m_byte_buffer.getFloat(index);
    }
    
    public double getDouble(int index) throws IOException
    {
        return m_byte_buffer.getDouble(index);
    }
    
    public String getString(int index) throws IOException
    {
        return getString(index, ABSOLUTE_ACCESS);
    }
    
    /*
     * =============================================================================================================================
     * Relative put methods: These methods modify the variable m_write_psition
     * =============================================================================================================================
     */
    
    public void putByte(byte b)
    {
        m_byte_buffer.put(m_write_position++, b);
    }
    
    public void putBoolean(boolean val) throws IOException
    {
        putBoolean(m_write_position, val, RELATIVE_ACCESS);
    }
    
    public void putInt32(int value) throws IOException
    {
        writeSInt32(m_write_position, value, RELATIVE_ACCESS);
    }
    
    public void putUnInt32(int value) throws IOException
    {
        writeRawVarint32(m_write_position, value, RELATIVE_ACCESS);
    }
    
    public void putInt64(long value) throws IOException
    {
        writeSInt64(m_write_position, value, RELATIVE_ACCESS);
    }
    
    public void putUnInt64(long value) throws IOException
    {
        writeRawVarint64(m_write_position, value, RELATIVE_ACCESS);
    }
    
    public void putFixedSizeInt(int value) throws IOException
    {
        m_byte_buffer.position(m_write_position);
        m_byte_buffer.putInt(value);
        m_write_position = m_byte_buffer.position();
    }
    
    public void putFixedSizeLong(long value) throws IOException
    {
        m_byte_buffer.position(m_write_position);
        m_byte_buffer.putLong(value);
        m_write_position = m_byte_buffer.position();
    }
    
    public void putFloat(float value) throws IOException
    {
        m_byte_buffer.position(m_write_position);
        m_byte_buffer.putFloat(value);
        m_write_position = m_byte_buffer.position();
    }
    
    public void putDouble(double value) throws IOException
    {
        m_byte_buffer.position(m_write_position);
        m_byte_buffer.putDouble(value);
        m_write_position = m_byte_buffer.position();
    }
    
    /**
     * Writes string in encoded format in the byte buffer ByteBuffer class does not have any absolute method for bulk byte put hence
     * first we set the position and then use relative bulk method for better performance
     * */
    public void putString(final String value) throws IOException
    {
        putString(m_write_position, value, RELATIVE_ACCESS);
    }
    
    /*
     * =============================================================================================================================
     * Absolute put methods: These methods do not change m_write_position
     * =============================================================================================================================
     */
    public void putByte(int index, byte b)
    {
        m_byte_buffer.put(index, b);
    }
    
    public void putBoolean(int index, boolean val) throws IOException
    {
        putBoolean(index, val, ABSOLUTE_ACCESS);
    }
    
    public void putInt32(int index, int value) throws IOException
    {
        writeSInt32(index, value, ABSOLUTE_ACCESS);
    }
    
    public void putUnInt32(int index, int value) throws IOException
    {
        writeRawVarint32(index, value, ABSOLUTE_ACCESS);
    }
    
    public void putInt64(int index, long value) throws IOException
    {
        writeSInt64(index, value, ABSOLUTE_ACCESS);
    }
    
    public void putUnInt64(int index, long value) throws IOException
    {
        writeRawVarint64(index, value, ABSOLUTE_ACCESS);
    }
    
    public void putFixedSizeInt(int index, int value) throws IOException
    {
        m_byte_buffer.putInt(index, value);
    }
    
    public void putFixedSizeLong(int index, long value) throws IOException
    {
        m_byte_buffer.putLong(index, value);
    }
    
    public void putFloat(int index, float value) throws IOException
    {
        m_byte_buffer.putFloat(index, value);
    }
    
    public void putDouble(int index, double value) throws IOException
    {
        m_byte_buffer.putDouble(index, value);
    }
    
    public void putString(int index, final String value) throws IOException
    {
        putString(index, value, ABSOLUTE_ACCESS);
    }
    
    /** Read sint32 value from the buffer */
    protected int readSInt32(int index, boolean isRelative) throws IOException
    {
        return decodeZigZag32(readRawVarint32(index, isRelative));
    }
    
    /** Read a sint64 value from the buffer. */
    protected long readSInt64(int index, boolean isRelative) throws IOException
    {
        return decodeZigZag64(readRawVarint64(index, isRelative));
    }
    
    /**
     * Read a raw Varint from the stream. If larger than 32 bits, discard the upper bits.
     */
    protected int readRawVarint32(int index, boolean isRelative) throws IOException
    {
        int position = index;
        byte tmp = readRawByte(position++, isRelative);
        if (tmp >= 0)
        {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = readRawByte(position++, isRelative)) >= 0)
        {
            result |= tmp << 7;
        }
        else
        {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = readRawByte(position++, isRelative)) >= 0)
            {
                result |= tmp << 14;
            }
            else
            {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = readRawByte(position++, isRelative)) >= 0)
                {
                    result |= tmp << 21;
                }
                else
                {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = readRawByte(position++, isRelative)) << 28;
                    if (tmp < 0)
                    {
                        // Discard upper 32 bits.
                        for (int i = 0; i < 5; i++)
                        {
                            if (readRawByte(position++, isRelative) >= 0)
                            {
                                return result;
                            }
                        }
                        throw DataEncodingException.malformedVarint();
                    }
                }
            }
        }
        return result;
    }
    
    /** Read a raw Varint from the buffer. */
    protected long readRawVarint64(int index, boolean isRelative) throws IOException
    {
        int shift = 0;
        long result = 0;
        int position = index;
        while (shift < 64)
        {
            final byte b = readRawByte(position++, isRelative);
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return result;
            }
            shift += 7;
        }
        throw DataEncodingException.malformedVarint();
    }
    
    protected byte readRawByte(int index, boolean isRelative) throws IOException
    {
        if (index >= BLOCK_SIZE)
        {
            throw DataEncodingException.truncatedMessage();
        }
        byte b = m_byte_buffer.get(index);
        if (isRelative) m_read_position++;
        return b;
    }
    
    /**
     * Encode and write a varint. {@code value} is treated as unsigned, so it won't be sign-extended if negative.
     */
    protected void writeRawVarint32(int index, int value, boolean isRelative) throws IOException
    {
        int position = index;
        int val = value;
        while (true)
        {
            if ((val & ~0x7F) == 0)
            {
                writeRawByte(position++, val, isRelative);
                return;
            }
            else
            {
                writeRawByte(position++, (val & 0x7F) | 0x80, isRelative);
                val >>>= 7;
            }
        }
    }
    
    /** Encode and write a varint. */
    protected void writeRawVarint64(int index, long value, boolean isRelative) throws IOException
    {
        int position = index;
        long val = value;
        while (true)
        {
            if ((val & ~0x7FL) == 0)
            {
                writeRawByte(position++, (int) val, isRelative);
                return;
            }
            else
            {
                writeRawByte(position++, ((int) val & 0x7F) | 0x80, isRelative);
                val >>>= 7;
            }
        }
    }
    
    /** Write a single byte. */
    protected void writeRawByte(int index, final byte value, boolean isRelative) throws IOException
    {
        if (!isFull(index))
        {
            m_byte_buffer.put(index, value);
            if (isRelative) m_write_position++;
        }
    }
    
    /** Write a single byte, represented by an integer value. */
    protected void writeRawByte(int index, final int value, boolean isRelative) throws IOException
    {
        writeRawByte(index, (byte) value, isRelative);
    }
    
    /** Write a sint32 field to the stream. */
    protected void writeSInt32(int index, final int value, boolean isRelative) throws IOException
    {
        writeRawVarint32(index, encodeZigZag32(value), isRelative);
    }
    
    /** Write a sint64 field to the stream. */
    protected void writeSInt64(int index, final long value, boolean isRelative) throws IOException
    {
        writeRawVarint64(index, encodeZigZag64(value), isRelative);
    }
    
    /**
     * As of now, booleans are stored as one byte. It is stored as 1 if true and 0 if false
     * 
     * @throws IOException
     */
    protected void putBoolean(int index, boolean value, boolean isRelative) throws IOException
    {
        if (value)
        {
            writeRawVarint32(index, 1, isRelative);
        }
        else
        {
            writeRawVarint32(index, 0, isRelative);
        }
    }
    
    /**
     * @param n
     *            A signed 32-bit integer.
     * @return An unsigned 32-bit integer, stored in a signed int because Java has no explicit unsigned support.
     */
    public static int encodeZigZag32(final int n)
    {
        // Note: the right-shift must be arithmetic
        return (n << 1) ^ (n >> 31);
    }
    
    /**
     * @param n
     *            A signed 64-bit integer.
     * @return An unsigned 64-bit integer, stored in a signed int because Java has no explicit unsigned support.
     */
    public static long encodeZigZag64(final long n)
    {
        // Note: the right-shift must be arithmetic
        return (n << 1) ^ (n >> 63);
    }
    
    /**
     * @param n
     *            An unsigned 32-bit integer, stored in a signed int because Java has no explicit unsigned support.
     * @return A signed 32-bit integer.
     */
    public static int decodeZigZag32(final int n)
    {
        return (n >>> 1) ^ -(n & 1);
    }
    
    /**
     * Compute the number of bytes that would be needed to encode a varint. {@code value} is treated as unsigned, so it won't be
     * sign-extended if negative.
     */
    public static int computeRawVarint32Size(final int value)
    {
        if ((value & (0xffffffff << 7)) == 0) return 1;
        if ((value & (0xffffffff << 14)) == 0) return 2;
        if ((value & (0xffffffff << 21)) == 0) return 3;
        if ((value & (0xffffffff << 28)) == 0) return 4;
        return 5;
    }
    
    /**
     * @param n
     *            An unsigned 64-bit integer, stored in a signed int because Java has no explicit unsigned support.
     * @return A signed 64-bit integer.
     */
    public static long decodeZigZag64(final long n)
    {
        return (n >>> 1) ^ -(n & 1);
    }
    
    /** Read an string value from the byte buffer */
    protected String getString(int index, boolean isRelative) throws IOException
    {
        int position = index;
        final int size = readRawVarint32(position, isRelative);
        position = position + computeRawVarint32Size(size);
        if (size <= (BLOCK_SIZE - position) && size > 0)
        {
            byte[] bytes = new byte[size];
            m_byte_buffer.position(position);
            m_byte_buffer.get(bytes, 0, size);
            final String result = new String(bytes, 0, size, "UTF-8");
            if (isRelative)
            {
                m_read_position = m_byte_buffer.position();
            }
            return result;
        }
        else
        {
            throw DataEncodingException.truncatedMessage();
        }
    }
    
    /**
     * Writes string in encoded format in the byte buffer ByteBuffer class does not have any absolute method for bulk byte put hence
     * first we set the position and then use relative bulk method for better performance
     * */
    protected void putString(int index, final String value, boolean isRelative) throws IOException
    {
        // Unfortunately there does not appear to be any way to tell Java to encode
        // UTF-8 directly into our buffer, so we have to let it create its own byte
        // array and then copy.
        final byte[] bytes = value.getBytes("UTF-8");
        int position = index;
        int len = bytes.length;
        writeRawVarint32(index, len, isRelative);
        position = position + computeRawVarint32Size(len);
        m_byte_buffer.position(position);
        m_byte_buffer.put(bytes, 0, len);
        if (isRelative) m_write_position = m_byte_buffer.position();
    }
    
}
