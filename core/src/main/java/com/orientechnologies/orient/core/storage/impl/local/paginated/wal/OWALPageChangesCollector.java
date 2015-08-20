package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/17/2015
 */
public class OWALPageChangesCollector {
  private static final int PAGE_SIZE  = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024 + 2
                                          * OWOWCache.PAGE_PADDING;

  public static final int  CHUNK_SIZE = 64;

  private final byte[][]   pageChunks;
  private final int        pageSize;

  public OWALPageChangesCollector() {
    this(PAGE_SIZE);
  }

  public OWALPageChangesCollector(int pageSize) {
    this.pageSize = pageSize;
    pageChunks = new byte[(pageSize + (CHUNK_SIZE - 1)) / CHUNK_SIZE][];
  }

  public void setLongValue(ODirectMemoryPointer pointer, int offset, long value) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];
    OLongSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setIntValue(ODirectMemoryPointer pointer, int offset, int value) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];
    OIntegerSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setShortValue(ODirectMemoryPointer pointer, int offset, short value) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];
    OShortSerializer.INSTANCE.serializeNative(value, data, 0);

    updateData(pointer, offset, data);
  }

  public void setByteValue(ODirectMemoryPointer pointer, int offset, byte value) {
    byte[] data = new byte[] { value };

    updateData(pointer, offset, data);
  }

  public void setBinaryValue(ODirectMemoryPointer pointer, int offset, byte[] value) {
    updateData(pointer, offset, value);
  }

  public long getLongValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[OLongSerializer.LONG_SIZE];

    readData(pointer, offset, data);

    return OLongSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public int getIntValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[OIntegerSerializer.INT_SIZE];

    readData(pointer, offset, data);

    return OIntegerSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public short getShortValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[OShortSerializer.SHORT_SIZE];

    readData(pointer, offset, data);

    return OShortSerializer.INSTANCE.deserializeNative(data, 0);
  }

  public byte getByteValue(ODirectMemoryPointer pointer, int offset) {
    byte[] data = new byte[1];

    readData(pointer, offset, data);

    return data[0];
  }

  public byte[] getBinaryValue(ODirectMemoryPointer pointer, int offset, int len) {
    byte[] data = new byte[len];
    readData(pointer, offset, data);

    return data;
  }

  public void applyChanges(ODirectMemoryPointer pointer) {
    for (int i = 0; i < pageChunks.length; i++) {
      byte[] chunk = pageChunks[i];
      if (chunk != null) {
        if (i < pageChunks.length - 1) {
          pointer.set(((long) i) * CHUNK_SIZE, chunk, 0, chunk.length);
        } else {
          final int wl = Math.min(chunk.length, pageSize - (pageChunks.length - 1) * CHUNK_SIZE);
          pointer.set(((long) i) * CHUNK_SIZE, chunk, 0, wl);
        }
      }
    }
  }

  public ODirectPointerWrapper wrap(ODirectMemoryPointer pointer) {
    return new PointerWrapper(pointer);
  }

  public int serializedSize() {
    int result = 1 + OShortSerializer.SHORT_SIZE;
    byte[] chunk = pageChunks[0];

    if (chunk != null)
      result += CHUNK_SIZE;

    for (int i = 1; i < pageChunks.length; i++) {
      if (pageChunks[i] == null && chunk != null || pageChunks[i] != null && chunk == null) {
        result += OShortSerializer.SHORT_SIZE;
        chunk = pageChunks[i];
      }

      if (pageChunks[i] != null) {
        result += CHUNK_SIZE;
      }
    }

    return result;
  }

  public int toStream(byte[] stream, int offset) {

    if (pageChunks[0] != null) {
      stream[offset] = 1;
    }

    offset++;

    int batchLen = 0;
    int chunkIndex = 0;

    int startIndex = 0;
    byte[] chunk;

    while (chunkIndex < pageChunks.length) {
      chunk = pageChunks[chunkIndex];

      while (chunkIndex < pageChunks.length
          && (chunk == null && pageChunks[chunkIndex] == null || chunk != null && pageChunks[chunkIndex] != null)) {
        batchLen++;
        chunkIndex++;
      }

      OShortSerializer.INSTANCE.serializeNative((short) batchLen, stream, offset);
      offset += OShortSerializer.SHORT_SIZE;

      if (chunk != null) {
        for (int i = startIndex; i < chunkIndex; i++) {
          assert pageChunks[i] != null;
          System.arraycopy(pageChunks[i], 0, stream, offset, CHUNK_SIZE);
          offset += CHUNK_SIZE;
        }
      }

      startIndex = chunkIndex;
      batchLen = 0;
    }
    return offset;
  }

  public int fromStream(byte[] stream, int offset) {
    boolean isNull = stream[offset] == 0;
    offset++;

    int chunkLength;
    int chunkIndex = 0;

    do {

      chunkLength = OShortSerializer.INSTANCE.deserializeNative(stream, offset);
      offset += OShortSerializer.SHORT_SIZE;

      if (!isNull) {
        for (int i = 0; i < chunkLength; i++) {
          byte[] chunk = new byte[CHUNK_SIZE];
          System.arraycopy(stream, offset, chunk, 0, CHUNK_SIZE);

          pageChunks[chunkIndex] = chunk;
          chunkIndex++;

          offset += CHUNK_SIZE;
        }
      } else {
        chunkIndex += chunkLength;
      }

      isNull = !isNull;
    } while (chunkIndex < pageChunks.length);

    return offset;
  }

  private void readData(ODirectMemoryPointer pointer, int offset, byte[] data) {
    int chunkIndex = offset / CHUNK_SIZE;
    int chunkOffset = offset - chunkIndex * CHUNK_SIZE;

    int read = 0;

    while (read < data.length) {
      byte[] chunk = pageChunks[chunkIndex];

      if (chunk == null) {
        if (pointer != null) {
          if (chunkIndex < pageChunks.length - 1) {
            chunk = pointer.get(((long) chunkIndex * CHUNK_SIZE), CHUNK_SIZE);
          } else {
            final int chunkSize = Math.min(CHUNK_SIZE, pageSize - (pageChunks.length - 1) * CHUNK_SIZE);
            chunk = new byte[CHUNK_SIZE];

            assert chunkSize <= CHUNK_SIZE;
            assert chunkSize > 0;

            System.arraycopy(pointer.get(((long) chunkIndex * CHUNK_SIZE), chunkSize), 0, chunk, 0, chunkSize);
          }
        } else
          chunk = new byte[CHUNK_SIZE];
      }

      final int rl = Math.min(CHUNK_SIZE - chunkOffset, data.length - read);
      System.arraycopy(chunk, chunkOffset, data, read, rl);

      read += rl;
      chunkOffset = 0;
      chunkIndex++;
    }
  }

  private void updateData(ODirectMemoryPointer pointer, int offset, byte[] data) {
    int chunkIndex = offset / CHUNK_SIZE;
    int chunkOffset = offset - chunkIndex * CHUNK_SIZE;

    int written = 0;

    while (written < data.length) {
      byte[] chunk = pageChunks[chunkIndex];

      if (chunk == null) {
        if (pointer != null) {
          if (chunkIndex < pageChunks.length - 1)
            chunk = pointer.get(((long) chunkIndex) * CHUNK_SIZE, CHUNK_SIZE);
          else {
            final int chunkSize = Math.min(CHUNK_SIZE, pageSize - (pageChunks.length - 1) * CHUNK_SIZE);
            chunk = new byte[CHUNK_SIZE];

            assert chunkSize <= CHUNK_SIZE;
            assert chunkSize > 0;

            System.arraycopy(pointer.get(((long) chunkIndex * CHUNK_SIZE), chunkSize), 0, chunk, 0, chunkSize);
          }
        } else {
          chunk = new byte[CHUNK_SIZE];
        }

        pageChunks[chunkIndex] = chunk;
      }

      final int wl = Math.min(CHUNK_SIZE - chunkOffset, data.length - written);
      System.arraycopy(data, written, chunk, chunkOffset, wl);

      written += wl;
      chunkOffset = 0;
      chunkIndex++;
    }
  }

  private final class PointerWrapper implements ODirectPointerWrapper {
    private final ODirectMemoryPointer pointer;

    public PointerWrapper(ODirectMemoryPointer pointer) {
      this.pointer = pointer;
    }

    @Override
    public byte getByte(long offset) {
      return getByteValue(pointer, (int) offset);
    }

    @Override
    public short getShort(long offset) {
      return getShortValue(pointer, (int) offset);
    }

    @Override
    public int getInt(long offset) {
      return getIntValue(pointer, (int) offset);
    }

    @Override
    public long getLong(long offset) {
      return getLongValue(pointer, (int) offset);
    }

    @Override
    public byte[] get(long offset, int len) {
      return getBinaryValue(pointer, (int) offset, len);
    }

    @Override
    public char getChar(long offset) {
      return (char) getShortValue(pointer, (int) offset);
    }
  }
}
