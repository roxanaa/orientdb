/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.storage.fs;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.atomic.AtomicLong;

import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.concur.lock.OReadersWriterSpinLock;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.serialization.OBinaryProtocol;

public class OFileClassic implements OFile {
  private static final boolean         trackFileClose           = OGlobalConfiguration.TRACK_FILE_CLOSE.getValueAsBoolean();

  public final static String           NAME                     = "classic";
  public static final int              HEADER_SIZE              = 1024;
  private static final int             SOFTLY_CLOSED_OFFSET_V_0 = 8;
  private static final int             SOFTLY_CLOSED_OFFSET     = 16;
  private static final int             VERSION_OFFSET           = 48;
  private static final int             CURRENT_VERSION          = 1;
  private static final int             OPEN_RETRY_MAX           = 10;
  private static final int             OPEN_DELAY_RETRY         = 100;
  private static final long            LOCK_WAIT_TIME           = 300;
  private static final int             LOCK_MAX_RETRIES         = 10;

  private volatile File                osFile;
  private final String                 mode;

  private RandomAccessFile             accessFile;
  private FileChannel                  channel;
  private int                          version;

  private boolean                      failCheck                = true;
  private final AtomicLong             size                     = new AtomicLong();
  private FileLock                     fileLock;
  private boolean                      wasSoftlyClosed          = true;
  private final OReadersWriterSpinLock rwSpinLock               = new OReadersWriterSpinLock();

  public OFileClassic(String osFile, String mode) {
    this.mode = mode;
    this.osFile = new File(osFile);
  }

  @Override
  public long allocateSpace(long requiredSize) {
    long currentSize;

    rwSpinLock.acquireReadLock();
    try {
      currentSize = size.get();
      if (currentSize >= requiredSize)
        return 0;

      while (!size.compareAndSet(currentSize, requiredSize)) {
        currentSize = size.get();

        if (currentSize >= requiredSize)
          return 0;
      }

      return requiredSize - currentSize;
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  @Override
  public void shrink(long size) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireWriteLock();
        try {
          channel.truncate(HEADER_SIZE + size);
          this.size.set(size);

          break;

        } finally {
          rwSpinLock.releaseWriteLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during file shrink for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public long getFileSize() {
    return size.get();
  }

  public void read(long offset, byte[] data, int length, int arrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset = checkRegions(offset, length);

          ByteBuffer buffer = ByteBuffer.wrap(data, arrayOffset, length);
          channel.read(buffer, offset);
          break;

        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data read for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  public void write(long offset, byte[] data, int size, int arrayOffset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          writeInternal(offset, data, size, arrayOffset);
          break;
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during data write for file  " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  private void writeInternal(long offset, byte[] data, int size, int arrayOffset) throws IOException {
    if (data != null) {
      offset += HEADER_SIZE;
      ByteBuffer byteBuffer = ByteBuffer.wrap(data, arrayOffset, size);
      channel.write(byteBuffer, offset);
    }
  }

  @Override
  public void read(long offset, byte[] destBuffer, int length) throws IOException {
    read(offset, destBuffer, length, 0);
  }

  @Override
  public int readInt(long offset) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset = checkRegions(offset, OBinaryProtocol.SIZE_INT);
          return readData(offset, OBinaryProtocol.SIZE_INT).getInt();
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during read of int data for file " + getName() + " " + attempts + "-th attempt.",
            e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public long readLong(long offset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset = checkRegions(offset, OBinaryProtocol.SIZE_LONG);
          return readData(offset, OBinaryProtocol.SIZE_LONG).getLong();
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during read of long data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public short readShort(long offset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset = checkRegions(offset, OBinaryProtocol.SIZE_SHORT);
          return readData(offset, OBinaryProtocol.SIZE_SHORT).getShort();
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during read of short data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public byte readByte(long offset) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset = checkRegions(offset, OBinaryProtocol.SIZE_BYTE);
          return readData(offset, OBinaryProtocol.SIZE_BYTE).get();
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during read of byte data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeInt(long offset, final int value) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset += HEADER_SIZE;

          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_INT);
          buffer.putInt(value);
          writeBuffer(buffer, offset);

          break;
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of int data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeLong(long offset, final long value) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset += HEADER_SIZE;
          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_LONG);
          buffer.putLong(value);
          writeBuffer(buffer, offset);
          break;
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of long data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeShort(long offset, final short value) throws IOException {
    int attempts = 0;

    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset += HEADER_SIZE;
          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_SHORT);
          buffer.putShort(value);
          writeBuffer(buffer, offset);
          break;
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of short data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  @Override
  public void writeByte(long offset, final byte value) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          offset += HEADER_SIZE;
          final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_BYTE);
          buffer.put(value);
          writeBuffer(buffer, offset);
          break;
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this,
            "Error during write of byte data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }

  }

  @Override
  public void write(long offset, final byte[] sourceBuffer) throws IOException {
    int attempts = 0;
    while (true) {
      try {
        rwSpinLock.acquireReadLock();
        try {
          if (sourceBuffer != null) {
            writeInternal(offset, sourceBuffer, sourceBuffer.length, 0);
          }
          break;
        } finally {
          rwSpinLock.releaseReadLock();
          attempts++;
        }
      } catch (IOException e) {
        OLogManager.instance().error(this, "Error during write of data for file " + getName() + " " + attempts + "-th attempt.", e);
        reopenFile(attempts, e);
      }
    }
  }

  /**
   * Synchronizes the buffered changes to disk.
   *
   * @throws IOException
   */
  @Override
  public void synch() throws IOException {
    rwSpinLock.acquireReadLock();
    try {
      try {
        if (channel != null)
          channel.force(false);
      } catch (IOException e) {
        OLogManager.instance()
            .warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName(), e);
      }
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  @Override
  public void create() throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      openChannel();
      init(HEADER_SIZE);

      setVersion(OFileClassic.CURRENT_VERSION);
      version = OFileClassic.CURRENT_VERSION;
      setSoftlyClosed(!failCheck);
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  @Override
  public boolean isSoftlyClosed() throws IOException {
    rwSpinLock.acquireReadLock();
    try {
      final ByteBuffer buffer;
      if (version == 0)
        buffer = readData(SOFTLY_CLOSED_OFFSET_V_0, 1);
      else
        buffer = readData(SOFTLY_CLOSED_OFFSET, 1);

      return buffer.get(0) > 0;
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  public void setSoftlyClosed(final boolean value) throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      if (channel == null || mode.indexOf('w') < 0)
        return;

      final ByteBuffer buffer = ByteBuffer.allocate(1);
      buffer.put(0, (byte) (value ? 1 : 0));

      writeBuffer(buffer, SOFTLY_CLOSED_OFFSET);

      try {
        channel.force(true);
      } catch (IOException e) {
        OLogManager.instance()
            .warn(this, "Error during flush of file %s. Data may be lost in case of power failure.", getName(), e);
      }
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  /**
   * ALWAYS ADD THE HEADER SIZE BECAUSE ON THIS TYPE IS ALWAYS NEEDED
   */
  private long checkRegions(final long offset, final long length) {
    if (offset < 0 || offset + length > size.get())
      throw new OIOException("You cannot access outside the file size (" + size + " bytes). You have requested portion " + offset
          + "-" + (offset + length) + " bytes. File: " + toString());

    return offset + HEADER_SIZE;
  }

  private ByteBuffer readData(final long offset, final int size) throws IOException {
    ByteBuffer buffer = ByteBuffer.allocate(size);
    channel.read(buffer, offset);
    buffer.rewind();
    return buffer;
  }

  private void writeBuffer(final ByteBuffer buffer, final long offset) throws IOException {
    buffer.rewind();
    channel.write(buffer, offset);
  }

  private void setVersion(int version) throws IOException {
    final ByteBuffer buffer = ByteBuffer.allocate(OBinaryProtocol.SIZE_BYTE);
    buffer.put((byte) version);
    writeBuffer(buffer, VERSION_OFFSET);
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#open()
   */
  public boolean open() throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      if (!osFile.exists())
        throw new FileNotFoundException("File: " + osFile.getAbsolutePath());

      openChannel();
      init(-1);

      OLogManager.instance().debug(this, "Checking file integrity of " + osFile.getName() + "...");

      if (failCheck) {
        wasSoftlyClosed = isSoftlyClosed();

        if (wasSoftlyClosed)
          setSoftlyClosed(false);
      }

      if (version < CURRENT_VERSION) {
        setVersion(CURRENT_VERSION);
        version = CURRENT_VERSION;
        setSoftlyClosed(!failCheck);
      }

      if (failCheck)
        return wasSoftlyClosed;

      return true;
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  public boolean wasSoftlyClosed() {
    rwSpinLock.acquireReadLock();
    try {
      return wasSoftlyClosed;
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#close()
   */
  public void close() throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      if (trackFileClose) {
        final Exception exception = new Exception();
        final StringWriter writer = new StringWriter();
        writer.append("File ").append(getName()).append(" was closed at : \r\n");

        final PrintWriter printWriter = new PrintWriter(writer);
        exception.printStackTrace(printWriter);
        printWriter.flush();

        OLogManager.instance().warn(this, writer.toString());
      }

      if (accessFile != null && (accessFile.length() - HEADER_SIZE) < getFileSize())
        accessFile.setLength(getFileSize() + HEADER_SIZE);

      setSoftlyClosed(true);

      if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
        unlock();

      if (channel != null && channel.isOpen()) {
        channel.close();
        channel = null;
      }

      if (accessFile != null) {
        accessFile.close();
        accessFile = null;
      }
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error on closing file " + osFile.getAbsolutePath(), e, OIOException.class);
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#delete()
   */
  public void delete() throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      close();
      if (osFile != null) {
        boolean deleted = OFileUtils.delete(osFile);
        int retryCount = 0;

        while (!deleted) {
          deleted = OFileUtils.delete(osFile);
          retryCount++;

          if (retryCount > 10)
            throw new IOException("Can not delete file. Retry limit exceeded.");
        }
      }
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#lock()
   */
  public void lock() throws IOException {
    if (channel == null)
      return;

    rwSpinLock.acquireWriteLock();
    try {
      for (int i = 0; i < LOCK_MAX_RETRIES; ++i) {
        try {
          fileLock = channel.tryLock();
          if (fileLock != null)
            break;
        } catch (OverlappingFileLockException e) {
          OLogManager.instance().debug(this,
              "Cannot open file '" + osFile.getAbsolutePath() + "' because it is locked. Waiting %d ms and retrying %d/%d...",
              LOCK_WAIT_TIME, i, LOCK_MAX_RETRIES);
        }

        if (fileLock == null)
          throw new OLockException(
              "File '"
                  + osFile.getPath()
                  + "' is locked by another process, maybe the database is in use by another process. Use the remote mode with a OrientDB server to allow multiple access to the same database.");
      }
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#unlock()
   */
  public void unlock() throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      if (fileLock != null) {
        try {
          fileLock.release();
        } catch (ClosedChannelException e) {
        }
        fileLock = null;
      }
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  private void openChannel() throws IOException {
    OLogManager.instance().debug(this, "[OFile.openChannel] opening channel for file '%s' of size: %d", osFile, osFile.length());

    for (int i = 0; i < OPEN_RETRY_MAX; ++i)
      try {
        accessFile = new RandomAccessFile(osFile, mode);
        break;
      } catch (FileNotFoundException e) {
        if (i == OPEN_RETRY_MAX - 1)
          throw e;

        // TRY TO RE-CREATE THE DIRECTORY (THIS HAPPENS ON WINDOWS AFTER A DELETE IS PENDING, USUALLY WHEN REOPEN THE DB VERY
        // FREQUENTLY)
        if (!osFile.getParentFile().mkdirs())
          try {
            Thread.sleep(OPEN_DELAY_RETRY);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }
      }

    if (accessFile == null)
      throw new FileNotFoundException(osFile.getAbsolutePath());

    channel = accessFile.getChannel();

    if (OGlobalConfiguration.FILE_LOCK.getValueAsBoolean())
      lock();
  }

  private void init(long newSize) throws IOException {
    if (newSize > -1 && accessFile.length() != newSize)
      accessFile.setLength(newSize);

    size.set(accessFile.length() - HEADER_SIZE);
    assert size.get() >= 0;

    accessFile.seek(VERSION_OFFSET);
    version = accessFile.read();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#isOpen()
   */
  public boolean isOpen() {
    rwSpinLock.acquireReadLock();
    try {
      return accessFile != null;
    } finally {
      rwSpinLock.releaseReadLock();
    }

  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#exists()
   */
  public boolean exists() {
    rwSpinLock.acquireReadLock();
    try {
      return osFile != null && osFile.exists();
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  public String getName() {
    rwSpinLock.acquireReadLock();
    try {
      if (osFile == null)
        return null;

      return osFile.getName();
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  public String getPath() {
    rwSpinLock.acquireReadLock();
    try {
      return osFile.getPath();
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  public String getAbsolutePath() {
    rwSpinLock.acquireReadLock();
    try {
      return osFile.getAbsolutePath();
    } finally {
      rwSpinLock.releaseReadLock();
    }
  }

  public boolean renameTo(final File newFile) throws IOException {
    rwSpinLock.acquireWriteLock();
    try {
      close();

      final boolean renamed = OFileUtils.renameFile(osFile, newFile);
      if (renamed)
        osFile = new File(newFile.getAbsolutePath());

      open();

      return renamed;
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.orientechnologies.orient.core.storage.fs.OFileAAA#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder(128);
    builder.append("File: ");
    builder.append(osFile.getName());
    if (accessFile != null) {
      builder.append(" os-size=");
      try {
        builder.append(accessFile.length());
      } catch (IOException e) {
        builder.append("?");
      }
    }
    builder.append(", stored=");
    builder.append(getFileSize());
    builder.append("");
    return builder.toString();
  }

  private void reopenFile(int attempt, IOException e) throws IOException {
    if (attempt > 1)
      throw e;

    rwSpinLock.acquireWriteLock();
    try {
      try {
        unlock();
      } catch (IOException ioe) {
        OLogManager.instance().error(this, "Error during unlock of file " + osFile.getName() + ", during IO exception handling.",
            ioe);
      }

      try {
        channel.close();
      } catch (IOException ioe) {
        OLogManager.instance().error(this,
            "Error during channel close for file " + osFile.getAbsolutePath() + ", during IO exception handling.", ioe);
      }

      try {
        accessFile.close();
      } catch (IOException ioe) {
        OLogManager.instance().error(this,
            "Error during close of file " + osFile.getAbsolutePath() + ", during IO exception handling.", ioe);
      }

      channel = null;
      accessFile = null;

      openChannel();
    } finally {
      rwSpinLock.releaseWriteLock();
    }
  }
}
