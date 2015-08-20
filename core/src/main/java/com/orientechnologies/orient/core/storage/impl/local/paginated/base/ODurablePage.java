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

package com.orientechnologies.orient.core.storage.impl.local.paginated.base;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChangesTree;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPageChangesCollector;

import java.io.IOException;

/**
 * Base page class for all durable data structures, that is data structures state of which can be consistently restored after system
 * crash but results of last operations in small interval before crash may be lost.
 * 
 * This page has several booked memory areas with following offsets at the beginning:
 * <ol>
 * <li>from 0 to 7 - Magic number</li>
 * <li>from 8 to 11 - crc32 of all page content, which is calculated by cache system just before save</li>
 * <li>from 12 to 23 - LSN of last operation which was stored for given page</li>
 * </ol>
 * 
 * Developer which will extend this class should use all page memory starting from {@link #NEXT_FREE_POSITION} offset.
 * 
 * {@link OReadCache#release(OCacheEntry, com.orientechnologies.orient.core.storage.cache.OWriteCache)} back to the cache.
 * 
 * All data structures which use this kind of pages should be derived from
 * {@link com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent} class.
 * 
 * @author Andrey Lomakin
 * @since 16.08.13
 */
public class ODurablePage {
  public static final int            PAGE_PADDING        = OWOWCache.PAGE_PADDING;

  protected static final int         MAGIC_NUMBER_OFFSET = 0;
  protected static final int         CRC32_OFFSET        = MAGIC_NUMBER_OFFSET + OLongSerializer.LONG_SIZE;

  public static final int            WAL_SEGMENT_OFFSET  = CRC32_OFFSET + OIntegerSerializer.INT_SIZE;
  public static final int            WAL_POSITION_OFFSET = WAL_SEGMENT_OFFSET + OLongSerializer.LONG_SIZE;
  public static final int            MAX_PAGE_SIZE_BYTES = OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024;

  protected static final int         NEXT_FREE_POSITION  = WAL_POSITION_OFFSET + OLongSerializer.LONG_SIZE;

  protected OWALPageChangesCollector changesCollector;

  private final OCacheEntry          cacheEntry;
  private final ODirectMemoryPointer pagePointer;

  public ODurablePage(OCacheEntry cacheEntry, OWALPageChangesCollector changesCollector) {
    assert cacheEntry != null || changesCollector != null;

    this.cacheEntry = cacheEntry;

    if (cacheEntry != null) {
      final OCachePointer cachePointer = cacheEntry.getCachePointer();
      this.pagePointer = cachePointer.getDataPointer();
    } else
      this.pagePointer = null;

    this.changesCollector = changesCollector;
  }

  public static OLogSequenceNumber getLogSequenceNumberFromPage(ODirectMemoryPointer dataPointer) {
    final long segment = OLongSerializer.INSTANCE.deserializeFromDirectMemory(dataPointer, WAL_SEGMENT_OFFSET + PAGE_PADDING);
    final long position = OLongSerializer.INSTANCE.deserializeFromDirectMemory(dataPointer, WAL_POSITION_OFFSET + PAGE_PADDING);

    return new OLogSequenceNumber(segment, position);
  }

  protected int getIntValue(int pageOffset) {
    if (changesCollector == null)
      return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, pageOffset + PAGE_PADDING);

    return OIntegerSerializer.INSTANCE.deserializeFromDirectMemory(changesCollector.wrap(pagePointer), pageOffset + PAGE_PADDING);
  }

  protected long getLongValue(int pageOffset) {
    if (changesCollector == null)
      return OLongSerializer.INSTANCE.deserializeFromDirectMemory(pagePointer, pageOffset + PAGE_PADDING);

    return OLongSerializer.INSTANCE.deserializeFromDirectMemory(changesCollector.wrap(pagePointer), pageOffset + PAGE_PADDING);
  }

  protected byte[] getBinaryValue(int pageOffset, int valLen) {
    if (changesCollector == null)
      return pagePointer.get(pageOffset + PAGE_PADDING, valLen);

    return changesCollector.getBinaryValue(pagePointer, pageOffset + PAGE_PADDING, valLen);
  }

  protected int getObjectSizeInDirectMemory(OBinarySerializer binarySerializer, long offset) {
    if (changesCollector == null)
      return binarySerializer.getObjectSizeInDirectMemory(pagePointer, offset + PAGE_PADDING);

    return binarySerializer.getObjectSizeInDirectMemory(changesCollector.wrap(pagePointer), offset + PAGE_PADDING);
  }

  protected <T> T deserializeFromDirectMemory(OBinarySerializer<T> binarySerializer, long offset) {
    if (changesCollector == null)
      return binarySerializer.deserializeFromDirectMemoryObject(pagePointer, offset + PAGE_PADDING);

    return binarySerializer.deserializeFromDirectMemoryObject(changesCollector.wrap(pagePointer), offset + PAGE_PADDING);
  }

  protected byte getByteValue(int pageOffset) {
    if (changesCollector == null)
      return pagePointer.getByte(pageOffset + PAGE_PADDING);

    return changesCollector.getByteValue(pagePointer, pageOffset + PAGE_PADDING);
  }

  protected int setIntValue(int pageOffset, int value) throws IOException {
    if (changesCollector != null) {
      changesCollector.setIntValue(pagePointer, pageOffset + PAGE_PADDING, value);
    } else
      OIntegerSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset + PAGE_PADDING);

    cacheEntry.markDirty();

    return OIntegerSerializer.INT_SIZE;

  }

  protected int setByteValue(int pageOffset, byte value) {
    if (changesCollector != null) {
      changesCollector.setByteValue(pagePointer, pageOffset + PAGE_PADDING, value);
    } else
      pagePointer.setByte(pageOffset + PAGE_PADDING, value);

    cacheEntry.markDirty();

    return OByteSerializer.BYTE_SIZE;
  }

  protected int setLongValue(int pageOffset, long value) throws IOException {
    if (changesCollector != null) {
      changesCollector.setLongValue(pagePointer, pageOffset + PAGE_PADDING, value);
    } else
      OLongSerializer.INSTANCE.serializeInDirectMemory(value, pagePointer, pageOffset + PAGE_PADDING);

    cacheEntry.markDirty();

    return OLongSerializer.LONG_SIZE;
  }

  protected int setBinaryValue(int pageOffset, byte[] value) throws IOException {
    if (value.length == 0)
      return 0;

    if (changesCollector != null) {
      changesCollector.setBinaryValue(pagePointer, pageOffset + PAGE_PADDING, value);
    } else
      pagePointer.set(pageOffset + PAGE_PADDING, value, 0, value.length);

    cacheEntry.markDirty();

    return value.length;
  }

  protected void moveData(int from, int to, int len) throws IOException {
    if (len == 0)
      return;

    if (changesCollector != null) {
      byte[] content = changesCollector.getBinaryValue(pagePointer, from + PAGE_PADDING, len);

      changesCollector.setBinaryValue(pagePointer, to + PAGE_PADDING, content);
    } else
      pagePointer.moveData(from + PAGE_PADDING, pagePointer, to + PAGE_PADDING, len);

    cacheEntry.markDirty();
  }

  public OWALPageChangesCollector getChanges() {
    return changesCollector;
  }

  public void restoreChanges(OWALPageChangesCollector changesCollector) {
    changesCollector.applyChanges(cacheEntry.getCachePointer().getDataPointer());
    cacheEntry.markDirty();
  }

  public OLogSequenceNumber getLsn() {
    final long segment = getLongValue(WAL_SEGMENT_OFFSET);
    final long position = getLongValue(WAL_POSITION_OFFSET);

    return new OLogSequenceNumber(segment, position);
  }

  public void setLsn(OLogSequenceNumber lsn) {
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getSegment(), pagePointer, WAL_SEGMENT_OFFSET + PAGE_PADDING);
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getPosition(), pagePointer, WAL_POSITION_OFFSET + PAGE_PADDING);

    cacheEntry.markDirty();
  }
}
