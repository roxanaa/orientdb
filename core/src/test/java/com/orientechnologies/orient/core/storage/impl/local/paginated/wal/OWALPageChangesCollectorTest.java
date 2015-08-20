package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Random;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/19/2015
 */
@Test
public class OWALPageChangesCollectorTest {

  public void testSingleLongValueInStartChunk() {
    byte[] data = new byte[128];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setLongValue(pointer, 64, 42);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 64), 42);

    pointer.free();
  }

  public void testSingleLongValuesInMiddleOfChunk() {
    byte[] data = new byte[128];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setLongValue(pointer, 60, 42);
    Assert.assertEquals(changesCollector.getLongValue(pointer, 60), 42);

    pointer.free();
  }

  public void testSingleIntValue() {
    byte[] data = new byte[128];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setIntValue(pointer, 64, 42);
    Assert.assertEquals(changesCollector.getIntValue(pointer, 64), 42);

    pointer.free();
  }

  public void testSingleShortValue() {
    byte[] data = new byte[128];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setShortValue(pointer, 64, (short) 42);
    Assert.assertEquals(changesCollector.getShortValue(pointer, 64), 42);

    pointer.free();
  }

  public void testSingleByteValue() {
    byte[] data = new byte[128];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(128);
    changesCollector.setByteValue(pointer, 64, (byte) 42);
    Assert.assertEquals(changesCollector.getByteValue(pointer, 64), 42);

    pointer.free();
  }

  public void testBinaryValueTwoChunksFromStart() {
    byte[] data = new byte[256];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(256);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, 64, changes);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 64, 128), changes);

    pointer.free();
  }

  public void testBinaryValueTwoChunksInMiddle() {
    byte[] data = new byte[256];
    ODirectMemoryPointer pointer = new ODirectMemoryPointer(data);

    OWALPageChangesCollector changesCollector = new OWALPageChangesCollector(256);
    byte[] changes = new byte[128];

    Random random = new Random();
    random.nextBytes(changes);

    changesCollector.setBinaryValue(pointer, 32, changes);

    Assert.assertEquals(changesCollector.getBinaryValue(pointer, 32, 128), changes);

    pointer.free();
  }
}
