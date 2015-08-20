package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class UpdatePageRecordTest {
  public void testSerializationPrevLSNIsNotNull() {
    OWALPageChangesCollector changes = new OWALPageChangesCollector();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changes);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

  public void testSerializationPrevLSNIsNull() {
    OWALPageChangesCollector changes = new OWALPageChangesCollector();

    OOperationUnitId unitId = OOperationUnitId.generateId();

    OUpdatePageRecord serializedUpdatePageRecord = new OUpdatePageRecord(12, 100, unitId, changes);

    byte[] content = new byte[serializedUpdatePageRecord.serializedSize() + 1];

    int toStreamOffset = serializedUpdatePageRecord.toStream(content, 1);
    Assert.assertEquals(toStreamOffset, content.length);

    OUpdatePageRecord restoredUpdatePageRecord = new OUpdatePageRecord();
    int fromStreamOffset = restoredUpdatePageRecord.fromStream(content, 1);
    Assert.assertEquals(fromStreamOffset, content.length);

    Assert.assertEquals(restoredUpdatePageRecord, serializedUpdatePageRecord);
  }

}
