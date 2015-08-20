package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

/**
 * @author Andrey Lomakin <lomakin.andrey@gmail.com>.
 * @since 8/19/2015
 */
public interface ODirectPointerWrapper {
  byte getByte(long offset);

  short getShort(long offset);

  int getInt(long offset);

  long getLong(long offset);

  byte[] get(long offset, int len);

  char getChar(long offset);
}
