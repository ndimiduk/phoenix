package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.sql.Types;

public class UnsignedSmallintArray extends PArrayDataType<short[]> {

  public static final UnsignedSmallintArray INSTANCE = new UnsignedSmallintArray();

  private UnsignedSmallintArray() {
    super("UNSIGNED_SMALLINT ARRAY",
        PDataType.ARRAY_TYPE_BASE + UnsignedSmallint.INSTANCE.getSqlType(), PhoenixArray.class,
        null, 44);
  }

  @Override
  public boolean isArrayType() {
    return true;
  }

  @Override
  public boolean isFixedWidth() {
    return false;
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return compareTo(lhs, rhs);
  }

  @Override
  public Integer getByteSize() {
    return null;
  }

  @Override
  public byte[] toBytes(Object object) {
    return toBytes(object, SortOrder.ASC);
  }

  @Override
  public byte[] toBytes(Object object, SortOrder sortOrder) {
    return toBytes(object, UnsignedSmallint.INSTANCE, sortOrder);
  }

  @Override
  public Object toObject(byte[] bytes, int offset, int length,
      PDataType actualType, SortOrder sortOrder, Integer maxLength,
      Integer scale) {
    return toObject(bytes, offset, length, UnsignedSmallint.INSTANCE, sortOrder, maxLength,
        scale, UnsignedSmallint.INSTANCE);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return isCoercibleTo(targetType, this);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value == null) {
      return true;
    }
    PhoenixArray pArr = (PhoenixArray) value;
    short[] shortArr = (short[]) pArr.array;
    for (short i : shortArr) {
      if (!super.isCoercibleTo(UnsignedSmallint.INSTANCE, i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int getResultSetSqlType() {
    return Types.ARRAY;
  }

  @Override
  public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType,
      Integer maxLength, Integer scale, SortOrder actualModifer, Integer desiredMaxLength,
      Integer desiredScale, SortOrder desiredModifier) {
    coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
        this, actualModifer, desiredModifier);
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return getSampleValue(UnsignedSmallint.INSTANCE, arrayLength, maxLength);
  }
}
