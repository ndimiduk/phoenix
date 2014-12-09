package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.sql.Types;

public class CharArray extends PArrayDataType<String[]> {

  public static final CharArray INSTANCE = new CharArray();

  private CharArray() {
    super("CHAR ARRAY", PDataType.ARRAY_TYPE_BASE + Char.INSTANCE.getSqlType(), PhoenixArray.class,
        null, 29);
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
    return toBytes(object, Char.INSTANCE, sortOrder);
  }

  @Override
  public Object toObject(byte[] bytes, int offset, int length,
      PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
    return toObject(bytes, offset, length, Char.INSTANCE, sortOrder, maxLength, scale,
        Char.INSTANCE);
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
    Object[] charArr = (Object[]) pArr.array;
    for (Object i : charArr) {
      if (!super.isCoercibleTo(Char.INSTANCE, i)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType,
      Integer maxLength, Integer scale, SortOrder actualModifer, Integer desiredMaxLength,
      Integer desiredScale, SortOrder desiredModifier) {
    coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
        this, actualModifer, desiredModifier);
  }

  @Override
  public int getResultSetSqlType() {
    return Types.ARRAY;
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return getSampleValue(Char.INSTANCE, arrayLength, maxLength);
  }
}
