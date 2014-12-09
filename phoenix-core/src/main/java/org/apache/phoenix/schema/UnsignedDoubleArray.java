package org.apache.phoenix.schema;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.sql.Types;

public class UnsignedDoubleArray extends PArrayDataType<double[]> {

  public static final UnsignedDoubleArray INSTANCE = new UnsignedDoubleArray();

  private UnsignedDoubleArray() {
    super("UNSIGNED_DOUBLE ARRAY", PDataType.ARRAY_TYPE_BASE + UnsignedDouble.INSTANCE.getSqlType(),
        PhoenixArray.class, null, 47);
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
    return toBytes(object, UnsignedDouble.INSTANCE, sortOrder);
  }

  @Override
  public Object toObject(byte[] bytes, int offset, int length,
      PDataType actualType, SortOrder sortOrder, Integer maxLength,
      Integer scale) {
    return toObject(bytes, offset, length, UnsignedDouble.INSTANCE, sortOrder, maxLength,
        scale, UnsignedDouble.INSTANCE);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return isCoercibleTo(targetType, this);
  }

  @Override
  public void coerceBytes(ImmutableBytesWritable ptr, Object object, PDataType actualType,
      Integer maxLength, Integer scale, SortOrder actualModifer, Integer desiredMaxLength,
      Integer desiredScale, SortOrder desiredModifier) {
    coerceBytes(ptr, object, actualType, maxLength, scale, desiredMaxLength, desiredScale,
        this, actualModifer, desiredModifier);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    if (value == null) {
      return true;
    }
    PhoenixArray pArr = (PhoenixArray) value;
    double[] doubleArr = (double[]) pArr.array;
    for (double i : doubleArr) {
      if (!super.isCoercibleTo(UnsignedDouble.INSTANCE, i) && (!super.isCoercibleTo(
          UnsignedTimestamp.INSTANCE, i))
          && (!super.isCoercibleTo(UnsignedTime.INSTANCE, i)) && (!super
          .isCoercibleTo(UnsignedDate.INSTANCE, i))) {
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
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return getSampleValue(UnsignedDouble.INSTANCE, arrayLength, maxLength);
  }
}
