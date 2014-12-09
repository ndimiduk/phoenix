package org.apache.phoenix.schema;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hbase.util.Bytes;

public class UnsignedFloat extends PDataType<PFloat> {

  public static final UnsignedFloat INSTANCE = new UnsignedFloat();

  private UnsignedFloat() {
    super("UNSIGNED_FLOAT", 14, Float.class, new UnsignedFloatCodec(), 19);
  }

  @Override
  public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
    return PFloat.INSTANCE.compareTo(lhs, rhs, rhsType);
  }

  @Override
  public boolean isFixedWidth() {
    return true;
  }

  @Override
  public Integer getByteSize() {
    return Bytes.SIZEOF_FLOAT;
  }

  @Override
  public Integer getScale(Object o) {
    return PFloat.INSTANCE.getScale(o);
  }

  @Override
  public Integer getMaxLength(Object o) {
    return PFloat.INSTANCE.getMaxLength(o);
  }

  @Override
  public byte[] toBytes(Object object) {
    byte[] b = new byte[Bytes.SIZEOF_FLOAT];
    toBytes(object, b, 0);
    return b;
  }

  @Override
  public int toBytes(Object object, byte[] bytes, int offset) {
    if (object == null) {
      throw newIllegalDataException(this + " may not be null");
    }
    return this.getCodec().encodeFloat(((Number) object).floatValue(),
        bytes, offset);
  }

  @Override
  public Object toObject(String value) {
    if (value == null || value.length() == 0) {
      return null;
    }
    try {
      Float f = Float.parseFloat(value);
      if (f.floatValue() < 0) {
        throw newIllegalDataException("Value may not be negative("
            + f + ")");
      }
      return f;
    } catch (NumberFormatException e) {
      throw newIllegalDataException(e);
    }
  }

  @Override
  public Object toObject(Object object, PDataType actualType) {
    Float v = (Float) PFloat.INSTANCE.toObject(object, actualType);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public Object toObject(byte[] b, int o, int l, PDataType actualType, SortOrder sortOrder,
      Integer maxLength, Integer scale) {
    Float v = (Float) PFloat.INSTANCE.toObject(b, o, l, actualType, sortOrder);
    throwIfNonNegativeNumber(v);
    return v;
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType, Object value) {
    return super.isCoercibleTo(targetType) || PFloat.INSTANCE.isCoercibleTo(targetType, value);
  }

  @Override
  public boolean isCoercibleTo(PDataType targetType) {
    return this.equals(targetType) || UnsignedDouble.INSTANCE.isCoercibleTo(targetType) || PFloat.INSTANCE
        .isCoercibleTo(targetType);
  }

  @Override
  public int getResultSetSqlType() {
    return PFloat.INSTANCE.getResultSetSqlType();
  }

  @Override
  public Object getSampleValue(Integer maxLength, Integer arrayLength) {
    return Math.abs((Float) PFloat.INSTANCE.getSampleValue(maxLength, arrayLength));
  }

  static class UnsignedFloatCodec extends PFloat.FloatCodec {

    @Override
    public int encodeFloat(float v, byte[] b, int o) {
      checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
      if (v < 0) {
        throw newIllegalDataException();
      }
      Bytes.putFloat(b, o, v);
      return Bytes.SIZEOF_FLOAT;
    }

    @Override
    public float decodeFloat(byte[] b, int o, SortOrder sortOrder) {
      Preconditions.checkNotNull(sortOrder);
      checkForSufficientLength(b, o, Bytes.SIZEOF_FLOAT);
      if (sortOrder == SortOrder.DESC) {
        b = SortOrder.invert(b, o, new byte[Bytes.SIZEOF_FLOAT], 0, Bytes.SIZEOF_FLOAT);
      }
      float v = Bytes.toFloat(b, o);
      if (v < 0) {
        throw newIllegalDataException();
      }
      return v;
    }
  }
}
