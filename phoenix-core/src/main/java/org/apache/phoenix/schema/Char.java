package org.apache.phoenix.schema;

import com.google.common.base.Strings;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.ValueTypeIncompatibleException;
import org.apache.phoenix.util.StringUtil;

import java.sql.Types;
import java.text.Format;
import java.util.Arrays;

/**
 * Fixed length single byte characters
 */
public class Char extends PDataType<String> {

  public static final Char INSTANCE = new Char();

  private Char() {
    super("CHAR", Types.CHAR, String.class, null, 1);
  }

    @Override
    public void pad(ImmutableBytesWritable ptr, Integer maxLength) {
      if (ptr.getLength() >= maxLength) {
        return;
      }
      byte[] newBytes = new byte[maxLength];
      System.arraycopy(ptr.get(), ptr.getOffset(), newBytes, 0, ptr.getLength());
      Arrays.fill(newBytes, ptr.getLength(), maxLength, StringUtil.SPACE_UTF8);
      ptr.set(newBytes);
    }

    @Override
    public Object pad(Object object, Integer maxLength) {
      String s = (String) object;
      if (s == null) {
        return s;
      }
      if (s.length() == maxLength) {
        return object;
      }
      if (s.length() > maxLength) {
        throw new ValueTypeIncompatibleException(this,maxLength,null);
      }
      return Strings.padEnd(s, maxLength, ' ');
    }

    @Override
    public byte[] toBytes(Object object) {
      if (object == null) {
        throw newIllegalDataException(this + " may not be null");
      }
      byte[] b = Varchar.INSTANCE.toBytes(object);
      if (b.length != ((String) object).length()) {
        throw newIllegalDataException("CHAR types may only contain single byte characters (" + object + ")");
      }
      return b;
    }

    @Override
    public int toBytes(Object object, byte[] bytes, int offset) {
      if (object == null) {
        throw newIllegalDataException(this + " may not be null");
      }
      int len = Varchar.INSTANCE.toBytes(object, bytes, offset);
      if (len != ((String) object).length()) {
        throw newIllegalDataException("CHAR types may only contain single byte characters (" + object + ")");
      }
      return len;
    }

    @Override
    public Object toObject(byte[] bytes, int offset, int length, PDataType actualType, SortOrder sortOrder, Integer maxLength, Integer scale) {
      if (!actualType.isCoercibleTo(this)) { // TODO: have isCoercibleTo that takes bytes, offset?
        throwConstraintViolationException(actualType,this);
      }
      if (length == 0) {
        return null;
      }
      length = StringUtil.getUnpaddedCharLength(bytes, offset, length, sortOrder);
      if (sortOrder == SortOrder.DESC) {
        bytes = SortOrder.invert(bytes, offset, length);
        offset = 0;
      }
      // TODO: UTF-8 decoder that will invert as it decodes
      String s = Bytes.toString(bytes, offset, length);
      if (length != s.length()) {
        throw newIllegalDataException("CHAR types may only contain single byte characters (" + s + ")");
      }
      return s;
    }

    @Override
    public Object toObject(Object object, PDataType actualType) {
      if (equalsAny(actualType, Varchar.INSTANCE, this)) {
        String s = (String) object;
        return s == null || s.length() > 0 ? s : null;
      }
      return throwConstraintViolationException(actualType,this);
    }

    @Override
    public boolean isCoercibleTo(PDataType targetType) {
      return equalsAny(targetType, this, Varchar.INSTANCE, Binary.INSTANCE, Varbinary.INSTANCE);
    }

    @Override
    public void coerceBytes(ImmutableBytesWritable ptr, Object o, PDataType actualType,
        Integer actualMaxLength, Integer actualScale, SortOrder actualModifier,
        Integer desiredMaxLength, Integer desiredScale, SortOrder expectedModifier) {
      if (o != null && actualType.equals(Varchar.INSTANCE) && ((String)o).length() != ptr.getLength()) {
        throw newIllegalDataException("CHAR types may only contain single byte characters (" + o + ")");
      }
      super.coerceBytes(ptr, o, actualType, actualMaxLength, actualScale, actualModifier, desiredMaxLength, desiredScale, expectedModifier);
    }

    @Override
    public boolean isSizeCompatible(ImmutableBytesWritable ptr, Object value, PDataType srcType,
        Integer maxLength, Integer scale, Integer desiredMaxLength, Integer desiredScale) {
      return Varchar.INSTANCE.isSizeCompatible(ptr, value, srcType, maxLength, scale, desiredMaxLength, desiredScale);
    }

    @Override
    public boolean isFixedWidth() {
      return true;
    }

    @Override
    public Integer getByteSize() {
      return null;
    }

    @Override
    public Integer getMaxLength(Object o) {
      if (o == null) {
        return null;
      }
      String value = (String) o;
      return value.length();
    }

    @Override
    public int estimateByteSize(Object o) {
      String value = (String) o;
      return value.length();
    }

    @Override
    public int compareTo(Object lhs, Object rhs, PDataType rhsType) {
      return Varchar.INSTANCE.compareTo(lhs, rhs, rhsType);
    }

    @Override
    public Object toObject(String value) {
      if (value == null || value.length() == 0) {
        throw newIllegalDataException(this + " may not be null");
      }
      if (StringUtil.hasMultiByteChars(value)) {
        throw newIllegalDataException("CHAR types may only contain single byte characters (" + value + ")");
      }
      return value;
    }

    @Override
    public Integer estimateByteSizeFromLength(Integer length) {
      return length;
    }

    @Override
    public boolean isBytesComparableWith(PDataType otherType) {
      return super.isBytesComparableWith(otherType) || otherType.equals(Varchar.INSTANCE);
    }

    @Override
    public String toStringLiteral(byte[] b, int offset, int length, Format formatter) {
      return Varchar.INSTANCE.toStringLiteral(b, offset, length, formatter);
    }

    @Override
    public Object getSampleValue(Integer maxLength, Integer arrayLength) {
      return Varchar.INSTANCE.getSampleValue(maxLength, arrayLength);
    }
}
