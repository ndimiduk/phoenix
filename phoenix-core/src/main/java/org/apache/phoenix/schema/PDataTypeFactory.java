package org.apache.phoenix.schema;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Factory avoids circular dependency problem in static initializers across types.
 */
public class PDataTypeFactory {

  private static PDataTypeFactory INSTANCE;
  private final PDataType[] orderedTypes;
  private final SortedSet<PDataType> types;
  private final Map<Class<? extends PDataType>, PDataType> classToInstance;

  public static PDataTypeFactory getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new PDataTypeFactory();
    }
    return INSTANCE;
  }

  private PDataTypeFactory() {
    types = new TreeSet<>(new Comparator<PDataType>() {
      @Override
      public int compare(PDataType o1, PDataType o2) {
        return Integer.compare(o1.ordinal(), o2.ordinal());
      }
    });    // TODO: replace with ServiceLoader or some other plugin system
    types.add(Binary.INSTANCE);
    types.add(BinaryArray.INSTANCE);
    types.add(Char.INSTANCE);
    types.add(CharArray.INSTANCE);
    types.add(Decimal.INSTANCE);
    types.add(DecimalArray.INSTANCE);
    types.add(PBoolean.INSTANCE);
    types.add(PBooleanArray.INSTANCE);
    types.add(PDate.INSTANCE);
    types.add(PDateArray.INSTANCE);
    types.add(PDouble.INSTANCE);
    types.add(PDoubleArray.INSTANCE);
    types.add(PFloat.INSTANCE);
    types.add(PFloatArray.INSTANCE);
    types.add(PInteger.INSTANCE);
    types.add(PIntegerArray.INSTANCE);
    types.add(PLong.INSTANCE);
    types.add(PLongArray.INSTANCE);
    types.add(PTime.INSTANCE);
    types.add(PTimeArray.INSTANCE);
    types.add(PTimestamp.INSTANCE);
    types.add(PTimestampArray.INSTANCE);
    types.add(Smallint.INSTANCE);
    types.add(SmallintArray.INSTANCE);
    types.add(Tinyint.INSTANCE);
    types.add(TinyintArray.INSTANCE);
    types.add(UnsignedDate.INSTANCE);
    types.add(UnsignedDateArray.INSTANCE);
    types.add(UnsignedDouble.INSTANCE);
    types.add(UnsignedDoubleArray.INSTANCE);
    types.add(UnsignedFloat.INSTANCE);
    types.add(UnsignedFloatArray.INSTANCE);
    types.add(UnsignedInt.INSTANCE);
    types.add(UnsignedIntArray.INSTANCE);
    types.add(UnsignedLong.INSTANCE);
    types.add(UnsignedLongArray.INSTANCE);
    types.add(UnsignedSmallint.INSTANCE);
    types.add(UnsignedSmallintArray.INSTANCE);
    types.add(UnsignedTime.INSTANCE);
    types.add(UnsignedTimeArray.INSTANCE);
    types.add(UnsignedTimestamp.INSTANCE);
    types.add(UnsignedTimestampArray.INSTANCE);
    types.add(UnsignedTinyint.INSTANCE);
    types.add(UnsignedTinyintArray.INSTANCE);
    types.add(Varbinary.INSTANCE);
    types.add(VarbinaryArray.INSTANCE);
    types.add(Varchar.INSTANCE);
    types.add(VarcharArray.INSTANCE);

    classToInstance = new HashMap<>(types.size());
    for (PDataType t : types) {
      classToInstance.put(t.getClass(), t);
    }
    orderedTypes = types.toArray(new PDataType[types.size()]);
  }

  public Set<PDataType> getTypes() {
    return types;
  }

  public PDataType[] getOrderedTypes() {
    return orderedTypes;
  }

  public PDataType instanceFromClass(Class<? extends PDataType> clazz) {
    return classToInstance.get(clazz);
  }
}
