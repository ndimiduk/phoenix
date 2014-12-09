/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.pig.util;

import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.phoenix.pig.writable.PhoenixPigDBWritable;
import org.apache.phoenix.schema.Binary;
import org.apache.phoenix.schema.Char;
import org.apache.phoenix.schema.Decimal;
import org.apache.phoenix.schema.PBoolean;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PDate;
import org.apache.phoenix.schema.PDouble;
import org.apache.phoenix.schema.PFloat;
import org.apache.phoenix.schema.PInteger;
import org.apache.phoenix.schema.PLong;
import org.apache.phoenix.schema.PTime;
import org.apache.phoenix.schema.PTimestamp;
import org.apache.phoenix.schema.Smallint;
import org.apache.phoenix.schema.Tinyint;
import org.apache.phoenix.schema.UnsignedDate;
import org.apache.phoenix.schema.UnsignedDouble;
import org.apache.phoenix.schema.UnsignedFloat;
import org.apache.phoenix.schema.UnsignedInt;
import org.apache.phoenix.schema.UnsignedLong;
import org.apache.phoenix.schema.UnsignedSmallint;
import org.apache.phoenix.schema.UnsignedTime;
import org.apache.phoenix.schema.UnsignedTimestamp;
import org.apache.phoenix.schema.UnsignedTinyint;
import org.apache.phoenix.schema.Varbinary;
import org.apache.phoenix.schema.Varchar;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public final class TypeUtil {
	
    private static final Log LOG = LogFactory.getLog(TypeUtil.class);
    private static final HBaseBinaryConverter binaryConverter = new HBaseBinaryConverter ();
	private static final ImmutableMap<PDataType,Byte> phoenixTypeToPigDataType = init();
	
	private TypeUtil(){
	}
	
	/**
	 * A map of Phoenix to Pig data types.
	 * @return
	 */
	private static ImmutableMap<PDataType, Byte> init() {
        final ImmutableMap.Builder<PDataType,Byte> builder = new Builder<PDataType,Byte> ();
        builder.put(PLong.INSTANCE,DataType.LONG);
        builder.put(Varbinary.INSTANCE,DataType.BYTEARRAY);
        builder.put(Char.INSTANCE,DataType.CHARARRAY);
        builder.put(Varchar.INSTANCE,DataType.CHARARRAY);
        builder.put(PDouble.INSTANCE,DataType.DOUBLE);
        builder.put(PFloat.INSTANCE,DataType.FLOAT);
        builder.put(PInteger.INSTANCE,DataType.INTEGER);
        builder.put(Tinyint.INSTANCE,DataType.INTEGER);
        builder.put(Smallint.INSTANCE,DataType.INTEGER);
        builder.put(Decimal.INSTANCE,DataType.BIGDECIMAL);
        builder.put(PTime.INSTANCE,DataType.DATETIME);
        builder.put(PTimestamp.INSTANCE,DataType.DATETIME);
        builder.put(PBoolean.INSTANCE,DataType.BOOLEAN);
        builder.put(PDate.INSTANCE,DataType.DATETIME);
        builder.put(UnsignedDate.INSTANCE,DataType.DATETIME);
        builder.put(UnsignedDouble.INSTANCE,DataType.DOUBLE);
        builder.put(UnsignedFloat.INSTANCE,DataType.FLOAT);
        builder.put(UnsignedInt.INSTANCE,DataType.INTEGER);
        builder.put(UnsignedLong.INSTANCE,DataType.LONG);
        builder.put(UnsignedSmallint.INSTANCE,DataType.INTEGER);
        builder.put(UnsignedTime.INSTANCE,DataType.DATETIME);
        builder.put(UnsignedTimestamp.INSTANCE,DataType.DATETIME);
        builder.put(UnsignedTinyint.INSTANCE,DataType.INTEGER);
        return builder.build();
    }
    /**
	 * This method returns the most appropriate PDataType associated with 
	 * the incoming Pig type. Note for Pig DataType DATETIME, returns DATE as 
	 * inferredSqlType. 
	 * 
	 * This is later used to make a cast to targetPhoenixType accordingly. See
	 * {@link #castPigTypeToPhoenix(Object, byte, PDataType)}
	 * 
	 * @param obj
	 * @return PDataType
	 */
	public static PDataType getType(Object obj, byte type) {
		if (obj == null) {
			return null;
		}
		PDataType sqlType;

		switch (type) {
		case DataType.BYTEARRAY:
			sqlType = Varbinary.INSTANCE;
			break;
		case DataType.CHARARRAY:
			sqlType = Varchar.INSTANCE;
			break;
		case DataType.DOUBLE:
		case DataType.BIGDECIMAL:
			sqlType = PDouble.INSTANCE;
			break;
		case DataType.FLOAT:
			sqlType = PFloat.INSTANCE;
			break;
		case DataType.INTEGER:
			sqlType = PInteger.INSTANCE;
			break;
		case DataType.LONG:
		case DataType.BIGINTEGER:
			sqlType = PLong.INSTANCE;
			break;
		case DataType.BOOLEAN:
			sqlType = PBoolean.INSTANCE;
			break;
		case DataType.DATETIME:
			sqlType = PDate.INSTANCE;
			break;
		case DataType.BYTE:
			sqlType = Tinyint.INSTANCE;
			break;
		default:
			throw new RuntimeException("Unknown type " + obj.getClass().getName()
					+ " passed to PhoenixHBaseStorage");
		}

		return sqlType;

	}

	/**
	 * This method encodes a value with Phoenix data type. It begins
	 * with checking whether an object is BINARY and makes a call to
	 * {@link #castBytes(Object, PDataType)} to convery bytes to
	 * targetPhoenixType
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return Object
	 */
	public static Object castPigTypeToPhoenix(Object o, byte objectType, PDataType targetPhoenixType) {
		PDataType inferredPType = getType(o, objectType);
		
		if(inferredPType == null) {
			return null;
		}

		if(inferredPType == Varbinary.INSTANCE) {
			try {
				o = castBytes(o, targetPhoenixType);
				if(targetPhoenixType != Varbinary.INSTANCE && targetPhoenixType != Binary.INSTANCE) {
					inferredPType = getType(o, DataType.findType(o));	
				}
			} catch (IOException e) {
				throw new RuntimeException("Error while casting bytes for object " +o);
			}
		}
		if(inferredPType == PDate.INSTANCE) {
			int inferredSqlType = targetPhoenixType.getSqlType();

			if(inferredSqlType == Types.DATE) {
				return new Date(((DateTime)o).getMillis());
			} 
			if(inferredSqlType == Types.TIME) {
				return new Time(((DateTime)o).getMillis());
			}
			if(inferredSqlType == Types.TIMESTAMP) {
				return new Timestamp(((DateTime)o).getMillis());
			}
		}
		
		if (targetPhoenixType == inferredPType || inferredPType.isCoercibleTo(targetPhoenixType)) {
			return inferredPType.toObject(o, targetPhoenixType);
		}
		
		throw new RuntimeException(o.getClass().getName()
				+ " cannot be coerced to "+targetPhoenixType.toString());
	}
	
	/**
	 * This method converts bytes to the target type required
	 * for Phoenix. It uses {@link Utf8StorageConverter} for
	 * the conversion.
	 * 
	 * @param o
	 * @param targetPhoenixType
	 * @return Object
	 * @throws IOException
	 */
	private static Object castBytes(Object o, PDataType targetPhoenixType) throws IOException {
        byte[] bytes = ((DataByteArray)o).get();

        if (PDataType.equalsAny(targetPhoenixType, Char.INSTANCE, Varchar.INSTANCE)) {
            return binaryConverter.bytesToCharArray(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, UnsignedSmallint.INSTANCE, Smallint.INSTANCE)) {
            return binaryConverter.bytesToInteger(bytes).shortValue();
        } else if (PDataType.equalsAny(targetPhoenixType, UnsignedTinyint.INSTANCE, Tinyint.INSTANCE)) {
            return binaryConverter.bytesToInteger(bytes).byteValue();
        } else if (PDataType.equalsAny(targetPhoenixType, UnsignedInt.INSTANCE, PInteger.INSTANCE)) {
            return binaryConverter.bytesToInteger(bytes);
        } else if (targetPhoenixType.equals(PBoolean.INSTANCE)) {
            return binaryConverter.bytesToBoolean(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PFloat.INSTANCE, UnsignedFloat.INSTANCE)) {
            return binaryConverter.bytesToFloat(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, PDouble.INSTANCE, UnsignedDouble.INSTANCE)) {
            return binaryConverter.bytesToDouble(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, UnsignedLong.INSTANCE, PLong.INSTANCE)) {
            return binaryConverter.bytesToLong(bytes);
        } else if (PDataType.equalsAny(targetPhoenixType, Varbinary.INSTANCE, Binary.INSTANCE)) {
            return bytes;
        } else {
            return o;
        }        
    }
    
    /**
     * Transforms the PhoenixRecord to Pig {@link Tuple}.
     * @param record
     * @param projectedColumns
     * @return
     * @throws IOException
     */
    public static Tuple transformToTuple(final PhoenixPigDBWritable record, final ResourceFieldSchema[] projectedColumns) throws IOException {
        
        List<Object> columnValues = record.getValues();
        if(columnValues == null || columnValues.size() == 0 || projectedColumns == null || projectedColumns.length != columnValues.size()) {
            return null;
        }
        int columns = columnValues.size();
        Tuple tuple = TupleFactory.getInstance().newTuple(columns);
        try {
            for(int i = 0 ; i < columns ; i++) {
                final ResourceFieldSchema fieldSchema = projectedColumns[i];
                Object object = columnValues.get(i);
                if (object == null) {
                    tuple.set(i, null);
                    continue;
                }
                
                switch(fieldSchema.getType()) {
                    case DataType.BYTEARRAY:
                        byte[] bytes = PDataType.fromTypeId(Binary.INSTANCE.getSqlType()).toBytes(object);
                        tuple.set(i,new DataByteArray(bytes,0,bytes.length));
                        break;
                    case DataType.CHARARRAY:
                        tuple.set(i,DataType.toString(object));
                        break;
                    case DataType.DOUBLE:
                        tuple.set(i,DataType.toDouble(object));
                        break;
                    case DataType.FLOAT:
                        tuple.set(i,DataType.toFloat(object));
                        break;
                    case DataType.INTEGER:
                        tuple.set(i,DataType.toInteger(object));
                        break;
                    case DataType.LONG:
                        tuple.set(i,DataType.toLong(object));
                        break;
                    case DataType.BOOLEAN:
                        tuple.set(i,DataType.toBoolean(object));
                        break;
                    case DataType.DATETIME:
                        tuple.set(i,DataType.toDateTime(object));
                        break;
                    default:
                        throw new RuntimeException(String.format(" Not supported [%s] pig type" , fieldSchema));
                }
            }
        } catch( Exception ex) {
            final String errorMsg = String.format(" Error transforming PhoenixRecord to Tuple [%s] ", ex.getMessage());
            LOG.error(errorMsg);
            throw new PigException(errorMsg);
        }
          return tuple;
    }
    
    /**
     * Returns the mapping pig data type for a given phoenix data type.
     * @param phoenixDataType
     * @return
     */
    public static Byte getPigDataTypeForPhoenixType(final PDataType phoenixDataType) {
        Preconditions.checkNotNull(phoenixDataType);
        final Byte pigDataType = phoenixTypeToPigDataType.get(phoenixDataType);
        if(LOG.isDebugEnabled()) {
            LOG.debug(String.format(" For PhoenixDataType [%s] , pigDataType is [%s] " , phoenixDataType.getSqlTypeName() , DataType.findTypeName(pigDataType)));    
        }
        return pigDataType;
    }

}
