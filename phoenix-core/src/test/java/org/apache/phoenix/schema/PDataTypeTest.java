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
package org.apache.phoenix.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.exception.SQLExceptionCode;
import org.apache.phoenix.util.TestUtil;
import org.junit.Test;


public class PDataTypeTest {
    @Test
    public void testFloatToLongComparison() {
        // Basic tests
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(1e100), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(0.001), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        // Edge tests
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 1.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0); // Passes due to rounding
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 129.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 128.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 129.0F), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        float f1 = 9111111111111111.0F;
        float f2 = 9111111111111112.0F;
        assertTrue(f1 == f2);
        long la = 9111111111111111L;
        assertTrue(f1 > Integer.MAX_VALUE);
        assertTrue(la == f1);
        assertTrue(la == f2);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(f1), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PFloat.INSTANCE.compareTo(PFloat.INSTANCE.toBytes(f2), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);

        // Same as above, but reversing LHS and RHS
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(1e100), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(0.001), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) > 0);

        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MAX_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0,
                PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) > 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 1.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) < 0); // Passes due to rounding
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MAX_VALUE + 129.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 128.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Integer.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PFloat.INSTANCE.toBytes(Integer.MIN_VALUE - 129.0F), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE) > 0);

        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(f1), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(la), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PFloat.INSTANCE.toBytes(f2), 0, PFloat.INSTANCE.getByteSize(), SortOrder.getDefault(), PFloat.INSTANCE) == 0);
    }        
        
    @Test
    public void testDoubleToDecimalComparison() {
        // Basic tests
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(1.23), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                   Decimal.INSTANCE.toBytes(BigDecimal.valueOf(1.24)), 0, Decimal.INSTANCE.getByteSize(), SortOrder.getDefault(), Decimal.INSTANCE) < 0);
    }
    
    @Test
    public void testDoubleToLongComparison() {
        // Basic tests
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(-1e100), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(0.001), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1024.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1025.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) > 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1024.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) == 0);
        assertTrue(PDouble.INSTANCE.compareTo(PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1025.0), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PLong.INSTANCE) < 0);

        // Same as above, but reversing LHS and RHS
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE.toBytes(-1e100), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) > 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(1), 0, PLong.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE.toBytes(0.001), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) > 0);

        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MAX_VALUE - 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MAX_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE + 1), 0,
                PLong.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0,
                PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(), PDouble.INSTANCE) > 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1024.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MAX_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MAX_VALUE + 1025.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) < 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1024.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) == 0);
        assertTrue(PLong.INSTANCE.compareTo(PLong.INSTANCE.toBytes(Long.MIN_VALUE), 0, PLong.INSTANCE.getByteSize(),
        		SortOrder.getDefault(), PDouble.INSTANCE.toBytes(Long.MIN_VALUE - 1025.0), 0, PDouble.INSTANCE.getByteSize(), SortOrder.getDefault(),
                PDouble.INSTANCE) > 0);

        long i = 10;
        long maxl = (1L << 62);
        try {
            for (; i < 100; i++) {
                double d = Math.pow(2, i);
                if ((long)d > maxl) {
                    assertTrue(i > 62);
                    continue;
                }
                long l = (1L << i) - 1;
                assertTrue(l + 1L == (long)d);
                assertTrue(l < (long)d);
            }
        } catch (AssertionError t) {
            throw t;
        }
        double d = 0.0;
        try {
            while (d <= 1024) {
                double d1 = Long.MAX_VALUE;
                double d2 = Long.MAX_VALUE + d;
                assertTrue(d2 == d1);
                d++;
            }
        } catch (AssertionError t) {
            throw t;
        }
        d = 0.0;
        try {
            while (d >= -1024) {
                double d1 = Long.MIN_VALUE;
                double d2 = Long.MIN_VALUE + d;
                assertTrue(d2 == d1);
                d--;
            }
        } catch (AssertionError t) {
            throw t;
        }
        double d1 = Long.MAX_VALUE;
        double d2 = Long.MAX_VALUE + 1024.0;
        double d3 = Long.MAX_VALUE + 1025.0;
        assertTrue(d1 == d2);
        assertTrue(d3 > d1);
        long l1 = Long.MAX_VALUE - 1;
        assertTrue((long)d1 > l1);
    }
        
    @Test
    public void testLong() {
        Long la = 4L;
        byte[] b = PLong.INSTANCE.toBytes(la);
        Long lb = (Long) PLong.INSTANCE.toObject(b);
        assertEquals(la,lb);

        Long na = 1L;
        Long nb = -1L;
        byte[] ba = PLong.INSTANCE.toBytes(na);
        byte[] bb = PLong.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = PLong.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Long);
        assertEquals(100, ((Long)obj).longValue());
        
        Long longValue = 100l;
        Object longObj = PLong.INSTANCE.toObject(longValue, PLong.INSTANCE);
        assertTrue(longObj instanceof Long);
        assertEquals(100, ((Long)longObj).longValue());
        
        assertEquals(0, PLong.INSTANCE.compareTo(Long.MAX_VALUE, Float.valueOf(Long.MAX_VALUE), PFloat.INSTANCE));
        assertEquals(0, PLong.INSTANCE.compareTo(Long.MAX_VALUE, Double.valueOf(Long.MAX_VALUE), PDouble.INSTANCE));
        assertEquals(-1, PLong.INSTANCE.compareTo(99, Float.valueOf(100), PFloat.INSTANCE));
        assertEquals(1, PLong.INSTANCE.compareTo(101, Float.valueOf(100), PFloat.INSTANCE));
        
        Double d = -2.0;
        Object lo = PLong.INSTANCE.toObject(d, PDouble.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        byte[] bytes = PDouble.INSTANCE.toBytes(d);
        lo = PLong.INSTANCE.toObject(bytes,0, bytes.length, PDouble.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        Float f = -2.0f;
        lo = PLong.INSTANCE.toObject(f, PFloat.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        bytes = PFloat.INSTANCE.toBytes(f);
        lo = PLong.INSTANCE.toObject(bytes,0, bytes.length, PFloat.INSTANCE);
        assertEquals(-2L, ((Long)lo).longValue());
        
        // Checks for unsignedlong
        d = 2.0;
        lo = UnsignedLong.INSTANCE.toObject(d, PDouble.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
        bytes = PDouble.INSTANCE.toBytes(d);
        lo = UnsignedLong.INSTANCE.toObject(bytes,0, bytes.length, PDouble.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
        f = 2.0f;
        lo = UnsignedLong.INSTANCE.toObject(f, PFloat.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
        bytes = PFloat.INSTANCE.toBytes(f);
        lo = UnsignedLong.INSTANCE.toObject(bytes,0, bytes.length, PFloat.INSTANCE);
        assertEquals(2L, ((Long)lo).longValue());
        
    }

    @Test
    public void testInt() {
        Integer na = 4;
        byte[] b = PInteger.INSTANCE.toBytes(na);
        Integer nb = (Integer) PInteger.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = PInteger.INSTANCE.toBytes(na);
        byte[] bb = PInteger.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = PInteger.INSTANCE.toBytes(na);
        bb = PInteger.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100000000;
        ba = PInteger.INSTANCE.toBytes(na);
        bb = PInteger.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Long value = 100l;
        Object obj = PInteger.INSTANCE.toObject(value, PLong.INSTANCE);
        assertTrue(obj instanceof Integer);
        assertEquals(100, ((Integer)obj).intValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = PInteger.INSTANCE.toObject(unsignedFloatValue, UnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Integer);
        assertEquals(100, ((Integer)unsignedFloatObj).intValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = PInteger.INSTANCE.toObject(unsignedDoubleValue, UnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Integer);
        assertEquals(100, ((Integer)unsignedDoubleObj).intValue());
        
        Float floatValue = 100f;
        Object floatObj = PInteger.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Integer);
        assertEquals(100, ((Integer)floatObj).intValue());
        
        Double doubleValue = 100d;
        Object doubleObj = PInteger.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Integer);
        assertEquals(100, ((Integer)doubleObj).intValue());
        
        Short shortValue = 100;
        Object shortObj = PInteger.INSTANCE.toObject(shortValue, Smallint.INSTANCE);
        assertTrue(shortObj instanceof Integer);
        assertEquals(100, ((Integer)shortObj).intValue());
    }
    
    @Test
    public void testSmallInt() {
        Short na = 4;
        byte[] b = Smallint.INSTANCE.toBytes(na);
        Short nb = (Short)Smallint.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 4;
        b = Smallint.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = Smallint.INSTANCE.getCodec().decodeShort(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = Smallint.INSTANCE.toBytes(na);
        byte[] bb = Smallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = Smallint.INSTANCE.toBytes(na);
        bb = Smallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -10000;
        ba = Smallint.INSTANCE.toBytes(na);
        bb = Smallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = Smallint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Short);
        assertEquals(100, ((Short)obj).shortValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = Smallint.INSTANCE.toObject(unsignedFloatValue, UnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Short);
        assertEquals(100, ((Short)unsignedFloatObj).shortValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = Smallint.INSTANCE.toObject(unsignedDoubleValue, UnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Short);
        assertEquals(100, ((Short)unsignedDoubleObj).shortValue());
        
        Float floatValue = 100f;
        Object floatObj = Smallint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Short);
        assertEquals(100, ((Short)floatObj).shortValue());
        
        Double doubleValue = 100d;
        Object doubleObj = Smallint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Short);
        assertEquals(100, ((Short)doubleObj).shortValue());
    }
    
    @Test
    public void testTinyInt() {
        Byte na = 4;
        byte[] b = Tinyint.INSTANCE.toBytes(na);
        Byte nb = (Byte)Tinyint.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 1;
        nb = -1;
        byte[] ba = Tinyint.INSTANCE.toBytes(na);
        byte[] bb = Tinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1;
        nb = -3;
        ba = Tinyint.INSTANCE.toBytes(na);
        bb = Tinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -3;
        nb = -100;
        ba = Tinyint.INSTANCE.toBytes(na);
        bb = Tinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = Tinyint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Byte);
        assertEquals(100, ((Byte)obj).byteValue());
        
        Float floatValue = 100f;
        Object floatObj = Tinyint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Byte);
        assertEquals(100, ((Byte)floatObj).byteValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = Tinyint.INSTANCE.toObject(unsignedFloatValue, UnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedFloatObj).byteValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = Tinyint.INSTANCE.toObject(unsignedDoubleValue, UnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedDoubleObj).byteValue());
        
        Double doubleValue = 100d;
        Object doubleObj = Tinyint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Byte);
        assertEquals(100, ((Byte)doubleObj).byteValue());
    }
    
    @Test
    public void testUnsignedSmallInt() {
        Short na = 4;
        byte[] b = UnsignedSmallint.INSTANCE.toBytes(na);
        Short nb = (Short)UnsignedSmallint.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 10;
        nb = 8;
        byte[] ba = UnsignedSmallint.INSTANCE.toBytes(na);
        byte[] bb = UnsignedSmallint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = UnsignedSmallint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Short);
        assertEquals(100, ((Short)obj).shortValue());
        
        Float floatValue = 100f;
        Object floatObj = UnsignedSmallint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Short);
        assertEquals(100, ((Short)floatObj).shortValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = UnsignedSmallint.INSTANCE.toObject(unsignedFloatValue, UnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Short);
        assertEquals(100, ((Short)unsignedFloatObj).shortValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = UnsignedSmallint.INSTANCE.toObject(unsignedDoubleValue, UnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Short);
        assertEquals(100, ((Short)unsignedDoubleObj).shortValue());
        
        Double doubleValue = 100d;
        Object doubleObj = UnsignedSmallint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Short);
        assertEquals(100, ((Short)doubleObj).shortValue());
    }
    
    @Test
    public void testUnsignedTinyInt() {
        Byte na = 4;
        byte[] b = UnsignedTinyint.INSTANCE.toBytes(na);
        Byte nb = (Byte)UnsignedTinyint.INSTANCE.toObject(b);
        assertEquals(na,nb);

        na = 10;
        nb = 8;
        byte[] ba = UnsignedTinyint.INSTANCE.toBytes(na);
        byte[] bb = UnsignedTinyint.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        Integer value = 100;
        Object obj = UnsignedTinyint.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Byte);
        assertEquals(100, ((Byte)obj).byteValue());
        
        Float floatValue = 100f;
        Object floatObj = UnsignedTinyint.INSTANCE.toObject(floatValue, PFloat.INSTANCE);
        assertTrue(floatObj instanceof Byte);
        assertEquals(100, ((Byte)floatObj).byteValue());
        
        Float unsignedFloatValue = 100f;
        Object unsignedFloatObj = UnsignedTinyint.INSTANCE.toObject(unsignedFloatValue, UnsignedFloat.INSTANCE);
        assertTrue(unsignedFloatObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedFloatObj).byteValue());
        
        Double unsignedDoubleValue = 100d;
        Object unsignedDoubleObj = UnsignedTinyint.INSTANCE.toObject(unsignedDoubleValue, UnsignedDouble.INSTANCE);
        assertTrue(unsignedDoubleObj instanceof Byte);
        assertEquals(100, ((Byte)unsignedDoubleObj).byteValue());
        
        Double doubleValue = 100d;
        Object doubleObj = UnsignedTinyint.INSTANCE.toObject(doubleValue, PDouble.INSTANCE);
        assertTrue(doubleObj instanceof Byte);
        assertEquals(100, ((Byte)doubleObj).byteValue());
    }
    
    @Test
    public void testUnsignedFloat() {
        Float na = 0.005f;
        byte[] b = UnsignedFloat.INSTANCE.toBytes(na);
        Float nb = (Float)UnsignedFloat.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0f;
        b = UnsignedFloat.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = UnsignedFloat.INSTANCE.getCodec().decodeFloat(ptr, SortOrder.DESC);
        assertEquals(na,nb);
        
        na = 2.0f;
        nb = 1.0f;
        byte[] ba = UnsignedFloat.INSTANCE.toBytes(na);
        byte[] bb = UnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = 0.0f;
        nb = Float.MIN_VALUE;
        ba = UnsignedFloat.INSTANCE.toBytes(na);
        bb = UnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MIN_VALUE;
        nb = Float.MAX_VALUE;
        ba = UnsignedFloat.INSTANCE.toBytes(na);
        bb = UnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MAX_VALUE;
        nb = Float.POSITIVE_INFINITY;
        ba = UnsignedFloat.INSTANCE.toBytes(na);
        bb = UnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.POSITIVE_INFINITY;
        nb = Float.NaN;
        ba = UnsignedFloat.INSTANCE.toBytes(na);
        bb = UnsignedFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = UnsignedFloat.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Float);
    }
    
    @Test
    public void testUnsignedDouble() {
        Double na = 0.005;
        byte[] b = UnsignedDouble.INSTANCE.toBytes(na);
        Double nb = (Double)UnsignedDouble.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0;
        b = UnsignedDouble.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = UnsignedDouble.INSTANCE.getCodec().decodeDouble(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 2.0;
        nb = 1.0;
        byte[] ba = UnsignedDouble.INSTANCE.toBytes(na);
        byte[] bb = UnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = 0.0;
        nb = Double.MIN_VALUE;
        ba = UnsignedDouble.INSTANCE.toBytes(na);
        bb = UnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MIN_VALUE;
        nb = Double.MAX_VALUE;
        ba = UnsignedDouble.INSTANCE.toBytes(na);
        bb = UnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MAX_VALUE;
        nb = Double.POSITIVE_INFINITY;
        ba = UnsignedDouble.INSTANCE.toBytes(na);
        bb = UnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.POSITIVE_INFINITY;
        nb = Double.NaN;
        ba = UnsignedDouble.INSTANCE.toBytes(na);
        bb = UnsignedDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = UnsignedDouble.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Double);
        
        assertEquals(1, UnsignedDouble.INSTANCE.compareTo(Double.valueOf(101), Long.valueOf(100), PLong.INSTANCE));
        assertEquals(0, UnsignedDouble.INSTANCE.compareTo(Double.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, PLong.INSTANCE));
        assertEquals(-1, UnsignedDouble.INSTANCE.compareTo(Double.valueOf(1), Long.valueOf(100), PLong.INSTANCE));
        
        assertEquals(0, UnsignedDouble.INSTANCE.compareTo(Double.valueOf(101), BigDecimal.valueOf(101.0), Decimal.INSTANCE));
    }
    
    @Test
    public void testFloat() {
        Float na = 0.005f;
        byte[] b = PFloat.INSTANCE.toBytes(na);
        Float nb = (Float) PFloat.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0f;
        b = PFloat.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PFloat.INSTANCE.getCodec().decodeFloat(ptr, SortOrder.DESC);
        assertEquals(na,nb);
        
        na = 1.0f;
        nb = -1.0f;
        byte[] ba = PFloat.INSTANCE.toBytes(na);
        byte[] bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1f;
        nb = -3f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = Float.NEGATIVE_INFINITY;
        nb = -Float.MAX_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Float.MAX_VALUE;
        nb = -Float.MIN_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Float.MIN_VALUE;
        nb = -0.0f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -0.0f;
        nb = 0.0f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = 0.0f;
        nb = Float.MIN_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MIN_VALUE;
        nb = Float.MAX_VALUE;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.MAX_VALUE;
        nb = Float.POSITIVE_INFINITY;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Float.POSITIVE_INFINITY;
        nb = Float.NaN;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PFloat.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Float);
        
        Double dvalue = Double.NEGATIVE_INFINITY;
        obj = PFloat.INSTANCE.toObject(dvalue, PDouble.INSTANCE);
        assertTrue(obj instanceof Float);
        assertEquals(Float.NEGATIVE_INFINITY, obj);
        
        na = 1.0f;
        nb = -1.0f;
        ba = PFloat.INSTANCE.toBytes(na);
        bb = PFloat.INSTANCE.toBytes(nb);
        float nna = PFloat.INSTANCE.getCodec().decodeFloat(ba, 0, SortOrder.DESC);
        float nnb = PFloat.INSTANCE.getCodec().decodeFloat(bb, 0, SortOrder.DESC);
        assertTrue(Float.compare(nna, nnb) < 0);
    }
    
    @Test
    public void testDouble() {
        Double na = 0.005;
        byte[] b = PDouble.INSTANCE.toBytes(na);
        Double nb = (Double) PDouble.INSTANCE.toObject(b);
        assertEquals(na,nb);
        
        na = 10.0;
        b = PDouble.INSTANCE.toBytes(na, SortOrder.DESC);
        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        ptr.set(b);
        nb = PDouble.INSTANCE.getCodec().decodeDouble(ptr, SortOrder.DESC);
        assertEquals(na,nb);

        na = 1.0;
        nb = -1.0;
        byte[] ba = PDouble.INSTANCE.toBytes(na);
        byte[] bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);

        na = -1.0;
        nb = -3.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        
        na = Double.NEGATIVE_INFINITY;
        nb = -Double.MAX_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Double.MAX_VALUE;
        nb = -Double.MIN_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -Double.MIN_VALUE;
        nb = -0.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = -0.0;
        nb = 0.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = 0.0;
        nb = Double.MIN_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MIN_VALUE;
        nb = Double.MAX_VALUE;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.MAX_VALUE;
        nb = Double.POSITIVE_INFINITY;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        na = Double.POSITIVE_INFINITY;
        nb = Double.NaN;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) < 0);
        
        Integer value = 100;
        Object obj = PDouble.INSTANCE.toObject(value, PInteger.INSTANCE);
        assertTrue(obj instanceof Double);
        
        na = 1.0;
        nb = -1.0;
        ba = PDouble.INSTANCE.toBytes(na);
        bb = PDouble.INSTANCE.toBytes(nb);
        double nna = PDouble.INSTANCE.getCodec().decodeDouble(ba, 0, SortOrder.DESC);
        double nnb = PDouble.INSTANCE.getCodec().decodeDouble(bb, 0, SortOrder.DESC);
        assertTrue(Double.compare(nna, nnb) < 0);
        
        assertEquals(1, PDouble.INSTANCE.compareTo(Double.valueOf(101), Long.valueOf(100), PLong.INSTANCE));
        assertEquals(0, PDouble.INSTANCE.compareTo(Double.valueOf(Long.MAX_VALUE), Long.MAX_VALUE, PLong.INSTANCE));
        assertEquals(-1, PDouble.INSTANCE.compareTo(Double.valueOf(1), Long.valueOf(100), PLong.INSTANCE));
        
        assertEquals(0, PDouble.INSTANCE.compareTo(Double.valueOf(101), BigDecimal.valueOf(101.0), Decimal.INSTANCE));
    }

    @Test
    public void testBigDecimal() {
        byte[] b;
        BigDecimal na, nb;

        b = new byte[] {
                (byte)0xc2,0x02,0x10,0x36,0x22,0x22,0x22,0x22,0x22,0x22,0x0f,0x27,0x38,0x1c,0x05,0x40,0x62,0x21,0x54,0x4d,0x4e,0x01,0x14,0x36,0x0d,0x33
        };
        BigDecimal decodedBytes = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(decodedBytes.compareTo(BigDecimal.ZERO) > 0);

        na = new BigDecimal(new BigInteger("12345678901239998123456789"), 2);
        //[-52, 13, 35, 57, 79, 91, 13, 40, 100, 82, 24, 46, 68, 90]
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal("115.533333333333331438552704639732837677001953125");
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));
        
        // test for negative serialization using biginteger
        na = new BigDecimal("-5.00000000000000000000000001");
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));
        
        // test for serialization of 38 digits
        na = new BigDecimal("-2.4999999999999999999999999999999999999");
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));
        
        // test for serialization of 39 digits, should round to -2.5
        na = new BigDecimal("-2.499999999999999999999999999999999999999");
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(nb.compareTo(new BigDecimal("-2.5")) == 0);
        assertEquals(new BigDecimal("-2.5"), nb);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal(2.5);
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(Double.parseDouble("96.45238095238095"));
        String naStr = na.toString();
        assertTrue(naStr != null);
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        TestUtil.assertRoundEquals(na,nb);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        // If we don't remove trailing zeros, this fails
        na = new BigDecimal(-1000);
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal("1000.5829999999999913");
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        na = TestUtil.computeAverage(11000, 3);
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal(new BigInteger("12345678901239999"), 2);
        b = Decimal.INSTANCE.toBytes(na);
        nb = (BigDecimal)Decimal.INSTANCE.toObject(b);
        assertTrue(na.compareTo(nb) == 0);
        assertTrue(b.length <= Decimal.INSTANCE.estimateByteSize(na));

        na = new BigDecimal(1);
        nb = new BigDecimal(-1);
        byte[] ba = Decimal.INSTANCE.toBytes(na);
        byte[] bb = Decimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= Decimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= Decimal.INSTANCE.estimateByteSize(nb));

        na = new BigDecimal(-1);
        nb = new BigDecimal(-2);
        ba = Decimal.INSTANCE.toBytes(na);
        bb = Decimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= Decimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= Decimal.INSTANCE.estimateByteSize(nb));

        na = new BigDecimal(-3);
        nb = new BigDecimal(-1000);
        assertTrue(na.compareTo(nb) > 0);
        ba = Decimal.INSTANCE.toBytes(na);
        bb = Decimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= Decimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= Decimal.INSTANCE.estimateByteSize(nb));

        na = new BigDecimal(BigInteger.valueOf(12345678901239998L), 2);
        nb = new BigDecimal(97);
        assertTrue(na.compareTo(nb) > 0);
        ba = Decimal.INSTANCE.toBytes(na);
        bb = Decimal.INSTANCE.toBytes(nb);
        assertTrue(Bytes.compareTo(ba, bb) > 0);
        assertTrue(ba.length <= Decimal.INSTANCE.estimateByteSize(na));
        assertTrue(bb.length <= Decimal.INSTANCE.estimateByteSize(nb));

        List<BigDecimal> values = Arrays.asList(new BigDecimal[] {
            new BigDecimal(-1000),
            new BigDecimal(-100000000),
            new BigDecimal(1000),
            new BigDecimal("-0.001"),
            new BigDecimal("0.001"),
            new BigDecimal(new BigInteger("12345678901239999"), 2),
            new BigDecimal(new BigInteger("12345678901239998"), 2),
            new BigDecimal(new BigInteger("12345678901239998123456789"), 2), // bigger than long
            new BigDecimal(new BigInteger("-1000"),3),
            new BigDecimal(new BigInteger("-1000"),10),
            new BigDecimal(99),
            new BigDecimal(97),
            new BigDecimal(-3)
        });

        List<byte[]> byteValues = new ArrayList<byte[]>();
        for (int i = 0; i < values.size(); i++) {
            byteValues.add(Decimal.INSTANCE.toBytes(values.get(i)));
        }

        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            BigDecimal actual = (BigDecimal)Decimal.INSTANCE.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(PDataType.DEFAULT_MATH_CONTEXT)) == 0);
            assertTrue(byteValues.get(i).length <= Decimal.INSTANCE.estimateByteSize(expected));
        }

        Collections.sort(values);
        Collections.sort(byteValues, Bytes.BYTES_COMPARATOR);

        for (int i = 0; i < values.size(); i++) {
            BigDecimal expected = values.get(i);
            byte[] bytes = Decimal.INSTANCE.toBytes(values.get(i));
            assertNotNull("bytes converted from values should not be null!", bytes);
            BigDecimal actual = (BigDecimal)Decimal.INSTANCE.toObject(byteValues.get(i));
            assertTrue("For " + i + " expected " + expected + " but got " + actual,expected.round(PDataType.DEFAULT_MATH_CONTEXT).compareTo(actual.round(PDataType.DEFAULT_MATH_CONTEXT))==0);
        }


        {
            String[] strs ={
                    "\\xC2\\x03\\x0C\\x10\\x01\\x01\\x01\\x01\\x01\\x019U#\\x13W\\x09\\x09"
                    ,"\\xC2\\x03<,ddddddN\\x1B\\x1B!.9N"
                    ,"\\xC2\\x039"
                    ,"\\xC2\\x03\\x16,\\x01\\x01\\x01\\x01\\x01\\x01E\\x16\\x16\\x03@\\x1EG"
                    ,"\\xC2\\x02d6dddddd\\x15*]\\x0E<1F"
                    ,"\\xC2\\x04 3"
                    ,"\\xC2\\x03$Ldddddd\\x0A\\x06\\x06\\x1ES\\x1C\\x08"
                    ,"\\xC2\\x03\\x1E\\x0A\\x01\\x01\\x01\\x01\\x01\\x01#\\x0B=4 AV"
                    ,"\\xC2\\x02\\\\x04dddddd\\x15*]\\x0E<1F"
                    ,"\\xC2\\x02V\"\\x01\\x01\\x01\\x01\\x01\\x02\\x1A\\x068\\x162&O"
            };
            for (String str : strs) {
                byte[] bytes = Bytes.toBytesBinary(str);
                Object o = Decimal.INSTANCE.toObject(bytes);
                assertNotNull(o);
                //System.out.println(o.getClass() +" " + bytesToHex(bytes)+" " + o+" ");
            }
        }
    }
    public static String bytesToHex(byte[] bytes) {
        final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Test
    public void testEmptyString() throws Throwable {
        byte[] b1 = Varchar.INSTANCE.toBytes("");
        byte[] b2 = Varchar.INSTANCE.toBytes(null);
        assert (b1.length == 0 && Bytes.compareTo(b1, b2) == 0);
    }

    @Test
    public void testNull() throws Throwable {
        byte[] b = new byte[8];
        for (PDataType type : PDataType.values()) {
            try {
				type.toBytes(null);
				type.toBytes(null, b, 0);
				type.toObject(new byte[0], 0, 0);
				type.toObject(new byte[0], 0, 0, type);
                if (type.isArrayType()) {
					type.toBytes(new PhoenixArray());
					type.toBytes(new PhoenixArray(), b, 0);
                }
            } catch (ConstraintViolationException e) {
            	if (!type.isArrayType() && ! ( type.isFixedWidth() && e.getMessage().contains("may not be null"))) {
            		// Fixed width types do not support the concept of a "null" value.
                    fail(type + ":" + e);
                }
            }
        }
    }

    @Test
    public void testValueCoersion() throws Exception {
        // Testing coercing integer to other values.
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, Double.valueOf(Float.MAX_VALUE) + Double.valueOf(Float.MAX_VALUE)));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, Double.valueOf(Long.MAX_VALUE) + Double.valueOf(Long.MAX_VALUE)));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -100000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -1000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -100000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -1000.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 10.0));
        assertTrue(PDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 0.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, -10.0));
        assertFalse(PDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, Double.MAX_VALUE));
        
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, Float.valueOf(Long.MAX_VALUE) + Float.valueOf(Long.MAX_VALUE)));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -100000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -1000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -100000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -10.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -1000.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 10.0f));
        assertTrue(PFloat.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 0.0f));
        assertFalse(PFloat.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, -10.0f));
        
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(PFloat.INSTANCE, Double.MAX_VALUE));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(PLong.INSTANCE, Double.MAX_VALUE));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 10.0));
        assertTrue(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 0.0));
        assertFalse(UnsignedDouble.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, Double.MAX_VALUE));
        
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0.0f));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(PLong.INSTANCE, Float.MAX_VALUE));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 0.0f));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0.0f));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0.0f));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0.0f));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0.0f));
        assertFalse(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0.0f));
        assertTrue(UnsignedFloat.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        
        // Testing coercing integer to other values.
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(PLong.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -100000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -1000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -100000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -10));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -1000));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, -10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 0));
        assertFalse(PInteger.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, -10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, 10));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, 0));
        assertTrue(PInteger.INSTANCE.isCoercibleTo(Varbinary.INSTANCE, 0));

        // Testing coercing long to other values.
        assertTrue(PLong.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Long.MAX_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MAX_VALUE + 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (long)Integer.MAX_VALUE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MAX_VALUE - 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 0L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, -10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MIN_VALUE + 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (long)Integer.MIN_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Integer.MIN_VALUE - 10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, Long.MIN_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, -10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, Long.MAX_VALUE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, Long.MIN_VALUE));
        assertFalse(PLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE, -100000L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, -1000L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, -100000L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10L));
        assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -10L));
        assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, -1000L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, 10L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, 0L));
		assertFalse(PLong.INSTANCE
				.isCoercibleTo(UnsignedDouble.INSTANCE, -1L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 10L));
		assertTrue(PLong.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, 0L));
		assertFalse(PLong.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, -1L));
        
        // Testing coercing smallint to other values.
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)0));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)-10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)0));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)-10));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (short)0));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (short)-10));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (short)0));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (short)-10));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)0));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)-10));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)1000));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (short)0));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (short)-10));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)0));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)-10));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)1000));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, (short)0));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, (short)-1));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, (short)10));
        assertTrue(Smallint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, (short)0));
        assertFalse(Smallint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, (short)-1));
        
        // Testing coercing tinyint to other values.
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)0));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)-10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)0));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)-10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (byte)100));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (byte)0));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (byte)-10));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (byte)0));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (byte)-10));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (byte)0));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (byte)-10));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (byte)0));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (byte)-10));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (byte)0));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (byte)-10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, (byte)0));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE, (byte)-1));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, (byte)10));
        assertTrue(Tinyint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, (byte)0));
        assertFalse(Tinyint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE, (byte)-1));

        // Testing coercing unsigned_int to other values.
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 0));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PLong.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(PLong.INSTANCE, 0));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, 0));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 100000));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 1000));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        assertTrue(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE));

        // Testing coercing unsigned_long to other values.
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 10L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(PInteger.INSTANCE, 0L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 10L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(Smallint.INSTANCE, 0L));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 10L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, 0L));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 10L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 0L));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, 100000L));
        assertFalse(UnsignedInt.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 10L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 0L));
        assertFalse(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, 1000L));
        assertTrue(UnsignedLong.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        
        // Testing coercing unsigned_smallint to other values.
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (short)0));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (short)0));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (short)0));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (short)0));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (short)0));
        assertFalse(UnsignedSmallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)0));
        assertFalse(UnsignedSmallint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (short)1000));
        assertFalse(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)10));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)0));
        assertFalse(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedTinyint.INSTANCE, (short)1000));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        assertTrue(UnsignedSmallint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE));
        
        // Testing coercing unsigned_tinyint to other values.
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PDouble.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PFloat.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PInteger.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(PLong.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedLong.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedInt.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(Smallint.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(Tinyint.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (byte)10));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedSmallint.INSTANCE, (byte)0));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedDouble.INSTANCE));
        assertTrue(UnsignedTinyint.INSTANCE.isCoercibleTo(UnsignedFloat.INSTANCE));
        
        // Testing coercing Date types
        assertTrue(PDate.INSTANCE.isCoercibleTo(PTimestamp.INSTANCE));
        assertTrue(PDate.INSTANCE.isCoercibleTo(PTime.INSTANCE));
        assertFalse(PTimestamp.INSTANCE.isCoercibleTo(PDate.INSTANCE));
        assertFalse(PTimestamp.INSTANCE.isCoercibleTo(PTime.INSTANCE));
        assertTrue(PTime.INSTANCE.isCoercibleTo(PTimestamp.INSTANCE));
        assertTrue(PTime.INSTANCE.isCoercibleTo(PDate.INSTANCE));
    }

    @Test
    public void testGetDeicmalPrecisionAndScaleFromRawBytes() throws Exception {
        // Special case for 0.
        BigDecimal bd = new BigDecimal("0");
        byte[] b = Decimal.INSTANCE.toBytes(bd);
        int[] v = PDataType.getDecimalPrecisionAndScale(b, 0, b.length);
        assertEquals(0, v[0]);
        assertEquals(0, v[1]);

        BigDecimal[] bds = new BigDecimal[] {
                new BigDecimal("1"),
                new BigDecimal("0.11"),
                new BigDecimal("1.1"),
                new BigDecimal("11"),
                new BigDecimal("101"),
                new BigDecimal("10.1"),
                new BigDecimal("1.01"),
                new BigDecimal("0.101"),
                new BigDecimal("1001"),
                new BigDecimal("100.1"),
                new BigDecimal("10.01"),
                new BigDecimal("1.001"),
                new BigDecimal("0.1001"),
                new BigDecimal("10001"),
                new BigDecimal("1000.1"),
                new BigDecimal("100.01"),
                new BigDecimal("10.001"),
                new BigDecimal("1.0001"),
                new BigDecimal("0.10001"),
                new BigDecimal("100000000000000000000000000000"),
                new BigDecimal("1000000000000000000000000000000"),
                new BigDecimal("0.000000000000000000000000000001"),
                new BigDecimal("0.0000000000000000000000000000001"),
                new BigDecimal("111111111111111111111111111111"),
                new BigDecimal("1111111111111111111111111111111"),
                new BigDecimal("0.111111111111111111111111111111"),
                new BigDecimal("0.1111111111111111111111111111111"),
        };

        for (int i=0; i<bds.length; i++) {
            testReadDecimalPrecisionAndScaleFromRawBytes(bds[i]);
            testReadDecimalPrecisionAndScaleFromRawBytes(bds[i].negate());
        }
        
        assertTrue(new BigDecimal("5").remainder(BigDecimal.ONE).equals(BigDecimal.ZERO));
        assertTrue(new BigDecimal("5.0").remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO)==0);
        assertTrue(new BigDecimal("5.00").remainder(BigDecimal.ONE).compareTo(BigDecimal.ZERO)==0);
        assertFalse(new BigDecimal("5.01").remainder(BigDecimal.ONE).equals(BigDecimal.ZERO));
        assertFalse(new BigDecimal("-5.1").remainder(BigDecimal.ONE).equals(BigDecimal.ZERO));
    }
    
    @Test
    public void testDateConversions() {
        long now = System.currentTimeMillis();
        Date date = new Date(now);
        Time t = new Time(now);
        Timestamp ts = new Timestamp(now);
        
        Object o = PDate.INSTANCE.toObject(ts, PTimestamp.INSTANCE);
        assertEquals(o.getClass(), java.sql.Date.class);
        o = PDate.INSTANCE.toObject(t, PTime.INSTANCE);
        assertEquals(o.getClass(), java.sql.Date.class);
        
        o = PTime.INSTANCE.toObject(date, PDate.INSTANCE);
        assertEquals(o.getClass(), java.sql.Time.class);
        o = PTime.INSTANCE.toObject(ts, PTimestamp.INSTANCE);
        assertEquals(o.getClass(), java.sql.Time.class);
                
        o = PTimestamp.INSTANCE.toObject(date, PDate.INSTANCE);
        assertEquals(o.getClass(), java.sql.Timestamp.class);
        o = PTimestamp.INSTANCE.toObject(t, PTime.INSTANCE);
        assertEquals(o.getClass(), java.sql.Timestamp.class); 
    }

    @Test
    public void testNegativeDateTime() {
        Date date1 = new Date(-1000);
        Date date2 = new Date(-2000);
        assertTrue(date1.compareTo(date2) > 0);
        
        byte[] b1 = PDate.INSTANCE.toBytes(date1);
        byte[] b2 = PDate.INSTANCE.toBytes(date2);
        assertTrue(Bytes.compareTo(b1, b2) > 0);
        
    }
    
    @Test
    public void testIllegalUnsignedDateTime() {
        Date date1 = new Date(-1000);
        try {
            UnsignedDate.INSTANCE.toBytes(date1);
            fail();
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof SQLException);
            SQLException sqlE = (SQLException)e.getCause();
            assertEquals(SQLExceptionCode.ILLEGAL_DATA.getErrorCode(), sqlE.getErrorCode());
        }
    }

    @Test
    public void testGetResultSetSqlType() {
        assertEquals(Types.INTEGER, PInteger.INSTANCE.getResultSetSqlType());
        assertEquals(Types.INTEGER, UnsignedInt.INSTANCE.getResultSetSqlType());
        assertEquals(Types.BIGINT, PLong.INSTANCE.getResultSetSqlType());
        assertEquals(Types.BIGINT, UnsignedLong.INSTANCE.getResultSetSqlType());
        assertEquals(Types.SMALLINT, Smallint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.SMALLINT, UnsignedSmallint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TINYINT, Tinyint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TINYINT, UnsignedTinyint.INSTANCE.getResultSetSqlType());
        assertEquals(Types.FLOAT, PFloat.INSTANCE.getResultSetSqlType());
        assertEquals(Types.FLOAT, UnsignedFloat.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DOUBLE, PDouble.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DOUBLE, UnsignedDouble.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DATE, PDate.INSTANCE.getResultSetSqlType());
        assertEquals(Types.DATE, UnsignedDate.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIME, PTime.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIME, UnsignedTime.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIMESTAMP, PTimestamp.INSTANCE.getResultSetSqlType());
        assertEquals(Types.TIMESTAMP, UnsignedTimestamp.INSTANCE.getResultSetSqlType());

        // Check that all array types are defined as java.sql.Types.ARRAY
        for (PDataType dataType : PDataType.values()) {
            if (dataType.isArrayType()) {
                assertEquals("Wrong datatype for " + dataType,
                        Types.ARRAY,
                        dataType.getResultSetSqlType());
            }
        }
    }

    private void testReadDecimalPrecisionAndScaleFromRawBytes(BigDecimal bd) {
        byte[] b = Decimal.INSTANCE.toBytes(bd);
        int[] v = PDataType.getDecimalPrecisionAndScale(b, 0, b.length);
        assertEquals(bd.toString(), bd.precision(), v[0]);
        assertEquals(bd.toString(), bd.scale(), v[1]);
    }

    @Test
    public void testArithmeticOnLong() {
        long startWith = -5;
        long incrementBy = 1;
        for (int i = 0; i < 10; i++) {
            long next = nextValueFor(startWith, incrementBy);
            assertEquals(startWith + incrementBy, next);
            startWith = next;
        }
        startWith = 5;
        incrementBy = -1;
        for (int i = 0; i < 10; i++) {
            long next = nextValueFor(startWith, incrementBy);
            assertEquals(startWith + incrementBy, next);
            startWith = next;
        }
        startWith = 0;
        incrementBy = 100;
        for (int i = 0; i < 10; i++) {
            long next = nextValueFor(startWith, incrementBy);
            assertEquals(startWith + incrementBy, next);
            startWith = next;
        }
    }
    
    @Test
    public void testGetSampleValue() {
        PDataType[] types = PDataType.values();
        // Test validity of 10 sample values for each type
        for (int i = 0; i < 10; i++) {
            for (PDataType type : types) {
                Integer maxLength = 
                        (type == Char.INSTANCE 
                        || type == Binary.INSTANCE 
                        || type == CharArray.INSTANCE 
                        || type == BinaryArray.INSTANCE) ? 10 : null;
                int arrayLength = 10;
                Object sampleValue = type.getSampleValue(maxLength, arrayLength);
                byte[] b = type.toBytes(sampleValue);
                type.toObject(b, 0, b.length, type, SortOrder.getDefault(), maxLength, null);
            }
        }
    }

    // Simulate what an HBase Increment does with the value encoded as a long
    private long nextValueFor(long startWith, long incrementBy) {
        long hstartWith = Bytes.toLong(PLong.INSTANCE.toBytes(startWith));
        hstartWith += incrementBy;
        return (Long) PLong.INSTANCE.toObject(Bytes.toBytes(hstartWith));
    }
    
}
