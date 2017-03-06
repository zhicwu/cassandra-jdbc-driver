/**
 * Copyright (C) 2015-2017, Zhichun Wu
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.cassandra.jdbc;

import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.pmw.tinylog.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;

public class CassandraDataTypeConverters {
    private static final Splitter valueSplitter = Splitter.on(',').trimResults().omitEmptyStrings();

    private static final byte[] emptyByteArray = new byte[0];

    private static final List emptyList = ImmutableList.builder().build();
    private static final Set emptySet = ImmutableSet.builder().build();
    private static final Map emptyMap = ImmutableMap.builder().build();

    static final CassandraDataTypeConverters instance = new CassandraDataTypeConverters();

    private final Map<String, Object> defaultValues = new HashMap<String, Object>();
    private final Map<String, Function> typeMappings = new HashMap<String, Function>();

    protected void init() {
        // use "null" instead of empty string to avoid "InvalidQueryException: Key may not be empty"
        addMapping(String.class, "null", new Function<Object, String>() {
            public String apply(Object input) {
                String result;
                if (input instanceof Readable) {
                    try {
                        result = CharStreams.toString(((Readable) input));
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to read from Readable " + input, e);
                    }
                } else {
                    result = String.valueOf(input);
                }

                return result;
            }
        });
        addMapping(java.util.UUID.class, java.util.UUID.randomUUID(), new Function<Object, UUID>() {
            public UUID apply(Object input) {
                return java.util.UUID.fromString(String.valueOf(input));
            }
        });

        InetAddress defaultAddress = null;
        try {
            defaultAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            Logger.warn(e, "Failed to get local host");
        }
        addMapping(InetAddress.class, defaultAddress, new Function<Object, InetAddress>() {
            public InetAddress apply(Object input) {
                try {
                    return InetAddress.getByName(String.valueOf(input));
                } catch (UnknownHostException e) {
                    throw CassandraErrors.unexpectedException(e);
                }
            }
        });
        addMapping(Blob.class, new CassandraBlob(new byte[0]), new Function<Object, Blob>() {
            public Blob apply(Object input) {
                CassandraBlob blob;

                if (input instanceof ByteBuffer) {
                    blob = new CassandraBlob((ByteBuffer) input);
                } else if (input instanceof byte[]) {
                    blob = new CassandraBlob((byte[]) input);
                } else if (input instanceof InputStream) {
                    try {
                        blob = new CassandraBlob(ByteStreams.toByteArray((InputStream) input));
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to read from input stream " + input, e);
                    }
                } else {
                    blob = new CassandraBlob(String.valueOf(input).getBytes());
                }

                return blob;
            }
        });
        addMapping(byte[].class, emptyByteArray, new Function<Object, byte[]>() {
            public byte[] apply(Object input) {
                byte[] result;

                if (input instanceof ByteBuffer) {
                    result = ((ByteBuffer) input).array();
                } else {
                    result = String.valueOf(input).getBytes();
                }

                return result;
            }
        });
        /*
        addMapping(ByteBuffer.class, ByteBuffer.wrap(new byte[0]), new Function<Object, ByteBuffer>() {
            public ByteBuffer apply(Object input) {
                return ByteBuffer.wrap(input instanceof byte[] ? (byte[]) input : String.valueOf(input).getBytes());
            }
        });
        */
        addMapping(Boolean.class, Boolean.FALSE, new Function<Object, Boolean>() {
            public Boolean apply(Object input) {
                return Boolean.valueOf(String.valueOf(input));
            }
        });
        addMapping(Byte.class, (byte) 0, new Function<Object, Byte>() {
            public Byte apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).byteValue()
                        : Ints.tryParse(String.valueOf(input)).byteValue();
            }
        });
        addMapping(Short.class, (short) 0, new Function<Object, Short>() {
            public Short apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).shortValue()
                        : Ints.tryParse(String.valueOf(input)).shortValue();
            }
        });
        addMapping(Integer.class, 0, new Function<Object, Integer>() {
            public Integer apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).intValue()
                        : Ints.tryParse(String.valueOf(input));
            }
        });
        addMapping(Long.class, 0L, new Function<Object, Long>() {
            public Long apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).longValue()
                        : Long.parseLong(String.valueOf(input));
            }
        });
        addMapping(Float.class, 0.0F, new Function<Object, Float>() {
            public Float apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).floatValue()
                        : Doubles.tryParse(String.valueOf(input)).floatValue();
            }
        });
        addMapping(Double.class, 0.0D, new Function<Object, Double>() {
            public Double apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).doubleValue()
                        : Doubles.tryParse(String.valueOf(input));
            }
        });
        addMapping(BigDecimal.class, BigDecimal.ZERO, new Function<Object, BigDecimal>() {
            public BigDecimal apply(Object input) {
                return new BigDecimal(String.valueOf(input));
            }
        });
        addMapping(BigInteger.class, BigInteger.ZERO, new Function<Object, BigInteger>() {
            public BigInteger apply(Object input) {
                return new BigInteger(String.valueOf(input));
            }
        });

        addMapping(Date.class, new Date(System.currentTimeMillis()), new Function<Object, Date>() {
            public Date apply(Object input) {
                Date result;
                if (input instanceof LocalDate) {
                    result = new Date(((LocalDate) input).toDate().getTime());
                } else if (input instanceof java.util.Date) {
                    result = new Date(((java.util.Date) input).getTime());
                } else {
                    result = new Date(LocalDate.parse(String.valueOf(input)).toDate().getTime());
                }
                return result;
            }
        });
        addMapping(Time.class, new Time(System.currentTimeMillis()), new Function<Object, Time>() {
            public Time apply(Object input) {
                Time result;
                if (input instanceof LocalTime) {
                    result = new Time(((LocalTime) input).toDateTimeToday().getMillis());
                } else if (input instanceof java.util.Date) {
                    result = new Time(((java.util.Date) input).getTime());
                } else {
                    result = new Time(LocalTime.parse(String.valueOf(input)).toDateTimeToday().getMillis());
                }
                return result;
            }
        });
        addMapping(Timestamp.class, new Timestamp(System.currentTimeMillis()), new Function<Object, Timestamp>() {
            public Timestamp apply(Object input) {
                Timestamp result;
                if (input instanceof Instant) {
                    result = new Timestamp(((Instant) input).toDate().getTime());
                } else if (input instanceof java.util.Date) {
                    result = new Timestamp(((java.util.Date) input).getTime());
                } else if (input instanceof Number) {
                    result = new Timestamp(((Number) input).longValue());
                } else {
                    String dateTime = String.valueOf(input);
                    if (dateTime.indexOf(' ') == 10 && dateTime.indexOf('Z') < 0) {
                        StringBuilder builder = new StringBuilder(dateTime).append('Z');
                        builder.setCharAt(10, 'T');
                        dateTime = builder.toString();
                    }

                    result = new Timestamp(Instant.parse(dateTime).toDate().getTime());
                }
                return result;
            }
        });

        // now collections
        addMapping(List.class, emptyList, new Function<Object, List>() {
            public List apply(Object input) {
                List result;
                if (input instanceof Iterable) {
                    result = Lists.newArrayList((Iterable) input);
                } else if (input instanceof Object[]) {
                    result = Lists.newArrayList((Object[]) input);
                } else {
                    result = valueSplitter.splitToList(String.valueOf(input));
                }

                return result;
            }
        });
        addMapping(Set.class, emptySet, new Function<Object, Set>() {
            public Set apply(Object input) {
                Set result;
                if (input instanceof Iterable) {
                    result = Sets.newTreeSet((Iterable) input);
                } else if (input instanceof Object[]) {
                    result = Sets.newHashSet((Object[]) input);
                } else {
                    result = Sets.newTreeSet(valueSplitter.splitToList(String.valueOf(input)));
                }

                return result;
            }
        });
        addMapping(Set.class, emptyMap, new Function<Object, Map>() {
            public Map apply(Object input) {
                return Map.class.cast(input);
            }
        });
    }

    protected void addMapping(Class clazz, Object defaultValue, Function converter) {
        String key = clazz == null ? null : clazz.getName();

        if (defaultValue != null) {
            defaultValues.put(key, defaultValue);
        }

        if (converter != null) {
            typeMappings.put(key, converter);
        }
    }

    protected CassandraDataTypeConverters() {
        init();
    }

    public <T> T defaultValueOf(Class<T> type) {
        return (T) defaultValues.get(type.getName());
    }

    public <T> T convert(Object value, Class<T> type, boolean replaceNullValue) {
        T result;

        String key = type.getName();
        if (value == null) {
            result = replaceNullValue ? (T) defaultValues.get(key) : null;
        } else if (type.isInstance(value)) {
            result = (T) value;
        } else {
            Function<Object, T> func = typeMappings.get(key);
            result = func == null // convert function is not available for this type
                    ? type.cast(value) // this will usually end up with ClassCastException
                    : func.apply(value);
        }

        return result;
    }
}
