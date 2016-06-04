package com.github.cassandra.jdbc;

import com.google.common.base.Function;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CassandraDataTypeConverters {
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
        addMapping(ByteBuffer.class, ByteBuffer.wrap(new byte[0]), new Function<Object, ByteBuffer>() {
            public ByteBuffer apply(Object input) {
                return ByteBuffer.wrap(input instanceof byte[] ? (byte[]) input : String.valueOf(input).getBytes());
            }
        });
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
                return Date.valueOf(String.valueOf(input));
            }
        });
        addMapping(Time.class, new Time(System.currentTimeMillis()), new Function<Object, Time>() {
            public Time apply(Object input) {
                return Time.valueOf(String.valueOf(input));
            }
        });
        addMapping(Timestamp.class, new Timestamp(System.currentTimeMillis()),
                new Function<Object, Timestamp>() {
                    public Timestamp apply(Object input) {
                        return Timestamp.valueOf(String.valueOf(input));
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
        if (replaceNullValue && value == null) {
            result = (T) defaultValues.get(key);
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
