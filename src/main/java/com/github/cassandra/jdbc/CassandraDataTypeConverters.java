package com.github.cassandra.jdbc;

import com.google.common.base.Function;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.pmw.tinylog.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class CassandraDataTypeConverters {
    public static final CassandraDataTypeConverters instance = new CassandraDataTypeConverters();

    static {
        instance.addMapping(String.class, "", new Function<Object, String>() {
            public String apply(Object input) {
                return String.valueOf(input);
            }
        });
        instance.addMapping(java.util.UUID.class, java.util.UUID.randomUUID(), new Function<Object, UUID>() {
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
        instance.addMapping(InetAddress.class, defaultAddress, new Function<Object, InetAddress>() {
            public InetAddress apply(Object input) {
                try {
                    return InetAddress.getByName(String.valueOf(input));
                } catch (UnknownHostException e) {
                    throw CassandraErrors.unexpectedException(e);
                }
            }
        });
        instance.addMapping(ByteBuffer.class, ByteBuffer.wrap(new byte[0]), new Function<Object, ByteBuffer>() {
            public ByteBuffer apply(Object input) {
                return ByteBuffer.wrap(input instanceof byte[] ? (byte[]) input : String.valueOf(input).getBytes());
            }
        });
        instance.addMapping(Boolean.class, Boolean.FALSE, new Function<Object, Boolean>() {
            public Boolean apply(Object input) {
                return Boolean.valueOf(String.valueOf(input));
            }
        });
        instance.addMapping(Byte.class, 0, new Function<Object, Byte>() {
            public Byte apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).byteValue()
                        : Ints.tryParse(String.valueOf(input)).byteValue();
            }
        });
        instance.addMapping(Short.class, 0, new Function<Object, Short>() {
            public Short apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).shortValue()
                        : Ints.tryParse(String.valueOf(input)).shortValue();
            }
        });
        instance.addMapping(Integer.class, 0, new Function<Object, Integer>() {
            public Integer apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).intValue()
                        : Ints.tryParse(String.valueOf(input));
            }
        });
        instance.addMapping(Long.class, 0L, new Function<Object, Long>() {
            public Long apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).longValue()
                        : Long.parseLong(String.valueOf(input));
            }
        });
        instance.addMapping(Float.class, 0.0F, new Function<Object, Float>() {
            public Float apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).floatValue()
                        : Doubles.tryParse(String.valueOf(input)).floatValue();
            }
        });
        instance.addMapping(Double.class, 0.0D, new Function<Object, Double>() {
            public Double apply(Object input) {
                return input instanceof Number
                        ? ((Number) input).doubleValue()
                        : Doubles.tryParse(String.valueOf(input));
            }
        });
        instance.addMapping(BigDecimal.class, BigDecimal.ZERO, new Function<Object, BigDecimal>() {
            public BigDecimal apply(Object input) {
                return new BigDecimal(String.valueOf(input));
            }
        });
        instance.addMapping(BigInteger.class, BigInteger.ZERO, new Function<Object, BigInteger>() {
            public BigInteger apply(Object input) {
                return new BigInteger(String.valueOf(input));
            }
        });

        instance.addMapping(Date.class, new Date(System.currentTimeMillis()), new Function<Object, Date>() {
            public Date apply(Object input) {
                return Date.valueOf(String.valueOf(input));
            }
        });
        instance.addMapping(Time.class, new Time(System.currentTimeMillis()), new Function<Object, Time>() {
            public Time apply(Object input) {
                return Time.valueOf(String.valueOf(input));
            }
        });
        instance.addMapping(Timestamp.class, new Timestamp(System.currentTimeMillis()),
                new Function<Object, Timestamp>() {
                    public Timestamp apply(Object input) {
                        return Timestamp.valueOf(String.valueOf(input));
                    }
                });

        instance.seal();
    }

    public void addMapping(Class clazz, Object defaultValue, Function converter) {
        if (sealed) {
            throw new IllegalStateException("Sealed converters are readonly!");
        }

        String key = clazz == null ? null : clazz.getName();

        if (defaultValue != null) {
            defaultValues.put(key, defaultValue);
        }

        if (converter != null) {
            typeMappings.put(key, converter);
        }
    }

    public void seal() {
        sealed = true;
    }

    public <T> T convert(Object value, Class<T> type) {
        return convert(value, type, false);
    }

    public <T> T convert(Object value, Class<T> type, boolean replaceNullValue) {
        T result;

        String key = type.getName();
        if (replaceNullValue && value == null) {
            result = (T) defaultValues.get(key);
            if (result == null && parent != null) {
                result = parent.convert(value, type, replaceNullValue);
            }
        } else if (type.isInstance(value)) {
            result = (T) value;
        } else {
            Function<Object, T> func = typeMappings.get(key);
            result = func == null // convert function is not available for this type
                    ? (parent == null // top most converters
                    ? type.cast(value) // this will usually end up with ClassCastException
                    : parent.convert(value, type, replaceNullValue)) // delegate to parent
                    : func.apply(value);
        }

        return result;
    }

    private final Map<String, Object> defaultValues = new HashMap<String, Object>();
    private final Map<String, Function> typeMappings = new HashMap<String, Function>();

    private boolean sealed = false;

    private final CassandraDataTypeConverters parent;

    public CassandraDataTypeConverters() {
        this.parent = null;
    }

    public CassandraDataTypeConverters(CassandraDataTypeConverters parent) {
        this.parent = parent == null ? instance : parent;
    }
}
