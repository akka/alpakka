package org.influxdb.impl;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDBMapperException;

public class InfluxDBResultMapperHelper {

    private final InfluxDBResultMapper influxDBResultMapper = new InfluxDBResultMapper();

    public void cacheClassFields(final Class<?> clazz) {
        influxDBResultMapper.cacheMeasurementClass(clazz);
    }

    public <T> T parseRowAs(final Class<T> clazz, List<String> columns,final List<Object> values, TimeUnit precision) {

        try {
            ConcurrentMap<String, Field> fieldMap = influxDBResultMapper.getColNameAndFieldMap(clazz);

            T object = null;

            for (int i = 0; i < columns.size(); i++) {
                Field correspondingField = fieldMap.get(columns.get(i));

                if (correspondingField != null) {
                    if (object == null) {
                        object = clazz.newInstance();
                    }
                    influxDBResultMapper.setFieldValue(object, correspondingField, columns.get(i), precision);
                }

            }

            return object;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new InfluxDBMapperException(e);
        }
    }

}
