package org.influxdb.impl;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.influxdb.InfluxDBMapperException;
import org.influxdb.dto.QueryResult;

public class InfluxDBResultMapperHelper {

    private final InfluxDBResultMapper influxDBResultMapper = new InfluxDBResultMapper();

    public void cacheClassFields(final Class<?> clazz) {
        influxDBResultMapper.cacheMeasurementClass(clazz);
    }

    public <T> List<T> parseSeriesAs(final Class<T> clazz, final QueryResult.Series series, final TimeUnit precision) {
        influxDBResultMapper.cacheMeasurementClass(clazz);
        return series.getValues().stream()
                     .map(v-> parseRowAs(clazz, series.getColumns(), v, precision))
                     .collect(Collectors.toList());
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

                    influxDBResultMapper.setFieldValue(object, correspondingField, values.get(i), precision);
                }

            }

            return object;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new InfluxDBMapperException(e);
        }
    }

}
