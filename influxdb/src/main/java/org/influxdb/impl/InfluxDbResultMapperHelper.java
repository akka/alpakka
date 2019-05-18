/*
 * Copyright (C) 2016-2019 Lightbend Inc. <http://www.lightbend.com>
 */

package org.influxdb.impl;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.influxdb.InfluxDBMapperException;
import org.influxdb.annotation.Column;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;

public class InfluxDbResultMapperHelper {

  private final InfluxDBResultMapper influxDBResultMapper = new InfluxDBResultMapper();

  public void cacheClassFields(final Class<?> clazz) {
    influxDBResultMapper.cacheMeasurementClass(clazz);
  }

  public <T> String databaseName(T model) {
    Class<?> clazz = model.getClass();
    return influxDBResultMapper.getDatabaseName(clazz);
  }

  public <T> String retentionPolicy(T model) {
    Class<?> clazz = model.getClass();
    return influxDBResultMapper.getRetentionPolicy(clazz);
  }

  public <T> Point convertModelToPoint(T model) {
    influxDBResultMapper.throwExceptionIfMissingAnnotation(model.getClass());
    influxDBResultMapper.cacheMeasurementClass(model.getClass());

    ConcurrentMap<String, Field> colNameAndFieldMap =
        influxDBResultMapper.getColNameAndFieldMap(model.getClass());

    try {
      Class<?> modelType = model.getClass();
      String measurement = influxDBResultMapper.getMeasurementName(modelType);
      TimeUnit timeUnit = influxDBResultMapper.getTimeUnit(modelType);
      long time = timeUnit.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
      Point.Builder pointBuilder = Point.measurement(measurement).time(time, timeUnit);

      for (String key : colNameAndFieldMap.keySet()) {
        Field field = colNameAndFieldMap.get(key);
        Column column = field.getAnnotation(Column.class);
        String columnName = column.name();
        Class<?> fieldType = field.getType();

        if (!field.isAccessible()) {
          field.setAccessible(true);
        }

        Object value = field.get(model);

        if (column.tag()) {
          /** Tags are strings either way. */
          pointBuilder.tag(columnName, value.toString());
        } else if ("time".equals(columnName)) {
          if (value != null) {
            setTime(pointBuilder, fieldType, timeUnit, value);
          }
        } else {
          setField(pointBuilder, fieldType, columnName, value);
        }
      }

      return pointBuilder.build();
    } catch (IllegalAccessException e) {
      throw new InfluxDBMapperException(e);
    }
  }

  private void setTime(
      final Point.Builder pointBuilder,
      final Class<?> fieldType,
      final TimeUnit timeUnit,
      final Object value) {
    if (Instant.class.isAssignableFrom(fieldType)) {
      Instant instant = (Instant) value;
      long time = timeUnit.convert(instant.toEpochMilli(), TimeUnit.MILLISECONDS);
      pointBuilder.time(time, timeUnit);
    } else {
      throw new InfluxDBMapperException(
          "Unsupported type " + fieldType + " for time: should be of Instant type");
    }
  }

  private void setField(
      final Point.Builder pointBuilder,
      final Class<?> fieldType,
      final String columnName,
      final Object value) {
    if (boolean.class.isAssignableFrom(fieldType) || Boolean.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (boolean) value);
    } else if (long.class.isAssignableFrom(fieldType) || Long.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (long) value);
    } else if (double.class.isAssignableFrom(fieldType)
        || Double.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (double) value);
    } else if (int.class.isAssignableFrom(fieldType) || Integer.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (int) value);
    } else if (String.class.isAssignableFrom(fieldType)) {
      pointBuilder.addField(columnName, (String) value);
    } else {
      throw new InfluxDBMapperException(
          "Unsupported type " + fieldType + " for column " + columnName);
    }
  }

  public <T> List<T> parseSeriesAs(
      final Class<T> clazz, final QueryResult.Series series, final TimeUnit precision) {
    influxDBResultMapper.cacheMeasurementClass(clazz);
    return series.getValues().stream()
        .map(v -> parseRowAs(clazz, series.getColumns(), v, precision))
        .collect(Collectors.toList());
  }

  public <T> T parseRowAs(
      final Class<T> clazz, List<String> columns, final List<Object> values, TimeUnit precision) {

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
