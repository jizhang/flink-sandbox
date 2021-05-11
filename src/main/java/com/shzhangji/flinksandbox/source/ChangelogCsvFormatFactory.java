package com.shzhangji.flinksandbox.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ChangelogCsvFormatFactory implements DeserializationFormatFactory {
  public static final ConfigOption<String> COLUMN_DELIMITER = ConfigOptions.key("column-delimiter")
    .stringType()
    .defaultValue("|");

  @Override
  public String factoryIdentifier() {
    return "changelog-csv";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(COLUMN_DELIMITER);
    return options;
  }

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context,
      ReadableConfig formatOptions) {
    FactoryUtil.validateFactoryOptions(this, formatOptions);
    String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);
    return new ChangelogCsvFormat(columnDelimiter);
  }
}
