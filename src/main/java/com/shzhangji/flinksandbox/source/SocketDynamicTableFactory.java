package com.shzhangji.flinksandbox.source;

import com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class SocketDynamicTableFactory implements DynamicTableSourceFactory {
  public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
    .intType()
    .noDefaultValue();

  public static final ConfigOption<Integer> BYTE_DELIMITER = ConfigOptions.key("byte-delimiter")
    .intType()
    .defaultValue(10); // \n

  @Override
  public String factoryIdentifier() {
    return "socket";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(HOSTNAME);
    options.add(PORT);
    options.add(FactoryUtil.FORMAT);
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    Set<ConfigOption<?>> options = new HashSet<>();
    options.add(BYTE_DELIMITER);
    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

    DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
      DeserializationFormatFactory.class,
      FactoryUtil.FORMAT);

    helper.validate();

    ReadableConfig options = helper.getOptions();
    String hostname = options.get(HOSTNAME);
    int port = options.get(PORT);
    byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

    DataType producedDataType = context.getCatalogTable().getResolvedSchema().toPhysicalRowDataType();

    return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
  }
}
