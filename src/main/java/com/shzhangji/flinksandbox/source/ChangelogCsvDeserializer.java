package com.shzhangji.flinksandbox.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.util.List;
import java.util.regex.Pattern;

public class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {
  private List<LogicalType> parsingTypes;
  private DynamicTableSource.DataStructureConverter converter;
  private TypeInformation<RowData> producedTypeInfo;
  private String columnDelimiter;

  public ChangelogCsvDeserializer(
      List<LogicalType> parsingTypes,
      DynamicTableSource.DataStructureConverter converter,
      TypeInformation<RowData> producedTypeInfo,
      String columnDelimiter) {
    this.parsingTypes = parsingTypes;
    this.converter = converter;
    this.producedTypeInfo = producedTypeInfo;
    this.columnDelimiter = columnDelimiter;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return producedTypeInfo;
  }

  @Override
  public void open(InitializationContext context) {
    converter.open(RuntimeConverter.Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
  }

  @Override
  public RowData deserialize(byte[] message) {
    String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
    RowKind kind = RowKind.valueOf(columns[0]);
    Row row = new Row(kind, parsingTypes.size());
    for (int i = 0; i < parsingTypes.size(); ++i) {
      row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
    }
    return (RowData) converter.toInternal(row);
  }

  private Object parse(LogicalTypeRoot root, String value) {
    switch (root) {
      case INTEGER:
        return Integer.parseInt(value);
      case VARCHAR:
        return value;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }
}
