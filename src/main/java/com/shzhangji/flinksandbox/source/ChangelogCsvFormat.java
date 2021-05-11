package com.shzhangji.flinksandbox.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {
  private String columnDelimiter;

  public ChangelogCsvFormat(String columnDelimiter) {
    this.columnDelimiter = columnDelimiter;
  }

  @Override
  public DeserializationSchema<RowData> createRuntimeDecoder(DynamicTableSource.Context context, DataType producedDataType) {
    TypeInformation<RowData> producedTypeInfo = context.createTypeInformation(producedDataType);
    DynamicTableSource.DataStructureConverter converter = context.createDataStructureConverter(producedDataType);
    List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();
    return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.DELETE)
      .build();
  }
}
