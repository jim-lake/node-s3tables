/* eslint-disable */

declare module 'parquetjs' {
  export class ParquetSchema {
    constructor(schema: Record<string, { type: string }>);
  }

  interface ParquetColumnMetadata {
    meta_data?: {
      path_in_schema?: string[];
      total_compressed_size?: number;
      num_values?: number;
      statistics?: {
        distinct_count?: number;
        null_count?: number;
        min_value?: Buffer;
        max_value?: Buffer;
      };
    };
  }

  interface ParquetRowGroup {
    columns: ParquetColumnMetadata[];
  }

  interface ParquetEnvelopeWriter {
    rowGroups: ParquetRowGroup[];
  }

  export class ParquetWriter {
    static openStream(
      schema: ParquetSchema,
      stream: NodeJS.WritableStream
    ): Promise<ParquetWriter>;

    appendRow(row: Record<string, any>): Promise<void>;

    close(): Promise<void>;

    envelopeWriter: ParquetEnvelopeWriter;
  }
}
