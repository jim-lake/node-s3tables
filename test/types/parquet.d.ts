/* eslint-disable */

declare module 'parquetjs' {
  export class ParquetSchema {
    constructor(
      schema: Record<
        string,
        { type: string; compression?: string; optional?: boolean }
      >
    );
  }

  // Writer uses plain numbers
  interface ParquetWriterColumnMetadata {
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

  interface ParquetWriterRowGroup {
    columns: ParquetWriterColumnMetadata[];
  }

  interface ParquetEnvelopeWriter {
    rowGroups: ParquetWriterRowGroup[];
  }

  // Reader uses Int64Value objects
  interface Int64Value {
    buffer: Buffer;
    offset: number;
  }

  interface ParquetReaderColumnMetadata {
    meta_data?: {
      path_in_schema?: string[];
      total_compressed_size?: Int64Value;
      num_values?: Int64Value;
      statistics?: {
        distinct_count?: Int64Value;
        null_count?: Int64Value;
        min_value?: string | Buffer;
        max_value?: string | Buffer;
      };
    };
  }

  interface ParquetReaderRowGroup {
    columns: ParquetReaderColumnMetadata[];
  }

  interface ParquetMetadata {
    row_groups: ParquetReaderRowGroup[];
  }

  interface ParquetCursor {
    next(): Promise<Record<string, unknown> | null>;
  }

  interface ParquetReaderOptions {
    treatInt96AsTimestamp?: boolean;
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

  export class ParquetReader {
    static openBuffer(
      buffer: Buffer,
      options?: ParquetReaderOptions
    ): Promise<ParquetReader>;

    close(): Promise<void>;

    getCursor(): ParquetCursor;

    metadata: ParquetMetadata;
  }
}
