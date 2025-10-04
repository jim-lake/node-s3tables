declare module 'parquetjs' {
  export class ParquetSchema {
    constructor(schema: Record<string, { type: string }>);
  }

  export class ParquetWriter {
    static openStream(schema: ParquetSchema, stream: NodeJS.WritableStream): Promise<ParquetWriter>;
    appendRow(row: Record<string, any>): Promise<void>;
    close(): Promise<void>;
  }
}
