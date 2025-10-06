import { PassThrough } from 'node:stream';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { ParquetWriter, ParquetSchema } from 'parquetjs';
import { clients } from './aws_clients';

export async function createSimpleParquetFile(
  tableBucket: string,
  fileIndex: number
): Promise<{ key: string; size: number }> {
  const s3Key = `data/app=test-app/data-${Date.now()}-${fileIndex}.parquet`;

  const schema = new ParquetSchema({
    app: { type: 'UTF8' },
    event_datetime: { type: 'TIMESTAMP_MILLIS' },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(schema, stream);

  for (let i = 0; i < 10; i++) {
    await writer.appendRow({
      app: 'test-app',
      event_datetime: new Date(Date.now() + i * 1000),
    });
  }

  await writer.close();

  const fileBuffer = Buffer.concat(chunks);

  await clients.s3.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );

  return { key: s3Key, size: fileBuffer.length };
}

export async function createPartitionedParquetFile(
  tableBucket: string,
  appName: string,
  date: Date,
  fileIndex: number
): Promise<{ key: string; size: number }> {
  const dateParts = date.toISOString().split('T');
  const dateStr = dateParts[0];
  if (!dateStr) {
    throw new Error('Could not extract date string from ISO string');
  }
  const s3Key = `data/app_name=${appName}/event_datetime_day=${dateStr}/data-${Date.now()}-${fileIndex}.parquet`;

  const schema = new ParquetSchema({
    app_name: { type: 'UTF8' },
    event_datetime: { type: 'TIMESTAMP_MILLIS' },
    detail: { type: 'UTF8' },
  });

  const stream = new PassThrough();
  const chunks: Buffer[] = [];
  stream.on('data', (chunk: Buffer) => chunks.push(chunk));

  const writer = await ParquetWriter.openStream(schema, stream);

  for (let i = 0; i < 10; i++) {
    await writer.appendRow({
      app_name: appName,
      event_datetime: new Date(date.getTime() + i * 1000),
      detail: `Detail for ${appName} record ${i}`,
    });
  }

  await writer.close();

  const fileBuffer = Buffer.concat(chunks);

  await clients.s3.send(
    new PutObjectCommand({ Bucket: tableBucket, Key: s3Key, Body: fileBuffer })
  );

  return { key: s3Key, size: fileBuffer.length };
}
