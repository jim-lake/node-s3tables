'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var clientS3 = require('@aws-sdk/client-s3');
var clientS3tables = require('@aws-sdk/client-s3tables');
var signatureV4 = require('@smithy/signature-v4');
var sha256Js = require('@aws-crypto/sha256-js');
var protocolHttp = require('@smithy/protocol-http');
var credentialProviderNode = require('@aws-sdk/credential-provider-node');

async function icebergRequest(params) {
    const region = params.tableBucketARN.split(':')[3];
    if (!region) {
        throw new Error('bad tableBucketARN');
    }
    const arn = encodeURIComponent(params.tableBucketARN);
    const hostname = `s3tables.${region}.amazonaws.com`;
    const full_path = `/iceberg/v1/${arn}${params.suffix}`;
    const body = params.body ? JSON.stringify(params.body) : null;
    const req_opts = {
        method: params.method ?? 'GET',
        protocol: 'https:',
        path: full_path,
        hostname,
        headers: { host: hostname },
    };
    if (body && req_opts.headers) {
        req_opts.body = body;
        req_opts.headers['content-type'] = 'application/json';
        req_opts.headers['content-length'] = String(Buffer.byteLength(body));
    }
    const request = new protocolHttp.HttpRequest(req_opts);
    const signer = new signatureV4.SignatureV4({
        credentials: params.credentials ?? credentialProviderNode.defaultProvider(),
        region,
        service: 's3tables',
        sha256: sha256Js.Sha256,
    });
    const signed = await signer.sign(request);
    const url = `https://${hostname}${signed.path}`;
    const fetch_opts = {
        method: signed.method,
        headers: signed.headers,
    };
    if (signed.body) {
        fetch_opts.body = signed.body;
    }
    const res = await fetch(url, fetch_opts);
    if (!res.ok) {
        throw new Error(`request failed: ${res.status} ${res.statusText}`);
    }
    return (await res.json());
}

async function getMetadata(params) {
    const { config, ...other } = params;
    const client = new clientS3tables.S3TablesClient(config ?? {});
    const get_table_cmd = new clientS3tables.GetTableCommand(other);
    const response = await client.send(get_table_cmd);
    if (!response.metadataLocation) {
        throw new Error('missing metadataLocation');
    }
    const s3_client = new clientS3.S3Client(config ?? {});
    const { key, bucket } = _parseS3Url(response.metadataLocation);
    const get_file_cmd = new clientS3.GetObjectCommand({ Bucket: bucket, Key: key });
    const file_response = await s3_client.send(get_file_cmd);
    const body = await file_response.Body?.transformToString();
    if (!body) {
        throw new Error('missing body');
    }
    return JSON.parse(body);
}
async function addSchema(params) {
    return icebergRequest({
        tableBucketARN: params.tableBucketARN,
        method: 'POST',
        suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
        body: {
            requirements: [],
            updates: [
                {
                    action: 'add-schema',
                    schema: {
                        'schema-id': params.schemaId,
                        type: 'struct',
                        fields: params.fields,
                    },
                },
                { action: 'set-current-schema', 'schema-id': params.schemaId },
            ],
        },
    });
}
async function addPartitionSpec(params) {
    return icebergRequest({
        tableBucketARN: params.tableBucketARN,
        method: 'POST',
        suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
        body: {
            requirements: [],
            updates: [
                {
                    action: 'add-spec',
                    spec: {
                        'spec-id': params.specId,
                        type: 'struct',
                        fields: params.fields,
                    },
                },
                { action: 'set-default-spec', 'spec-id': params.specId },
            ],
        },
    });
}
const S3_REGEX = /^s3:\/\/([^/]+)\/(.+)$/;
function _parseS3Url(url) {
    const match = S3_REGEX.exec(url);
    if (!match) {
        throw new Error('Invalid S3 URL');
    }
    return { bucket: match[1], key: match[2] };
}

var index = { getMetadata, addSchema, addPartitionSpec };

exports.addPartitionSpec = addPartitionSpec;
exports.addSchema = addSchema;
exports.default = index;
exports.getMetadata = getMetadata;
