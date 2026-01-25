'use strict';

Object.defineProperty(exports, '__esModule', { value: true });

var node_crypto = require('node:crypto');
var avsc = require('avsc');
var zlib = require('node:zlib');
var clientS3 = require('@aws-sdk/client-s3');
var clientS3tables = require('@aws-sdk/client-s3tables');
var libStorage = require('@aws-sdk/lib-storage');
var node_stream = require('node:stream');
var LosslessJson = require('lossless-json');
var signatureV4 = require('@smithy/signature-v4');
var sha256Js = require('@aws-crypto/sha256-js');
var protocolHttp = require('@smithy/protocol-http');
var credentialProviderNode = require('@aws-sdk/credential-provider-node');

function _interopNamespaceDefault(e) {
    var n = Object.create(null);
    if (e) {
        Object.keys(e).forEach(function (k) {
            if (k !== 'default') {
                var d = Object.getOwnPropertyDescriptor(e, k);
                Object.defineProperty(n, k, d.get ? d : {
                    enumerable: true,
                    get: function () { return e[k]; }
                });
            }
        });
    }
    n.default = e;
    return Object.freeze(n);
}

var avsc__namespace = /*#__PURE__*/_interopNamespaceDefault(avsc);
var zlib__namespace = /*#__PURE__*/_interopNamespaceDefault(zlib);
var LosslessJson__namespace = /*#__PURE__*/_interopNamespaceDefault(LosslessJson);

function fixupMetadata(metadata) {
    const newMetadata = {};
    for (const [key, value] of Object.entries(metadata)) {
        if (Buffer.isBuffer(value)) {
            newMetadata[key] = value;
        }
        else {
            newMetadata[key] = Buffer.from(value, 'utf8');
        }
    }
    return newMetadata;
}
async function avroToBuffer(params) {
    const metadata = params.metadata
        ? fixupMetadata(params.metadata)
        : params.metadata;
    return new Promise((resolve, reject) => {
        try {
            const buffers = [];
            const opts = {
                writeHeader: true,
                codecs: { deflate: zlib__namespace.deflateRaw },
                codec: 'deflate',
                metadata,
            };
            const encoder = new avsc__namespace.streams.BlockEncoder(params.type, opts);
            encoder.on('data', (chunk) => {
                buffers.push(chunk);
            });
            encoder.on('end', () => {
                resolve(Buffer.concat(buffers));
            });
            encoder.on('error', reject);
            params.records.forEach((record) => {
                encoder.write(record);
            });
            encoder.end();
        }
        catch (err) {
            if (err instanceof Error) {
                reject(err);
            }
            else {
                reject(new Error(String(err)));
            }
        }
    });
}
function icebergToAvroFields(spec, schemas) {
    return spec.fields.map((p) => _icebergToAvroField(p, schemas));
}
function _icebergToAvroField(field, schemas) {
    let source;
    for (const schema of schemas) {
        for (const f of schema.fields) {
            if (f.id === field['source-id']) {
                source = f;
                break;
            }
        }
    }
    if (!source) {
        throw new Error(`Source field ${field['source-id']} not found in schemas`);
    }
    let avroType;
    switch (field.transform) {
        case 'identity':
            if (typeof source.type === 'string') {
                avroType = _mapPrimitiveToAvro(source.type);
                break;
            }
            throw new Error(`Unsupported transform: ${field.transform} for complex type`);
        case 'year':
            avroType = { type: 'int', logicalType: 'year' };
            break;
        case 'month':
            avroType = { type: 'int', logicalType: 'month' };
            break;
        case 'day':
            avroType = { type: 'int', logicalType: 'date' };
            break;
        case 'hour':
            avroType = { type: 'long', logicalType: 'hour' };
            break;
        default:
            if (field.transform.startsWith('bucket[')) {
                avroType = 'int';
                break;
            }
            else if (field.transform.startsWith('truncate[')) {
                avroType = 'string';
                break;
            }
            throw new Error(`Unsupported transform: ${field.transform} for type`);
    }
    return {
        name: field.name,
        type: ['null', avroType],
        default: null,
        'field-id': field['field-id'],
    };
}
function _mapPrimitiveToAvro(type) {
    switch (type) {
        case 'boolean':
            return 'int';
        case 'int':
            return 'int';
        case 'long':
        case 'time':
        case 'timestamp':
        case 'timestamptz':
            return 'long';
        case 'float':
        case 'double':
            return 'double';
        case 'date':
            return { type: 'int', logicalType: 'date' };
        case 'string':
        case 'uuid':
            return 'string';
        case 'binary':
            return 'bytes';
        default:
            throw new Error(`Unsupported primitive: ${type}`);
    }
}

var ManifestFileStatus;
(function (ManifestFileStatus) {
    ManifestFileStatus[ManifestFileStatus["EXISTING"] = 0] = "EXISTING";
    ManifestFileStatus[ManifestFileStatus["ADDED"] = 1] = "ADDED";
    ManifestFileStatus[ManifestFileStatus["DELETED"] = 2] = "DELETED";
})(ManifestFileStatus || (ManifestFileStatus = {}));
var DataFileContent;
(function (DataFileContent) {
    DataFileContent[DataFileContent["DATA"] = 0] = "DATA";
    DataFileContent[DataFileContent["POSITION_DELETES"] = 1] = "POSITION_DELETES";
    DataFileContent[DataFileContent["EQUALITY_DELETES"] = 2] = "EQUALITY_DELETES";
})(DataFileContent || (DataFileContent = {}));
var ListContent;
(function (ListContent) {
    ListContent[ListContent["DATA"] = 0] = "DATA";
    ListContent[ListContent["DELETES"] = 1] = "DELETES";
})(ListContent || (ListContent = {}));
const BigIntType = avsc__namespace.types.LongType.__with({
    fromBuffer(uint_array) {
        return Buffer.from(uint_array).readBigInt64LE();
    },
    toBuffer(n) {
        const buf = Buffer.alloc(8);
        buf.writeBigInt64LE(n);
        return buf;
    },
    fromJSON: BigInt,
    toJSON: Number,
    isValid: (n) => typeof n === 'bigint',
    compare(n1, n2) {
        return n1 === n2 ? 0 : n1 < n2 ? -1 : 1;
    },
});
class YearStringType extends avsc__namespace.types.LogicalType {
    _fromValue(val) {
        return (1970 + val).toString();
    }
    _toValue(str) {
        return parseInt(str, 10) - 1970;
    }
    _resolve(type) {
        if (avsc__namespace.Type.isType(type, 'int')) {
            return (val) => this._fromValue(val);
        }
        return null;
    }
}
class MonthStringType extends avsc__namespace.types.LogicalType {
    _fromValue(val) {
        const year = 1970 + Math.floor(val / 12);
        const month = (val % 12) + 1;
        return `${year}-${String(month).padStart(2, '0')}`;
    }
    _toValue(str) {
        const [y, m] = str.split('-').map(Number);
        return ((y ?? 1970) - 1970) * 12 + ((m ?? 1) - 1);
    }
    _resolve(type) {
        if (avsc__namespace.Type.isType(type, 'int')) {
            return (val) => this._fromValue(val);
        }
        return null;
    }
}
class DateStringType extends avsc__namespace.types.LogicalType {
    _fromValue(val) {
        const ms = val * 86400000;
        return new Date(ms).toISOString().slice(0, 10);
    }
    _toValue(str) {
        const [year, month, day] = str.split('-').map(Number);
        return Math.floor(Date.UTC(year ?? 1970, (month ?? 1) - 1, day ?? 1) / 86400000);
    }
    _resolve(type) {
        if (avsc__namespace.Type.isType(type, 'int')) {
            return (val) => this._fromValue(val);
        }
        return null;
    }
}
class HourStringType extends avsc__namespace.types.LogicalType {
    _fromValue(val) {
        const ms = val * 3600000;
        return new Date(ms).toISOString().slice(0, 13);
    }
    _toValue(str) {
        const d = new Date(str);
        return Math.floor(d.getTime() / 3600000);
    }
    _resolve(type) {
        if (avsc__namespace.Type.isType(type, 'long')) {
            return (val) => this._fromValue(val);
        }
        return null;
    }
}
const AvroRegistry = { long: BigIntType };
const AvroLogicalTypes = {
    year: YearStringType,
    month: MonthStringType,
    date: DateStringType,
    hour: HourStringType,
};

function makeManifestSchema(spec, schemas) {
    const part_fields = icebergToAvroFields(spec, schemas);
    return {
        type: 'record',
        name: 'manifest_entry',
        fields: [
            { name: 'status', type: 'int', 'field-id': 0 },
            {
                name: 'snapshot_id',
                type: ['null', 'long'],
                default: null,
                'field-id': 1,
            },
            {
                name: 'sequence_number',
                type: ['null', 'long'],
                default: null,
                'field-id': 3,
            },
            {
                name: 'file_sequence_number',
                type: ['null', 'long'],
                default: null,
                'field-id': 4,
            },
            {
                name: 'data_file',
                type: {
                    type: 'record',
                    name: 'r2',
                    fields: [
                        {
                            name: 'content',
                            type: 'int',
                            doc: 'Contents of the file: 0=data, 1=position deletes, 2=equality deletes',
                            'field-id': 134,
                        },
                        {
                            name: 'file_path',
                            type: 'string',
                            doc: 'Location URI with FS scheme',
                            'field-id': 100,
                        },
                        {
                            name: 'file_format',
                            type: 'string',
                            doc: 'File format name: avro, orc, or parquet',
                            'field-id': 101,
                        },
                        {
                            name: 'partition',
                            type: { type: 'record', name: 'r102', fields: part_fields },
                            doc: 'Partition data tuple, schema based on the partition spec',
                            'field-id': 102,
                        },
                        {
                            name: 'record_count',
                            type: 'long',
                            doc: 'Number of records in the file',
                            'field-id': 103,
                        },
                        {
                            name: 'file_size_in_bytes',
                            type: 'long',
                            doc: 'Total file size in bytes',
                            'field-id': 104,
                        },
                        {
                            name: 'column_sizes',
                            type: [
                                'null',
                                {
                                    type: 'array',
                                    items: {
                                        type: 'record',
                                        name: 'k117_v118',
                                        fields: [
                                            { name: 'key', type: 'int', 'field-id': 117 },
                                            { name: 'value', type: 'long', 'field-id': 118 },
                                        ],
                                    },
                                    logicalType: 'map',
                                },
                            ],
                            doc: 'Map of column id to total size on disk',
                            default: null,
                            'field-id': 108,
                        },
                        {
                            name: 'value_counts',
                            type: [
                                'null',
                                {
                                    type: 'array',
                                    items: {
                                        type: 'record',
                                        name: 'k119_v120',
                                        fields: [
                                            { name: 'key', type: 'int', 'field-id': 119 },
                                            { name: 'value', type: 'long', 'field-id': 120 },
                                        ],
                                    },
                                    logicalType: 'map',
                                },
                            ],
                            doc: 'Map of column id to total count, including null and NaN',
                            default: null,
                            'field-id': 109,
                        },
                        {
                            name: 'null_value_counts',
                            type: [
                                'null',
                                {
                                    type: 'array',
                                    items: {
                                        type: 'record',
                                        name: 'k121_v122',
                                        fields: [
                                            { name: 'key', type: 'int', 'field-id': 121 },
                                            { name: 'value', type: 'long', 'field-id': 122 },
                                        ],
                                    },
                                    logicalType: 'map',
                                },
                            ],
                            doc: 'Map of column id to null value count',
                            default: null,
                            'field-id': 110,
                        },
                        {
                            name: 'nan_value_counts',
                            type: [
                                'null',
                                {
                                    type: 'array',
                                    items: {
                                        type: 'record',
                                        name: 'k138_v139',
                                        fields: [
                                            { name: 'key', type: 'int', 'field-id': 138 },
                                            { name: 'value', type: 'long', 'field-id': 139 },
                                        ],
                                    },
                                    logicalType: 'map',
                                },
                            ],
                            doc: 'Map of column id to number of NaN values in the column',
                            default: null,
                            'field-id': 137,
                        },
                        {
                            name: 'lower_bounds',
                            type: [
                                'null',
                                {
                                    type: 'array',
                                    items: {
                                        type: 'record',
                                        name: 'k126_v127',
                                        fields: [
                                            { name: 'key', type: 'int', 'field-id': 126 },
                                            { name: 'value', type: 'bytes', 'field-id': 127 },
                                        ],
                                    },
                                    logicalType: 'map',
                                },
                            ],
                            doc: 'Map of column id to lower bound',
                            default: null,
                            'field-id': 125,
                        },
                        {
                            name: 'upper_bounds',
                            type: [
                                'null',
                                {
                                    type: 'array',
                                    items: {
                                        type: 'record',
                                        name: 'k129_v130',
                                        fields: [
                                            { name: 'key', type: 'int', 'field-id': 129 },
                                            { name: 'value', type: 'bytes', 'field-id': 130 },
                                        ],
                                    },
                                    logicalType: 'map',
                                },
                            ],
                            doc: 'Map of column id to upper bound',
                            default: null,
                            'field-id': 128,
                        },
                        {
                            name: 'key_metadata',
                            type: ['null', 'bytes'],
                            doc: 'Encryption key metadata blob',
                            default: null,
                            'field-id': 131,
                        },
                        {
                            name: 'split_offsets',
                            type: [
                                'null',
                                { type: 'array', items: 'long', 'element-id': 133 },
                            ],
                            doc: 'Splittable offsets',
                            default: null,
                            'field-id': 132,
                        },
                        {
                            name: 'equality_ids',
                            type: [
                                'null',
                                { type: 'array', items: 'int', 'element-id': 136 },
                            ],
                            doc: 'Equality comparison field IDs',
                            default: null,
                            'field-id': 135,
                        },
                        {
                            name: 'sort_order_id',
                            type: ['null', 'int'],
                            doc: 'Sort order ID',
                            default: null,
                            'field-id': 140,
                        },
                    ],
                },
                'field-id': 2,
            },
        ],
    };
}
function makeManifestType(spec, schemas) {
    return avsc__namespace.Type.forSchema(makeManifestSchema(spec, schemas), {
        registry: { ...AvroRegistry },
        logicalTypes: AvroLogicalTypes,
    });
}
const ManifestListSchema = {
    type: 'record',
    name: 'manifest_file',
    fields: [
        {
            name: 'manifest_path',
            type: 'string',
            doc: 'Location URI with FS scheme',
            'field-id': 500,
        },
        {
            name: 'manifest_length',
            type: 'long',
            doc: 'Total file size in bytes',
            'field-id': 501,
        },
        {
            name: 'partition_spec_id',
            type: 'int',
            doc: 'Spec ID used to write',
            'field-id': 502,
        },
        {
            name: 'content',
            type: 'int',
            doc: 'Contents of the manifest: 0=data, 1=deletes',
            'field-id': 517,
        },
        {
            name: 'sequence_number',
            type: 'long',
            doc: 'Sequence number when the manifest was added',
            'field-id': 515,
        },
        {
            name: 'min_sequence_number',
            type: 'long',
            doc: 'Lowest sequence number in the manifest',
            'field-id': 516,
        },
        {
            name: 'added_snapshot_id',
            type: 'long',
            doc: 'Snapshot ID that added the manifest',
            'field-id': 503,
        },
        {
            name: 'added_files_count',
            type: 'int',
            doc: 'Added entry count',
            'field-id': 504,
        },
        {
            name: 'existing_files_count',
            type: 'int',
            doc: 'Existing entry count',
            'field-id': 505,
        },
        {
            name: 'deleted_files_count',
            type: 'int',
            doc: 'Deleted entry count',
            'field-id': 506,
        },
        {
            name: 'added_rows_count',
            type: 'long',
            doc: 'Added rows count',
            'field-id': 512,
        },
        {
            name: 'existing_rows_count',
            type: 'long',
            doc: 'Existing rows count',
            'field-id': 513,
        },
        {
            name: 'deleted_rows_count',
            type: 'long',
            doc: 'Deleted rows count',
            'field-id': 514,
        },
        {
            name: 'partitions',
            type: [
                'null',
                {
                    type: 'array',
                    items: {
                        type: 'record',
                        name: 'r508',
                        fields: [
                            {
                                name: 'contains_null',
                                type: 'boolean',
                                doc: 'True if any file has a null partition value',
                                'field-id': 509,
                            },
                            {
                                name: 'contains_nan',
                                type: ['null', 'boolean'],
                                doc: 'True if any file has a nan partition value',
                                default: null,
                                'field-id': 518,
                            },
                            {
                                name: 'lower_bound',
                                type: ['null', 'bytes'],
                                doc: 'Partition lower bound for all files',
                                default: null,
                                'field-id': 510,
                            },
                            {
                                name: 'upper_bound',
                                type: ['null', 'bytes'],
                                doc: 'Partition upper bound for all files',
                                default: null,
                                'field-id': 511,
                            },
                        ],
                    },
                    'element-id': 508,
                },
            ],
            doc: 'Summary for each partition',
            default: null,
            'field-id': 507,
        },
        {
            name: 'key_metadata',
            type: ['null', 'bytes'],
            doc: 'Encryption key metadata blob',
            default: null,
            'field-id': 519,
        },
    ],
};
const ManifestListType = avsc__namespace.Type.forSchema(ManifestListSchema, {
    registry: { ...AvroRegistry },
});

function _isPrimitive(t) {
    return typeof t === 'string';
}
function _outputType(transform, sourceType) {
    if (transform === 'identity' || transform.startsWith('truncate[')) {
        if (_isPrimitive(sourceType)) {
            return sourceType;
        }
        return null;
    }
    if (transform.startsWith('bucket[')) {
        return 'int';
    }
    if (transform === 'year' ||
        transform === 'month' ||
        transform === 'day' ||
        transform === 'hour') {
        return 'int';
    }
    return null;
}
function _encodeValue(raw, transform, out_type) {
    if (raw === null || transform === null || out_type === null) {
        return null;
    }
    switch (transform) {
        case 'identity': {
            if (Buffer.isBuffer(raw)) {
                if (out_type === 'binary' ||
                    out_type.startsWith('decimal(') ||
                    out_type.startsWith('fixed[')) {
                    return raw;
                }
                throw new Error(`Buffer not allowed for identity with type ${out_type}`);
            }
            switch (out_type) {
                case 'int': {
                    const n = typeof raw === 'number' ? raw : Number(raw);
                    const buf = Buffer.alloc(4);
                    buf.writeInt32LE(Math.floor(n));
                    return buf;
                }
                case 'long': {
                    const n = typeof raw === 'bigint' ? raw : BigInt(raw);
                    const buf = Buffer.alloc(8);
                    buf.writeBigInt64LE(n);
                    return buf;
                }
                case 'float': {
                    const n = typeof raw === 'number' ? raw : Number(raw);
                    const buf = Buffer.alloc(4);
                    buf.writeFloatLE(n);
                    return buf;
                }
                case 'double': {
                    const n = typeof raw === 'number' ? raw : Number(raw);
                    const buf = Buffer.alloc(8);
                    buf.writeDoubleLE(n);
                    return buf;
                }
                case 'string':
                case 'uuid': {
                    const s = typeof raw === 'string' ? raw : String(raw);
                    return Buffer.from(s, 'utf8');
                }
                case 'boolean': {
                    const buf = Buffer.alloc(1);
                    buf.writeUInt8(raw ? 1 : 0);
                    return buf;
                }
                case 'binary':
                case 'date':
                case 'time':
                case 'timestamp':
                case 'timestamptz':
                    throw new Error(`Identity not implemented for type ${out_type}`);
                default:
                    throw new Error(`Identity not implemented for type ${out_type}`);
            }
        }
        case 'year':
        case 'month':
        case 'day':
        case 'hour': {
            let n;
            if (typeof raw === 'string') {
                const d = new Date(raw);
                if (transform === 'year') {
                    n = d.getUTCFullYear();
                }
                else if (transform === 'month') {
                    n = d.getUTCFullYear() * 12 + d.getUTCMonth();
                }
                else if (transform === 'day') {
                    n = Math.floor(d.getTime() / (24 * 3600 * 1000));
                }
                else {
                    n = Math.floor(d.getTime() / (3600 * 1000));
                }
            }
            else if (typeof raw === 'number' || typeof raw === 'bigint') {
                n = Number(raw);
            }
            else {
                throw new Error(`${transform} requires string|number|bigint`);
            }
            const buf = Buffer.alloc(4);
            buf.writeInt32LE(n);
            return buf;
        }
        default:
            if (transform.startsWith('bucket[')) {
                if (typeof raw !== 'number') {
                    throw new Error('bucket requires number input');
                }
                const buf = Buffer.alloc(4);
                buf.writeInt32LE(raw);
                return buf;
            }
            if (transform.startsWith('truncate[')) {
                if (typeof raw !== 'string') {
                    throw new Error('truncate requires string input');
                }
                const width = Number(/\d+/.exec(transform)?.[0]);
                return Buffer.from(raw.substring(0, width), 'utf8');
            }
            throw new Error(`Unsupported transform ${transform}`);
    }
}
const NaNValue = NaN;
function makeBounds(partitions, spec, schema) {
    return spec.fields.map((f) => {
        const schemaField = schema.fields.find((sf) => sf.id === f['source-id']);
        if (!schemaField) {
            throw new Error(`Schema field not found for source-id ${f['source-id']}`);
        }
        if (!(f.name in partitions)) {
            throw new Error(`partitions missing ${f.name}`);
        }
        const raw = partitions[f.name];
        if (typeof raw === 'number' && isNaN(raw)) {
            return NaNValue;
        }
        if (raw === null || raw === undefined) {
            return null;
        }
        const out_type = _outputType(f.transform, schemaField.type);
        return _encodeValue(raw, f.transform, out_type);
    });
}

function isRawRecordSchema(schema) {
    return (typeof schema === 'object' &&
        schema !== null &&
        'type' in schema &&
        schema.type === 'record' &&
        'fields' in schema);
}
function isRawArraySchema(schema) {
    return (typeof schema === 'object' &&
        schema !== null &&
        'type' in schema &&
        schema.type === 'array');
}
function isRawMapSchema(schema) {
    return (typeof schema === 'object' &&
        schema !== null &&
        'type' in schema &&
        schema.type === 'map');
}
function isRawUnionSchema(schema) {
    return Array.isArray(schema);
}
function translateRecord(sourceSchema, targetSchema, record) {
    return translateValue(sourceSchema, targetSchema, record);
}
function translateValue(sourceSchema, targetSchema, value) {
    if (value === null || value === undefined) {
        return value;
    }
    // Handle unions
    if (isRawUnionSchema(targetSchema)) {
        for (const targetBranch of targetSchema) {
            if (isRawUnionSchema(sourceSchema)) {
                for (const sourceBranch of sourceSchema) {
                    try {
                        return translateValue(sourceBranch, targetBranch, value);
                    }
                    catch {
                        // Try next branch
                    }
                }
            }
            else {
                try {
                    return translateValue(sourceSchema, targetBranch, value);
                }
                catch {
                    // Try next branch
                }
            }
        }
        return value;
    }
    if (isRawUnionSchema(sourceSchema)) {
        for (const sourceBranch of sourceSchema) {
            try {
                return translateValue(sourceBranch, targetSchema, value);
            }
            catch {
                // Try next branch
            }
        }
    }
    // Handle primitives
    if (typeof sourceSchema === 'string' && typeof targetSchema === 'string') {
        return value;
    }
    // Handle records
    if (isRawRecordSchema(sourceSchema) && isRawRecordSchema(targetSchema)) {
        return translateRecordValue(sourceSchema, targetSchema, value);
    }
    // Handle arrays
    if (isRawArraySchema(sourceSchema) && isRawArraySchema(targetSchema)) {
        if (!Array.isArray(value)) {
            return value;
        }
        return value.map((item) => translateValue(sourceSchema.items, targetSchema.items, item));
    }
    // Handle maps
    if (isRawMapSchema(sourceSchema) && isRawMapSchema(targetSchema)) {
        if (typeof value !== 'object') {
            return value;
        }
        const result = {};
        for (const [key, val] of Object.entries(value)) {
            result[key] = translateValue(sourceSchema.values, targetSchema.values, val);
        }
        return result;
    }
    return value;
}
function translateRecordValue(sourceSchema, targetSchema, record) {
    if (typeof record !== 'object' || record === null) {
        return record;
    }
    const sourceRecord = record;
    const result = {};
    // Build field maps from source
    const sourceFieldById = new Map();
    const sourceFieldByName = new Map();
    for (const field of sourceSchema.fields) {
        if (field['field-id'] !== undefined) {
            sourceFieldById.set(field['field-id'], field);
        }
        sourceFieldByName.set(field.name, field);
    }
    // Translate each target field
    for (const targetField of targetSchema.fields) {
        let sourceField;
        let sourceValue;
        // Match by field-id first
        if (targetField['field-id'] !== undefined) {
            sourceField = sourceFieldById.get(targetField['field-id']);
            if (sourceField) {
                sourceValue = sourceRecord[sourceField.name];
            }
        }
        // Fall back to name match
        if (sourceField === undefined) {
            sourceField = sourceFieldByName.get(targetField.name);
            if (sourceField) {
                sourceValue = sourceRecord[sourceField.name];
            }
        }
        // Handle missing source field or value
        if (sourceField === undefined) {
            if ('default' in targetField) {
                result[targetField.name] = targetField.default;
            }
        }
        else if (sourceValue === undefined) {
            if ('default' in targetField) {
                result[targetField.name] = targetField.default;
            }
        }
        else {
            // Translate the value
            result[targetField.name] = translateValue(sourceField.type, targetField.type, sourceValue);
        }
    }
    return result;
}

const S3_REGEX = /^s3:\/\/([^/]+)\/(.+)$/;
function parseS3Url(url) {
    const match = S3_REGEX.exec(url);
    if (!match) {
        throw new Error('Invalid S3 URL');
    }
    return { bucket: match[1], key: match[2] };
}
const g_s3Map = new Map();
const g_s3TablesMap = new Map();
function getS3Client(params) {
    const { region, credentials } = params;
    let ret = g_s3Map.get(region)?.get(credentials);
    if (!ret) {
        const opts = {};
        if (region) {
            opts.region = region;
        }
        if (credentials) {
            opts.credentials = credentials;
        }
        ret = new clientS3.S3Client(opts);
        _setMap(g_s3Map, region, credentials, ret);
    }
    return ret;
}
function getS3TablesClient(params) {
    const { region, credentials } = params;
    let ret = g_s3TablesMap.get(region)?.get(credentials);
    if (!ret) {
        const opts = {};
        if (region) {
            opts.region = region;
        }
        if (credentials) {
            opts.credentials = credentials;
        }
        ret = new clientS3tables.S3TablesClient(opts);
        _setMap(g_s3TablesMap, region, credentials, ret);
    }
    return ret;
}
function _setMap(map, region, credentials, client) {
    let region_map = map.get(region);
    region_map ??= new Map();
    region_map.set(credentials, client);
}
async function writeS3File(params) {
    const { credentials, region, bucket, key, body } = params;
    const s3 = getS3Client({ region, credentials });
    const command = new clientS3.PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: body,
    });
    await s3.send(command);
}
async function updateManifestList(params) {
    const { region, credentials, bucket, key, outKey, prepend } = params;
    const metadata = params.metadata
        ? fixupMetadata(params.metadata)
        : params.metadata;
    const s3 = getS3Client({ region, credentials });
    const get = new clientS3.GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3.send(get);
    const source = response.Body;
    if (!source) {
        throw new Error('failed to get source manifest list');
    }
    const passthrough = new node_stream.PassThrough();
    let sourceSchema;
    const decoder = new avsc__namespace.streams.BlockDecoder({
        codecs: { deflate: zlib__namespace.inflateRaw },
        parseHook(schema) {
            sourceSchema = schema;
            return avsc__namespace.Type.forSchema(schema, {
                registry: { ...AvroRegistry },
            });
        },
    });
    const encoder = new avsc__namespace.streams.BlockEncoder(ManifestListType, {
        codec: 'deflate',
        codecs: { deflate: zlib__namespace.deflateRaw },
        metadata,
    });
    encoder.pipe(passthrough);
    for (const record of prepend) {
        encoder.write(record);
    }
    const upload = new libStorage.Upload({
        client: s3,
        params: { Bucket: bucket, Key: outKey, Body: passthrough },
    });
    const stream_promise = new Promise((resolve, reject) => {
        source.on('error', (err) => {
            reject(err);
        });
        passthrough.on('error', (err) => {
            reject(err);
        });
        encoder.on('error', (err) => {
            reject(err);
        });
        decoder.on('error', (err) => {
            reject(err);
        });
        decoder.on('data', (record) => {
            const translated = translateRecord(sourceSchema, ManifestListSchema, record);
            if (translated.content !== ListContent.DATA ||
                translated.added_files_count > 0 ||
                translated.existing_files_count > 0) {
                if (!encoder.write(translated)) {
                    decoder.pause();
                    encoder.once('drain', () => decoder.resume());
                }
            }
        });
        decoder.on('end', () => {
            encoder.end();
        });
        encoder.on('finish', () => {
            resolve();
        });
        source.pipe(decoder);
    });
    await Promise.all([stream_promise, upload.done()]);
}
async function streamWriteAvro(params) {
    const { region, credentials, bucket, key } = params;
    const metadata = params.metadata
        ? fixupMetadata(params.metadata)
        : params.metadata;
    const s3 = getS3Client({ region, credentials });
    const encoder = new avsc__namespace.streams.BlockEncoder(params.avroType, {
        codec: 'deflate',
        codecs: { deflate: zlib__namespace.deflateRaw },
        metadata,
    });
    const upload = new libStorage.Upload({
        client: s3,
        params: { Bucket: bucket, Key: key, Body: encoder },
    });
    let file_size = 0;
    upload.on('httpUploadProgress', (progress) => {
        if (progress.loaded) {
            file_size = progress.loaded;
        }
    });
    const stream_promise = new Promise((resolve, reject) => {
        encoder.on('error', (err) => {
            reject(err);
        });
        encoder.on('finish', () => {
            resolve();
        });
    });
    for await (const batch of params.iter) {
        for (const record of batch) {
            encoder.write(record);
        }
    }
    encoder.end();
    await Promise.all([stream_promise, upload.done()]);
    return file_size;
}
async function downloadAvro(params) {
    const { region, credentials, bucket, key, avroSchema } = params;
    const s3 = getS3Client({ region, credentials });
    const get = new clientS3.GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3.send(get);
    const source = response.Body;
    if (!source) {
        throw new Error('failed to get source manifest list');
    }
    let sourceSchema;
    const decoder = new avsc__namespace.streams.BlockDecoder({
        codecs: { deflate: zlib__namespace.inflateRaw },
        parseHook(schema) {
            sourceSchema = schema;
            return avsc__namespace.Type.forSchema(schema, {
                registry: { ...AvroRegistry },
            });
        },
    });
    const records = [];
    const stream_promise = new Promise((resolve, reject) => {
        source.on('error', (err) => {
            reject(err);
        });
        decoder.on('error', (err) => {
            reject(err);
        });
        decoder.on('data', (record) => {
            const translated = translateRecord(sourceSchema, avroSchema, record);
            records.push(translated);
        });
        decoder.on('end', () => {
            resolve();
        });
        source.pipe(decoder);
    });
    await stream_promise;
    return records;
}

async function addManifest(params) {
    const { credentials, region, metadata } = params;
    const bucket = metadata.location.split('/').slice(-1)[0];
    const schema = metadata.schemas.find((s) => s['schema-id'] === params.schemaId);
    const spec = metadata['partition-specs'].find((p) => p['spec-id'] === params.specId);
    if (!bucket) {
        throw new Error('bad manifest location');
    }
    if (!schema) {
        throw new Error('schema not found');
    }
    if (!spec) {
        throw new Error('partition spec not found');
    }
    if (!params.files[0]) {
        throw new Error('must have at least 1 file');
    }
    let added_rows_count = 0n;
    const partitions = spec.fields.map(() => ({
        contains_null: false,
        contains_nan: false,
        upper_bound: null,
        lower_bound: null,
    }));
    const records = params.files.map((file) => {
        added_rows_count += file.recordCount;
        const bounds = makeBounds(file.partitions, spec, schema);
        for (let i = 0; i < partitions.length; i++) {
            const part = partitions[i];
            const bound = bounds[i];
            if (!part) {
                throw new Error('impossible');
            }
            else if (bound === null) {
                part.contains_null = true;
            }
            else if (Buffer.isBuffer(bound)) {
                part.upper_bound = _maxBuffer(part.upper_bound ?? null, bound);
                part.lower_bound = _minBuffer(part.lower_bound ?? null, bound);
            }
            else {
                part.contains_nan = true;
            }
        }
        return {
            status: ManifestFileStatus.ADDED,
            snapshot_id: params.snapshotId,
            sequence_number: params.sequenceNumber,
            file_sequence_number: params.sequenceNumber,
            data_file: {
                content: DataFileContent.DATA,
                file_path: file.file,
                file_format: 'PARQUET',
                record_count: file.recordCount,
                file_size_in_bytes: file.fileSize,
                partition: file.partitions,
                column_sizes: _transformRecord(schema, file.columnSizes),
                value_counts: _transformRecord(schema, file.valueCounts),
                null_value_counts: _transformRecord(schema, file.nullValueCounts),
                nan_value_counts: _transformRecord(schema, file.nanValueCounts),
                lower_bounds: _transformRecord(schema, file.lowerBounds),
                upper_bounds: _transformRecord(schema, file.upperBounds),
                key_metadata: file.keyMetadata ?? null,
                split_offsets: file.splitOffsets ?? null,
                equality_ids: file.equalityIds ?? null,
                sort_order_id: file.sortOrderId ?? null,
            },
        };
    });
    const manifest_type = makeManifestType(spec, [schema]);
    const manifest_buf = await avroToBuffer({
        type: manifest_type,
        metadata: {
            'partition-spec-id': String(params.specId),
            'partition-spec': JSON.stringify(spec.fields),
        },
        records,
    });
    const manifest_key = `metadata/${node_crypto.randomUUID()}.avro`;
    await writeS3File({
        credentials,
        region,
        bucket,
        key: manifest_key,
        body: manifest_buf,
    });
    const manifest_record = {
        manifest_path: `s3://${bucket}/${manifest_key}`,
        manifest_length: BigInt(manifest_buf.length),
        partition_spec_id: params.specId,
        content: ListContent.DATA,
        sequence_number: params.sequenceNumber,
        min_sequence_number: params.sequenceNumber,
        added_snapshot_id: params.snapshotId,
        added_files_count: params.files.length,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count,
        existing_rows_count: 0n,
        deleted_rows_count: 0n,
        partitions,
    };
    return manifest_record;
}
function _transformRecord(schema, map) {
    if (!map) {
        return null;
    }
    const ret = [];
    for (const field of schema.fields) {
        const value = map[field.name];
        if (value !== undefined) {
            ret.push({ key: field.id, value });
        }
    }
    return ret.length > 0 ? ret : null;
}
function _minBuffer(a, b) {
    if (!a && !b) {
        return null;
    }
    else if (!a) {
        return b;
    }
    else if (!b) {
        return a;
    }
    return Buffer.compare(a, b) <= 0 ? a : b;
}
function _maxBuffer(a, b) {
    if (!a && !b) {
        return null;
    }
    else if (!a) {
        return b;
    }
    else if (!b) {
        return a;
    }
    return Buffer.compare(a, b) >= 0 ? a : b;
}

function customNumberParser(value) {
    if (LosslessJson__namespace.isInteger(value)) {
        if (LosslessJson__namespace.isSafeNumber(value)) {
            return parseInt(value, 10);
        }
        return BigInt(value);
    }
    return parseFloat(value);
}
function parse(text) {
    return LosslessJson__namespace.parse(text, null, customNumberParser);
}

class IcebergHttpError extends Error {
    status;
    text;
    body;
    constructor(status, body, message) {
        super(message);
        this.status = status;
        if (typeof body === 'string') {
            this.text = body;
        }
        else if (body && typeof body === 'object') {
            this.body = body;
        }
    }
}
async function icebergRequest(params) {
    const region = params.tableBucketARN.split(':')[3];
    if (!region) {
        throw new Error('bad tableBucketARN');
    }
    const arn = encodeURIComponent(params.tableBucketARN);
    const hostname = `s3tables.${region}.amazonaws.com`;
    const full_path = `/iceberg/v1/${arn}${params.suffix}`;
    const body = params.body ? LosslessJson.stringify(params.body) : null;
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
    const text = await res.text();
    const ret = res.headers.get('content-type') === 'application/json'
        ? _parse(text)
        : text;
    if (!res.ok) {
        if (res.status) {
            throw new IcebergHttpError(res.status, ret, `request failed: ${res.statusText} ${text}`);
        }
        throw new Error(`request failed: ${res.statusText} ${text}`);
    }
    return ret;
}
function _parse(text) {
    try {
        return parse(text);
    }
    catch {
        return text;
    }
}

async function getMetadata(params) {
    if ('tableBucketARN' in params) {
        const icebergResponse = await icebergRequest({
            credentials: params.credentials,
            tableBucketARN: params.tableBucketARN,
            method: 'GET',
            suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
        });
        if (icebergResponse.metadata) {
            return icebergResponse.metadata;
        }
        throw new Error('invalid table metadata');
    }
    const { ...other } = params;
    const client = getS3TablesClient(params);
    const get_table_cmd = new clientS3tables.GetTableCommand(other);
    const response = await client.send(get_table_cmd);
    if (!response.metadataLocation) {
        throw new Error('missing metadataLocation');
    }
    const s3_client = getS3Client(params);
    const { key, bucket } = parseS3Url(response.metadataLocation);
    const get_file_cmd = new clientS3.GetObjectCommand({ Bucket: bucket, Key: key });
    const file_response = await s3_client.send(get_file_cmd);
    const body = await file_response.Body?.transformToString();
    if (!body) {
        throw new Error('missing body');
    }
    return parse(body);
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
async function removeSnapshots(params) {
    return icebergRequest({
        tableBucketARN: params.tableBucketARN,
        method: 'POST',
        suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
        body: {
            requirements: [],
            updates: [
                { action: 'remove-snapshots', 'snapshot-ids': params.snapshotIds },
            ],
        },
    });
}

const DEFAULT_RETRY_COUNT$1 = 5;
async function addDataFiles(params) {
    const { credentials } = params;
    const retry_max = params.retryCount ?? DEFAULT_RETRY_COUNT$1;
    const region = params.tableBucketARN.split(':')[3];
    if (!region) {
        throw new Error('bad tableBucketARN');
    }
    const snapshot_id = params.snapshotId ?? _randomBigInt64$1();
    const metadata = await getMetadata(params);
    const bucket = metadata.location.split('/').slice(-1)[0];
    const parent_snapshot_id = BigInt(metadata['current-snapshot-id']);
    const snapshot = metadata.snapshots.find((s) => BigInt(s['snapshot-id']) === parent_snapshot_id) ?? null;
    if (!bucket) {
        throw new Error('bad manifest location');
    }
    if (parent_snapshot_id > 0n && !snapshot) {
        throw new Error('no old snapshot');
    }
    let old_list_key = snapshot ? parseS3Url(snapshot['manifest-list']).key : '';
    if (snapshot && !old_list_key) {
        throw new Error('last snapshot invalid');
    }
    let sequence_number = BigInt(metadata['last-sequence-number']) + 1n;
    let remove_snapshot_id = 0n;
    if (params.maxSnapshots && metadata.snapshots.length >= params.maxSnapshots) {
        let earliest_time = 0;
        for (const snap of metadata.snapshots) {
            const snap_time = snap['timestamp-ms'];
            if (earliest_time === 0 || snap_time < earliest_time) {
                earliest_time = snap_time;
                remove_snapshot_id = BigInt(snap['snapshot-id']);
            }
        }
    }
    let added_files = 0;
    let added_records = 0n;
    let added_size = 0n;
    const records = await Promise.all(params.lists.map(async (list) => {
        added_files += list.files.length;
        for (const file of list.files) {
            added_records += file.recordCount;
            added_size += file.fileSize;
        }
        const opts = {
            credentials,
            region,
            metadata,
            schemaId: list.schemaId,
            specId: list.specId,
            snapshotId: snapshot_id,
            sequenceNumber: sequence_number,
            files: list.files,
        };
        return addManifest(opts);
    }));
    let expected_snapshot_id = parent_snapshot_id;
    for (let try_count = 0;; try_count++) {
        const manifest_list_key = `metadata/${node_crypto.randomUUID()}.avro`;
        const manifest_list_url = `s3://${bucket}/${manifest_list_key}`;
        if (old_list_key) {
            await updateManifestList({
                credentials,
                region,
                bucket,
                key: old_list_key,
                outKey: manifest_list_key,
                metadata: {
                    'sequence-number': String(sequence_number),
                    'snapshot-id': String(snapshot_id),
                    'parent-snapshot-id': String(parent_snapshot_id),
                },
                prepend: records,
            });
        }
        else {
            const manifest_list_buf = await avroToBuffer({
                type: ManifestListType,
                metadata: {
                    'sequence-number': String(sequence_number),
                    'snapshot-id': String(snapshot_id),
                    'parent-snapshot-id': 'null',
                },
                records,
            });
            await writeS3File({
                credentials,
                region,
                bucket,
                key: manifest_list_key,
                body: manifest_list_buf,
            });
        }
        try {
            const updates = [
                {
                    action: 'add-snapshot',
                    snapshot: {
                        'sequence-number': sequence_number,
                        'snapshot-id': snapshot_id,
                        'parent-snapshot-id': parent_snapshot_id,
                        'timestamp-ms': Date.now(),
                        summary: {
                            operation: 'append',
                            'added-data-files': String(added_files),
                            'added-records': String(added_records),
                            'added-files-size': String(added_size),
                        },
                        'manifest-list': manifest_list_url,
                        'schema-id': metadata['current-schema-id'],
                    },
                },
                {
                    action: 'set-snapshot-ref',
                    'snapshot-id': snapshot_id,
                    type: 'branch',
                    'ref-name': 'main',
                },
            ];
            if (remove_snapshot_id > 0n) {
                updates.push({
                    action: 'remove-snapshots',
                    'snapshot-ids': [remove_snapshot_id],
                });
            }
            const result = await icebergRequest({
                credentials: params.credentials,
                tableBucketARN: params.tableBucketARN,
                method: 'POST',
                suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
                body: {
                    requirements: expected_snapshot_id > 0n
                        ? [
                            {
                                type: 'assert-ref-snapshot-id',
                                ref: 'main',
                                'snapshot-id': expected_snapshot_id,
                            },
                        ]
                        : [],
                    updates,
                },
            });
            return {
                result,
                retriesNeeded: try_count,
                parentSnapshotId: parent_snapshot_id,
                snapshotId: snapshot_id,
                sequenceNumber: sequence_number,
            };
        }
        catch (e) {
            if (e instanceof IcebergHttpError &&
                e.status === 409 &&
                try_count < retry_max) {
                // retry case
                remove_snapshot_id = 0n;
            }
            else {
                throw e;
            }
        }
        // we do a merge in the append only simultanious case
        const conflict_metadata = await getMetadata(params);
        const conflict_snapshot_id = BigInt(conflict_metadata['current-snapshot-id']);
        if (conflict_snapshot_id <= 0n) {
            throw new Error('conflict');
        }
        const conflict_snap = conflict_metadata.snapshots.find((s) => s['snapshot-id'] === conflict_snapshot_id);
        if (!conflict_snap) {
            throw new Error('conflict');
        }
        if (conflict_snap.summary.operation === 'append' &&
            BigInt(conflict_snap['sequence-number']) === sequence_number) {
            old_list_key = parseS3Url(conflict_snap['manifest-list']).key;
            if (!old_list_key) {
                throw new Error('conflict');
            }
            added_files += parseInt(conflict_snap.summary['added-data-files'] ?? '0', 10);
            added_records += BigInt(conflict_snap.summary['added-records'] ?? '0');
            added_size += BigInt(conflict_snap.summary['added-files-size'] ?? '0');
            expected_snapshot_id = conflict_snapshot_id;
            sequence_number++;
        }
        else {
            throw new Error('conflict');
        }
    }
}
function _randomBigInt64$1() {
    const bytes = node_crypto.randomBytes(8);
    let ret = bytes.readBigUInt64BE();
    ret &= BigInt('0x7FFFFFFFFFFFFFFF');
    if (ret === 0n) {
        ret = 1n;
    }
    return ret;
}

const DEFAULT_RETRY_COUNT = 5;
async function submitSnapshot(params) {
    const { snapshotId, parentSnapshotId, resolveConflict } = params;
    let { sequenceNumber, removeSnapshotId, manifestListUrl, summary } = params;
    const retry_max = params.retryCount ?? DEFAULT_RETRY_COUNT;
    let expected_snapshot_id = parentSnapshotId;
    let conflict_snap;
    for (let try_count = 0;; try_count++) {
        if (conflict_snap && resolveConflict) {
            const resolve_result = await resolveConflict(conflict_snap);
            summary = resolve_result.summary;
            manifestListUrl = resolve_result.manifestListUrl;
        }
        else if (conflict_snap) {
            throw new Error('conflict');
        }
        try {
            const updates = [
                {
                    action: 'add-snapshot',
                    snapshot: {
                        'sequence-number': sequenceNumber,
                        'snapshot-id': snapshotId,
                        'parent-snapshot-id': parentSnapshotId,
                        'timestamp-ms': Date.now(),
                        summary,
                        'manifest-list': manifestListUrl,
                        'schema-id': params.currentSchemaId,
                    },
                },
                {
                    action: 'set-snapshot-ref',
                    'snapshot-id': snapshotId,
                    type: 'branch',
                    'ref-name': 'main',
                },
            ];
            if (removeSnapshotId && removeSnapshotId > 0n) {
                updates.push({
                    action: 'remove-snapshots',
                    'snapshot-ids': [removeSnapshotId],
                });
            }
            const result = await icebergRequest({
                credentials: params.credentials,
                tableBucketARN: params.tableBucketARN,
                method: 'POST',
                suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
                body: {
                    requirements: expected_snapshot_id > 0n
                        ? [
                            {
                                type: 'assert-ref-snapshot-id',
                                ref: 'main',
                                'snapshot-id': expected_snapshot_id,
                            },
                        ]
                        : [],
                    updates,
                },
            });
            return {
                result,
                retriesNeeded: try_count,
                parentSnapshotId,
                snapshotId,
                sequenceNumber,
            };
        }
        catch (e) {
            if (e instanceof IcebergHttpError &&
                e.status === 409 &&
                try_count < retry_max) {
                // retry case
                removeSnapshotId = 0n;
            }
            else {
                throw e;
            }
        }
        // we do a merge in the append only simultanious case
        const conflict_metadata = await getMetadata(params);
        const conflict_snapshot_id = BigInt(conflict_metadata['current-snapshot-id']);
        if (conflict_snapshot_id <= 0n) {
            throw new Error('conflict');
        }
        conflict_snap = conflict_metadata.snapshots.find((s) => s['snapshot-id'] === conflict_snapshot_id);
        if (!conflict_snap) {
            throw new Error('conflict');
        }
        if (conflict_snap.summary.operation === 'append' &&
            BigInt(conflict_snap['sequence-number']) === sequenceNumber) {
            expected_snapshot_id = conflict_snapshot_id;
            sequenceNumber++;
        }
        else {
            throw new Error('conflict');
        }
    }
}
async function setCurrentCommit(params) {
    const commit_result = await icebergRequest({
        credentials: params.credentials,
        tableBucketARN: params.tableBucketARN,
        method: 'POST',
        suffix: `/namespaces/${params.namespace}/tables/${params.name}`,
        body: {
            requirements: [],
            updates: [
                {
                    action: 'set-snapshot-ref',
                    'snapshot-id': params.snapshotId,
                    type: 'branch',
                    'ref-name': 'main',
                },
            ],
        },
    });
    return commit_result;
}

async function* asyncIterMap(items, func) {
    const pending = new Set();
    for (const item of items) {
        const ref = {};
        const wrapper = func(item).then((value) => ({
            self: ref.current,
            value,
        }));
        ref.current = wrapper;
        pending.add(wrapper);
    }
    while (pending.size) {
        const { self, value } = await Promise.race(pending);
        if (self) {
            pending.delete(self);
        }
        yield value;
    }
}

async function manifestCompact(params) {
    const { credentials, targetCount, calculateWeight } = params;
    const region = params.tableBucketARN.split(':')[3];
    if (!region) {
        throw new Error('bad tableBucketARN');
    }
    const snapshot_id = params.snapshotId ?? _randomBigInt64();
    const metadata = await getMetadata(params);
    const bucket = metadata.location.split('/').slice(-1)[0];
    const parent_snapshot_id = BigInt(metadata['current-snapshot-id']);
    const snapshot = metadata.snapshots.find((s) => BigInt(s['snapshot-id']) === parent_snapshot_id) ?? null;
    if (!bucket) {
        throw new Error('bad manifest location');
    }
    if (!snapshot) {
        return {
            result: {},
            retriesNeeded: 0,
            parentSnapshotId: parent_snapshot_id,
            snapshotId: 0n,
            sequenceNumber: 0n,
            changed: false,
            outputManifestCount: 0,
        };
    }
    if (parent_snapshot_id <= 0n) {
        throw new Error('no old snapshot');
    }
    const old_list_key = parseS3Url(snapshot['manifest-list']).key;
    if (!old_list_key) {
        throw new Error('last snapshot invalid');
    }
    const sequence_number = BigInt(metadata['last-sequence-number']) + 1n;
    let remove_snapshot_id = 0n;
    if (params.maxSnapshots && metadata.snapshots.length >= params.maxSnapshots) {
        let earliest_time = 0;
        for (const snap of metadata.snapshots) {
            const snap_time = snap['timestamp-ms'];
            if (earliest_time === 0 || snap_time < earliest_time) {
                earliest_time = snap_time;
                remove_snapshot_id = BigInt(snap['snapshot-id']);
            }
        }
    }
    const list = await downloadAvro({
        credentials,
        region,
        bucket,
        key: old_list_key,
        avroSchema: ManifestListSchema,
    });
    const filtered = list.filter(_filterDeletes);
    const groups = _groupList(filtered, (a, b) => {
        if (a.content === ListContent.DATA &&
            b.content === ListContent.DATA &&
            a.deleted_files_count === 0 &&
            b.deleted_files_count === 0 &&
            a.partition_spec_id === b.partition_spec_id) {
            return (!a.partitions ||
                a.partitions.every((part, i) => {
                    const other = b.partitions?.[i];
                    return (other &&
                        (part.upper_bound === other.upper_bound ||
                            (part.upper_bound &&
                                other.upper_bound &&
                                Buffer.compare(part.upper_bound, other.upper_bound) === 0)) &&
                        (part.lower_bound === other.lower_bound ||
                            (part.lower_bound &&
                                other.lower_bound &&
                                Buffer.compare(part.lower_bound, other.lower_bound) === 0)));
                }));
        }
        return false;
    });
    const final_groups = targetCount !== undefined &&
        calculateWeight !== undefined &&
        groups.length > targetCount
        ? _combineWeightGroups(groups, targetCount, calculateWeight)
        : groups;
    if (final_groups.length === list.length && !params.forceRewrite) {
        return {
            result: {},
            retriesNeeded: 0,
            parentSnapshotId: parent_snapshot_id,
            snapshotId: 0n,
            sequenceNumber: sequence_number,
            changed: false,
            outputManifestCount: 0,
        };
    }
    const manifest_list_key = `metadata/${node_crypto.randomUUID()}.avro`;
    const iter = asyncIterMap(final_groups, async (group) => {
        if (!group[0]) {
            return [];
        }
        const { partition_spec_id } = group[0];
        const spec = metadata['partition-specs'].find((p) => p['spec-id'] === partition_spec_id);
        if (!spec) {
            throw new Error(`Partition spec not found: ${partition_spec_id}`);
        }
        return _combineGroup({
            credentials,
            region,
            bucket,
            group,
            spec,
            snapshotId: snapshot_id,
            schemas: metadata.schemas,
            sequenceNumber: sequence_number,
            forceRewrite: params.forceRewrite ?? false,
        });
    });
    await streamWriteAvro({
        credentials,
        region,
        bucket,
        key: manifest_list_key,
        metadata: {
            'sequence-number': String(sequence_number),
            'snapshot-id': String(snapshot_id),
            'parent-snapshot-id': String(parent_snapshot_id),
        },
        avroType: ManifestListType,
        iter,
    });
    const summary = {
        operation: 'replace',
        'added-data-files': '0',
        'deleted-data-files': '0',
        'added-records': '0',
        'deleted-records': '0',
        'added-files-size': '0',
        'removed-files-size': '0',
        'changed-partition-count': '0',
    };
    const snap_result = await submitSnapshot({
        credentials,
        tableBucketARN: params.tableBucketARN,
        namespace: params.namespace,
        name: params.name,
        currentSchemaId: metadata['current-schema-id'],
        parentSnapshotId: parent_snapshot_id,
        snapshotId: snapshot_id,
        sequenceNumber: sequence_number,
        manifestListUrl: `s3://${bucket}/${manifest_list_key}`,
        summary,
        removeSnapshotId: remove_snapshot_id,
        retryCount: params.retryCount,
    });
    return {
        ...snap_result,
        changed: true,
        outputManifestCount: final_groups.length,
    };
}
async function _combineGroup(params) {
    const { credentials, region, bucket, group } = params;
    const record0 = group[0];
    if ((group.length === 1 && !params.forceRewrite) || !record0) {
        return group;
    }
    const key = `metadata/${node_crypto.randomUUID()}.avro`;
    const schema = makeManifestSchema(params.spec, params.schemas);
    const type = makeManifestType(params.spec, params.schemas);
    const iter = asyncIterMap(group, async (record) => {
        return _streamReadManifest({
            credentials,
            region,
            bucket,
            url: record.manifest_path,
            schema,
        });
    });
    const manifest_length = await streamWriteAvro({
        credentials,
        region,
        bucket,
        key,
        metadata: {
            'partition-spec-id': String(params.spec['spec-id']),
            'partition-spec': JSON.stringify(params.spec.fields),
        },
        avroType: type,
        iter,
    });
    const ret = {
        manifest_path: `s3://${bucket}/${key}`,
        manifest_length: BigInt(manifest_length),
        partition_spec_id: record0.partition_spec_id,
        content: record0.content,
        sequence_number: params.sequenceNumber,
        min_sequence_number: params.sequenceNumber,
        added_snapshot_id: params.snapshotId,
        added_files_count: 0,
        existing_files_count: 0,
        deleted_files_count: 0,
        added_rows_count: 0n,
        existing_rows_count: 0n,
        deleted_rows_count: 0n,
        partitions: record0.partitions ?? null,
    };
    for (const record of group) {
        ret.added_files_count += record.added_files_count;
        ret.existing_files_count += record.existing_files_count;
        ret.deleted_files_count += record.deleted_files_count;
        ret.added_rows_count += record.added_rows_count;
        ret.existing_rows_count += record.existing_rows_count;
        ret.deleted_rows_count += record.deleted_rows_count;
        ret.min_sequence_number = _bigintMin(ret.min_sequence_number, record.min_sequence_number);
    }
    for (let i = 1; i < group.length; i++) {
        const parts = group[i]?.partitions;
        if (ret.partitions && parts) {
            for (let j = 0; j < parts.length; j++) {
                const part = parts[j];
                const ret_part = ret.partitions[j];
                if (part && ret_part) {
                    ret_part.contains_null ||= part.contains_null;
                    if (part.contains_nan !== undefined) {
                        ret_part.contains_nan =
                            (ret_part.contains_nan ?? false) || part.contains_nan;
                    }
                    if (!ret_part.upper_bound ||
                        (part.upper_bound &&
                            Buffer.compare(part.upper_bound, ret_part.upper_bound) > 0)) {
                        ret_part.upper_bound = part.upper_bound ?? null;
                    }
                    if (!ret_part.lower_bound ||
                        (part.lower_bound &&
                            Buffer.compare(part.lower_bound, ret_part.lower_bound) < 0)) {
                        ret_part.lower_bound = part.lower_bound ?? null;
                    }
                }
            }
        }
        else if (parts) {
            ret.partitions = parts;
        }
    }
    return [ret];
}
async function _streamReadManifest(params) {
    let bucket = params.bucket;
    let key = params.url;
    if (params.url.startsWith('s3://')) {
        const parsed = parseS3Url(params.url);
        bucket = parsed.bucket;
        key = parsed.key;
    }
    if (!bucket || !key) {
        throw new Error(`invalid manfiest url: ${params.url}`);
    }
    return downloadAvro({
        credentials: params.credentials,
        region: params.region,
        bucket,
        key,
        avroSchema: params.schema,
    });
}
function _filterDeletes(record) {
    return (record.content === ListContent.DATA &&
        record.added_files_count === 0 &&
        record.existing_files_count === 0);
}
function _groupList(list, compare) {
    const ret = [];
    for (const item of list) {
        let added = false;
        for (const group of ret) {
            if (group[0] && compare(group[0], item)) {
                group.push(item);
                added = true;
                break;
            }
        }
        if (!added) {
            ret.push([item]);
        }
    }
    return ret;
}
function _combineWeightGroups(groups, targetCount, calculateWeight) {
    const weighted_groups = groups.map((group) => ({
        group,
        weight: calculateWeight(group),
    }));
    weighted_groups.sort(_sortGroup);
    while (weighted_groups.length > targetCount) {
        const remove_item = weighted_groups.shift();
        if (!remove_item) {
            break;
        }
        for (const item of remove_item.group) {
            weighted_groups[0]?.group.push(item);
        }
    }
    return weighted_groups.map((g) => g.group);
}
function _sortGroup(a, b) {
    return a.weight - b.weight;
}
function _randomBigInt64() {
    const bytes = node_crypto.randomBytes(8);
    let ret = bytes.readBigUInt64BE();
    ret &= BigInt('0x7FFFFFFFFFFFFFFF');
    if (ret === 0n) {
        ret = 1n;
    }
    return ret;
}
function _bigintMin(value0, ...values) {
    let ret = value0;
    for (const val of values) {
        if (val < ret) {
            ret = val;
        }
    }
    return ret;
}

var index = {
    IcebergHttpError,
    getMetadata,
    addSchema,
    addPartitionSpec,
    addManifest,
    addDataFiles,
    setCurrentCommit,
    removeSnapshots,
};

exports.IcebergHttpError = IcebergHttpError;
exports.addDataFiles = addDataFiles;
exports.addManifest = addManifest;
exports.addPartitionSpec = addPartitionSpec;
exports.addSchema = addSchema;
exports.default = index;
exports.getMetadata = getMetadata;
exports.manifestCompact = manifestCompact;
exports.removeSnapshots = removeSnapshots;
exports.setCurrentCommit = setCurrentCommit;
exports.submitSnapshot = submitSnapshot;
