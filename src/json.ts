import * as LosslessJson from 'lossless-json';
export { stringify } from 'lossless-json';

export type JSONPrimitive =
  | string
  | number
  | boolean
  | null
  | bigint
  | undefined;
export type JSONValue = JSONPrimitive | JSONObject | JSONArray;
export interface JSONObject {
  [key: string]: JSONValue;
}
export type JSONArray = JSONValue[];

function customNumberParser(value: string) {
  if (LosslessJson.isInteger(value)) {
    if (LosslessJson.isSafeNumber(value)) {
      return parseInt(value, 10);
    }
    return BigInt(value);
  }
  return parseFloat(value);
}
export function parse(text: string): JSONValue {
  return LosslessJson.parse(text, null, customNumberParser) as JSONValue;
}
