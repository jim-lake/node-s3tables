import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import typescript from '@rollup/plugin-typescript';
import dts from 'rollup-plugin-dts';

export default [
  {
    input: 'src/index.ts',
    output: { file: 'dist/index.js', format: 'cjs', exports: 'named' },
    plugins: [typescript(), resolve(), commonjs()],
    external: [/node_modules/],
    treeshake: 'smallest',
  },
  {
    input: 'src/index.ts',
    output: [{ file: 'dist/index.d.ts', format: 'es' }],
    plugins: [dts()],
  },
  {
    input: 'src/bin.ts',
    output: {
      file: 'dist/bin.js',
      format: 'cjs',
      banner: '#!/usr/bin/env node',
    },
    plugins: [typescript(), resolve(), commonjs()],
    external: [/node_modules/, 'node-s3tables'],
    treeshake: 'smallest',
  },
];
