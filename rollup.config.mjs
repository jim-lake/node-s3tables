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
];
