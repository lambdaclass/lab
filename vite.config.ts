import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';
import { generateHeadPlugin } from './vite-plugins/generate-head-plugin';

const BACKENDS: Record<string, string> = {
  local: 'http://localhost:8080',
  production: 'https://lab.ethpandaops.io:443',
};

const backendKey = process.env.BACKEND ?? 'local';
const backendTarget = BACKENDS[backendKey] ?? backendKey;

// https://vite.dev/config/
export default defineConfig({
  // LAB_BASE_PATH serves the lab under a sub-path, e.g. LAB_BASE_PATH=/lab/ pnpm build
  base: process.env.LAB_BASE_PATH ?? '/',
  define: {
    'import.meta.env.VITE_BASE_TITLE': JSON.stringify('The Lab by LambdaClass'),
    'import.meta.env.VITE_BASE_URL': JSON.stringify(process.env.LAB_PUBLIC_URL ?? 'https://grafana.ethrex.xyz/lab'),
  },
  plugins: [
    tanstackRouter({
      routesDirectory: './src/routes',
      generatedRouteTree: './src/routeTree.gen.ts',
      autoCodeSplitting: true,
      quoteStyle: 'single',
    }),
    tailwindcss(),
    react(),
    visualizer({
      open: false,
      gzipSize: true,
      brotliSize: true,
      filename: 'build/stats.html',
    }),
    generateHeadPlugin(),
  ],
  server: {
    proxy: {
      '/api': {
        target: backendTarget,
        changeOrigin: true,
        secure: backendTarget.startsWith('https'),
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
    conditions: ['import', 'module', 'browser', 'default'],
  },
  build: {
    outDir: 'dist',
    commonjsOptions: {
      transformMixedEsModules: true, // Handle packages with mixed ESM/CommonJS
    },
    rollupOptions: {
      output: {
        // Generate fresh random hashes on every build to bust cache completely
        chunkFileNames: () => {
          const randomHash = Math.random().toString(36).substring(2, 10);
          return `assets/[name]-${randomHash}.js`;
        },
        entryFileNames: () => {
          const randomHash = Math.random().toString(36).substring(2, 10);
          return `assets/[name]-${randomHash}.js`;
        },
        assetFileNames: assetInfo => {
          // Keep fonts stable for caching (they never change)
          if (assetInfo.name?.match(/\.(woff2?|ttf|eot|otf)$/)) {
            return 'assets/[name]-[hash].[ext]';
          }
          // Random hash for everything else
          const randomHash = Math.random().toString(36).substring(2, 10);
          return `assets/[name]-${randomHash}.[ext]`;
        },
      },
    },
  },
});
