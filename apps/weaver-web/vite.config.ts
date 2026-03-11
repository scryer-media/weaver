import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes("node_modules")) {
            return undefined;
          }

          if (
            id.includes("/react/") ||
            id.includes("/react-dom/") ||
            id.includes("/react-router/")
          ) {
            return "react-vendor";
          }

          if (
            id.includes("/urql/") ||
            id.includes("/graphql/") ||
            id.includes("/graphql-ws/")
          ) {
            return "graphql-vendor";
          }

          if (
            id.includes("/radix-ui/") ||
            id.includes("/lucide-react/") ||
            id.includes("/next-themes/")
          ) {
            return "ui-vendor";
          }

          if (id.includes("/@fontsource/")) {
            return "font-vendor";
          }

          return undefined;
        },
      },
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    proxy: {
      "/graphql": {
        target: "http://localhost:6789",
        ws: true,
      },
    },
  },
});
