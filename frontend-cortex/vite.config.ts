import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/cortex/",
  plugins: [react()],
  server: {
    port: 5177,
    proxy: {
      "/api/v2": {
        target: "http://127.0.0.1:8001",
        changeOrigin: true,
      },
    },
  },
});
