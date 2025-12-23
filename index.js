import express from "express";

import { mountCrypto } from "./crypto.js";

const app = express();

// Mount crypto routes
mountCrypto(app);

// Shared health
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "crypto",
    time: new Date().toISOString()
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 bruncher-api running on port ${PORT}`);
});
