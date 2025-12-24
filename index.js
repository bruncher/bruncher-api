import express from "express";
import { mountCrypto } from "./crypto.js";
import { mountGaming } from "./gaming.js";

const app = express();

mountCrypto(app);
mountGaming(app);

app.get("/", (req, res) => {
  res.json({
    status: "ok",
    message: "bruncher-api live",
    services: ["crypto", "gaming"]
  });
});

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    services: ["crypto"],
    time: new Date().toISOString()
  });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 bruncher-api running on port ${PORT}`);
});
