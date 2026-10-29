import express from "express";

const app = express();
app.use(express.json({ limit: "50mb" }));
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-App-Token");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") { res.sendStatus(204); return; }
  next();
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, ts: Date.now() });
});

const port = Number(process.env.PORT) || 3000;
app.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));