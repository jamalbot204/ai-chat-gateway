import express from "express";

const app = express();
app.use(express.json({ limit: "50mb" }));

app.get("/health", (_req, res) => {
  res.json({ ok: true, ts: Date.now() });
});

const port = Number(process.env.PORT) || 3000;
app.listen(port, () => console.log(`listening on ${port}`));