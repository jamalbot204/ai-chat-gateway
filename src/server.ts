import express from "express";
import {
  HttpError,
  buildUpstreamRequest,
  fetchImages,
  normalizeChatResponse,
  readUpstreamError,
  toNodeReadable,
  validateProviderBaseUrl,
} from "./provider.js";

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

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

app.post("/chat", async (req, res) => {
  // ---- auth ----
  const appToken = process.env.APP_TOKEN ?? "";
  const presented = req.get("X-App-Token") ?? "";
  if (!appToken || !presented || !timingSafeEqual(presented, appToken)) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }

  // ---- request validation ----
  const body = req.body as Record<string, unknown> | undefined;
  const message = body?.message;
  const provider = body?.provider as Record<string, unknown> | undefined;

  if (
    !body ||
    typeof body !== "object" ||
    typeof message !== "string" ||
    !provider ||
    typeof provider !== "object" ||
    typeof provider.api_key !== "string" ||
    provider.api_key === "" ||
    typeof provider.model !== "string" ||
    provider.model === "" ||
    typeof provider.base_url !== "string"
  ) {
    res.status(400).json({ error: "bad_request" });
    return;
  }

  const imagesRaw = body.images;
  if (imagesRaw !== undefined && !Array.isArray(imagesRaw)) {
    res.status(400).json({ error: "bad_request" });
    return;
  }
  const images = (imagesRaw ?? []) as string[];

  const stream = body.stream === true;
  const maxTokens = typeof body.max_tokens === "number" && body.max_tokens > 0
    ? body.max_tokens
    : 4000;

  let baseUrl: string;
  try {
    baseUrl = validateProviderBaseUrl(provider.base_url);
  } catch (err) {
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: err.error, detail: err.detail });
      return;
    }
    res.status(400).json({ error: "bad_request" });
    return;
  }

  // ---- image fetching ----
  const storageWorkerUrl = process.env.STORAGE_WORKER_URL ?? "";
  const storageToken = process.env.STORAGE_TOKEN ?? "";

  let imageData: { contentType: string; dataUrl: string }[];
  try {
    imageData = await fetchImages(images, storageWorkerUrl, storageToken);
  } catch (err) {
    if (err instanceof HttpError) {
      res.status(err.status).json({ error: err.error, detail: err.detail });
      return;
    }
    res.status(502).json({ error: "provider_unavailable" });
    return;
  }

  const imageUrls = imageData.map((d) => d.dataUrl);

  const { url, init } = buildUpstreamRequest(
    {
      message,
      images,
      provider: {
        baseUrl,
        apiKey: provider.api_key,
        model: provider.model,
      },
      stream,
      maxTokens,
    },
    imageUrls,
  );

  // ---- upstream call ----
  let upstream: Response;
  try {
    upstream = await fetch(url, init);
  } catch (err) {
    const detail = err instanceof Error ? err.message : "upstream request failed";
    res.status(502).json({ error: "provider_unavailable", detail });
    return;
  }

  // ---- upstream error -> 502 with verbatim text ----
  if (!upstream.ok) {
    const text = await readUpstreamError(upstream);
    res.status(502).json({ error: "provider_unavailable", detail: text });
    return;
  }

  // ---- streaming passthrough ----
  if (stream) {
    const nodeStream = toNodeReadable(upstream.body);
    if (!nodeStream) {
      res.status(502).json({ error: "provider_unavailable", detail: "upstream returned no body" });
      return;
    }
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();
    nodeStream.on("error", () => {
      if (!res.writableEnded) res.end();
    });
    res.on("close", () => {
      nodeStream.destroy();
    });
    nodeStream.pipe(res);
    return;
  }

  // ---- non-streaming normalization ----
  let json: unknown;
  try {
    json = await upstream.json();
  } catch {
    const text = await readUpstreamError(upstream).catch(() => "");
    res.status(502).json({ error: "provider_unavailable", detail: text || "invalid upstream JSON" });
    return;
  }

  const normalized = normalizeChatResponse(json);

  // Guard: an upstream 200 with no answer is reported as an upstream failure
  // rather than returned as a silent empty reply.
  if (normalized.reply === "") {
    res.status(502).json({
      error: "provider_unavailable",
      detail: "upstream returned an empty choices[0].message.content",
    });
    return;
  }

  res.status(200).json(normalized);
});

// ---- JSON error handler: overrides Express's default HTML error pages ----
app.use(
  (
    err: unknown,
    _req: express.Request,
    res: express.Response,
    _next: express.NextFunction,
  ) => {
    if (res.headersSent) return;
    const status = typeof (err as { status?: number })?.status === "number"
      ? (err as { status: number }).status
      : 500;
    // Body-parser emits 413 (too large) and 400 (malformed JSON); both must be JSON.
    if (status >= 400 && status < 500) {
      res.status(status).json({ error: "bad_request" });
      return;
    }
    res.status(500).json({ error: "internal" });
  },
);

const port = Number(process.env.PORT) || 3000;
app.listen(port, "0.0.0.0", () => console.log(`listening on ${port}`));
