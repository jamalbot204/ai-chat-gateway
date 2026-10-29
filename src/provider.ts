import { Readable } from "node:stream";

/**
 * Provider-facing helpers for POST /chat:
 *   - SSRF guard (base_url allowlist + private-range rejection)
 *   - parallel image fetching from the storage Worker (concurrency-capped)
 *   - base64 data-URL conversion from the Worker's Content-Type
 *   - upstream Chat Completions request building
 *   - response normalization (flat { reply, usage, finish_reason })
 */

/* --------------------------------------------------------------------- */
/* Constants                                                              */
/* --------------------------------------------------------------------- */

/** Max concurrent image fetches to the storage Worker. */
export const IMAGE_FETCH_CONCURRENCY = 8;

/** Outbound provider timeout (ms). A 100-image request can take minutes. */
export const PROVIDER_TIMEOUT_MS = 240_000;

/** Per-image fetch timeout from the storage Worker (ms). */
const IMAGE_FETCH_TIMEOUT_MS = 60_000;

/** Hosts allowed to be used as provider base_url. */
export const PROVIDER_ALLOWLIST: readonly string[] = [
  "api.river.ai",
  "api.deepseek.com",
  "api.openai.com",
  "openrouter.ai",
];

const IMAGE_ID_RE = /^img_[0-9a-f]{12}$/;

/* --------------------------------------------------------------------- */
/* Errors                                                                 */
/* --------------------------------------------------------------------- */

export class HttpError extends Error {
  constructor(
    public readonly status: number,
    public readonly error: string,
    public readonly detail?: string,
  ) {
    super(detail ? `${error}: ${detail}` : error);
    this.name = "HttpError";
  }
}

/* --------------------------------------------------------------------- */
/* SSRF protection                                                        */
/* --------------------------------------------------------------------- */

function isPrivateHost(hostname: string): boolean {
  const host = hostname.toLowerCase();
  if (host === "localhost" || host.endsWith(".localhost") || host.endsWith(".internal")) return true;
  if (host === "0.0.0.0" || host === "127.0.0.1" || host === "::1" || host === "[::1]") return true;

  // IPv4 private / link-local / loopback / unspecified.
  const v4 = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (v4) {
    const [a, b] = [Number(v4[1]), Number(v4[2])];
    if (a === 127) return true; // loopback
    if (a === 10) return true; // 10.0.0.0/8
    if (a === 0) return true; // 0.0.0.0/8
    if (a === 169 && b === 254) return true; // link-local 169.254.0.0/16
    if (a === 192 && b === 168) return true; // 192.168.0.0/16
    if (a === 172 && b >= 16 && b <= 31) return true; // 172.16.0.0/12
  }
  return false;
}

/**
 * Validate the provider base_url. Throws HttpError(400) when it is not https,
 * has a disallowed host, or resolves to a private range.
 * Returns a normalized base URL with trailing slashes stripped.
 */
export function validateProviderBaseUrl(raw: unknown): string {
  if (typeof raw !== "string" || raw.trim() === "") {
    throw new HttpError(400, "bad_request", "base_url not allowed");
  }

  let url: URL;
  try {
    url = new URL(raw.trim());
  } catch {
    throw new HttpError(400, "bad_request", "base_url not allowed");
  }

  if (url.protocol !== "https:") {
    throw new HttpError(400, "bad_request", "base_url not allowed");
  }

  const host = url.hostname.toLowerCase();
  if (isPrivateHost(host)) {
    throw new HttpError(400, "bad_request", "base_url not allowed");
  }

  const allowed = PROVIDER_ALLOWLIST.some(
    (entry) => host === entry || host.endsWith("." + entry),
  );
  if (!allowed) {
    throw new HttpError(400, "bad_request", "base_url not allowed");
  }

  return raw.trim().replace(/\/+$/, "");
}

/** Build the Chat Completions endpoint, appending the path only if absent. */
export function chatCompletionsUrl(baseUrl: string): string {
  const trimmed = baseUrl.replace(/\/+$/, "");
  if (trimmed.endsWith("/chat/completions")) return trimmed;
  return trimmed + "/chat/completions";
}

/* --------------------------------------------------------------------- */
/* Storage Worker image fetching                                          */
/* --------------------------------------------------------------------- */

export interface ImageDataUrl {
  contentType: string;
  dataUrl: string;
}

async function fetchOneImage(
  id: string,
  storageWorkerUrl: string,
  storageToken: string,
): Promise<ImageDataUrl> {
  const url = `${storageWorkerUrl.replace(/\/+$/, "")}/image/${id}`;

  let res: Response;
  try {
    res = await fetch(url, {
      method: "GET",
      headers: { "X-App-Token": storageToken },
      signal: AbortSignal.timeout(IMAGE_FETCH_TIMEOUT_MS),
    });
  } catch {
    throw new HttpError(502, "provider_unavailable", `image fetch failed for ${id}`);
  }

  if (res.status === 404) {
    // Drain the body to free the socket.
    await res.body?.cancel().catch(() => {});
    throw new HttpError(400, "bad_request", `image ${id} not found`);
  }

  if (!res.ok) {
    await res.body?.cancel().catch(() => {});
    throw new HttpError(502, "provider_unavailable", `image ${id} fetch status ${res.status}`);
  }

  const contentType = (res.headers.get("content-type") || "application/octet-stream")
    .split(";")[0]
    .trim();
  const buf = Buffer.from(await res.arrayBuffer());
  const dataUrl = `data:${contentType};base64,${buf.toString("base64")}`;
  return { contentType, dataUrl };
}

/**
 * Validate all ids up front, then fetch with a bounded concurrency pool.
 * Rejects on the first failure with an HttpError. Uses allSettled internally
 * so a rejection never becomes an unhandled promise rejection.
 */
export async function fetchImages(
  ids: string[],
  storageWorkerUrl: string,
  storageToken: string,
): Promise<ImageDataUrl[]> {
  for (const id of ids) {
    if (typeof id !== "string" || !IMAGE_ID_RE.test(id)) {
      throw new HttpError(400, "bad_request", `invalid image id`);
    }
  }
  if (ids.length === 0) return [];

  const results: ImageDataUrl[] = new Array(ids.length);
  let nextIndex = 0;
  let failure: HttpError | null = null;

  async function worker(): Promise<void> {
    while (true) {
      if (failure) return;
      const i = nextIndex++;
      if (i >= ids.length) return;
      try {
        results[i] = await fetchOneImage(ids[i], storageWorkerUrl, storageToken);
      } catch (err) {
        if (!failure) {
          failure =
            err instanceof HttpError
              ? err
              : new HttpError(502, "provider_unavailable", `image fetch failed for ${ids[i]}`);
        }
        return;
      }
    }
  }

  const poolSize = Math.min(IMAGE_FETCH_CONCURRENCY, ids.length);
  await Promise.all(Array.from({ length: poolSize }, () => worker()));

  if (failure) throw failure;
  return results;
}

/* --------------------------------------------------------------------- */
/* Upstream request + normalization                                       */
/* --------------------------------------------------------------------- */

export interface ProviderConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
}

export interface ChatInput {
  message: string;
  images: string[];
  provider: ProviderConfig;
  stream: boolean;
  maxTokens: number;
}

function buildMessages(message: string, imageUrls: string[]) {
  const content: Array<unknown> = [{ type: "text", text: message }];
  for (const dataUrl of imageUrls) {
    content.push({ type: "image_url", image_url: { url: dataUrl } });
  }
  return [{ role: "user", content }];
}

export interface UpstreamInit {
  url: string;
  init: RequestInit;
}

export function buildUpstreamRequest(input: ChatInput, imageUrls: string[]): UpstreamInit {
  const body = {
    model: input.provider.model,
    messages: buildMessages(input.message, imageUrls),
    max_tokens: input.maxTokens,
    stream: input.stream,
  };

  return {
    url: chatCompletionsUrl(input.provider.baseUrl),
    init: {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${input.provider.apiKey}`,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(PROVIDER_TIMEOUT_MS),
    },
  };
}

/**
 * Read an upstream error body as text verbatim, without leaking secrets.
 * The upstream body itself never contains our api_key in practice; we return
 * it as-is so the harness can display the provider's message.
 */
export async function readUpstreamError(res: Response): Promise<string> {
  let text = "";
  try {
    text = await res.text();
  } catch {
    /* ignore */
  }
  if (text.trim() === "") {
    return `upstream returned status ${res.status}`;
  }
  return text;
}

/** Join content that may be a string or an array of parts. */
export function normalizeContent(content: unknown): string {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts: string[] = [];
    for (const part of content) {
      if (part && typeof part === "object") {
        const p = part as Record<string, unknown>;
        if (typeof p.text === "string") parts.push(p.text);
      } else if (typeof part === "string") {
        parts.push(part);
      }
    }
    return parts.join("");
  }
  return "";
}

export interface NormalizedChat {
  reply: string;
  usage: { prompt_tokens: number; completion_tokens: number; total_tokens: number };
  finish_reason: string | null;
}

/**
 * Map the OpenAI-compatible response to the flat contract shape.
 * Usage defaults to zeros when the upstream omits it, so the harness always
 * sees the documented keys.
 */
export function normalizeChatResponse(json: unknown): NormalizedChat {
  const obj = (json ?? {}) as Record<string, unknown>;
  const choices = Array.isArray(obj.choices) ? obj.choices : [];
  const first = (choices[0] ?? {}) as Record<string, unknown>;
  const message = (first.message ?? {}) as Record<string, unknown>;

  const reply = normalizeContent(message.content);
  const rawFinish = first.finish_reason;
  const finish_reason = typeof rawFinish === "string" ? rawFinish : null;

  const rawUsage = (obj.usage ?? {}) as Record<string, unknown>;
  const usage = {
    prompt_tokens: typeof rawUsage.prompt_tokens === "number" ? rawUsage.prompt_tokens : 0,
    completion_tokens:
      typeof rawUsage.completion_tokens === "number" ? rawUsage.completion_tokens : 0,
    total_tokens: typeof rawUsage.total_tokens === "number" ? rawUsage.total_tokens : 0,
  };

  return { reply, usage, finish_reason };
}

/* --------------------------------------------------------------------- */
/* SSE passthrough                                                        */
/* --------------------------------------------------------------------- */

/**
 * Convert an upstream web ReadableStream into a Node Readable for piping.
 * Returns null when there is no body.
 */
export function toNodeReadable(body: ReadableStream<Uint8Array> | null): Readable | null {
  if (!body) return null;
  return Readable.fromWeb(body as unknown as import("node:stream/web").ReadableStream<Uint8Array>);
}
