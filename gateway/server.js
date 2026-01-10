/**
 * PulseStream Gateway (ESM)
 *
 * - WebSocket server for UI: ws://localhost:8080
 * - Optional connection to Engine: tcp://127.0.0.1:9000
 * - Producer TCP ingress for events: tcp://127.0.0.1:9001  (NDJSON)
 *
 * The UI updates ONLY on messages with { type: "SNAPSHOT" }.
 */

import net from "net";
import { WebSocketServer } from "ws";

const ENGINE_HOST = process.env.ENGINE_HOST || "127.0.0.1";
const ENGINE_PORT = Number(process.env.ENGINE_PORT || 9000);

const WS_PORT = Number(process.env.WS_PORT || 8080);
const PRODUCER_PORT = Number(process.env.PRODUCER_PORT || 9001);

// ---- State used to build SNAPSHOT ----
let group = "g1";
let totalEnd = 0;            // total events ingested (all topics)
let eventsThisSecond = 0;    // for throughput
const topicEnds = new Map(); // topic -> end_offset (simple counter)
const topicPartitions = new Map(); // topic -> Set(partitions)

// ---- WebSocket (UI) ----
const wss = new WebSocketServer({ port: WS_PORT });
console.log(`[gateway] WS listening on ws://localhost:${WS_PORT}`);

function broadcast(obj) {
  const data = JSON.stringify(obj);
  for (const client of wss.clients) {
    if (client.readyState === 1) client.send(data); // 1 = OPEN
  }
}

wss.on("connection", (ws) => {
  ws.send(JSON.stringify({ type: "hello", message: "connected" }));

  ws.on("message", (buf) => {
    try {
      const msg = JSON.parse(buf.toString());
      if (msg.type === "SET_GROUP" && msg.group) group = msg.group;
    } catch {}
  });
});

// ---- Engine connection (optional forward, and/or to read engine output if it streams) ----
let engineSock = null;
try {
  engineSock = net.createConnection({ host: ENGINE_HOST, port: ENGINE_PORT }, () => {
    console.log(`[gateway] connected to engine ${ENGINE_HOST}:${ENGINE_PORT}`);
  });

  engineSock.setEncoding("utf8");

  // If your engine actually streams NDJSON out, we can count it too.
  let buf = "";
  engineSock.on("data", (chunk) => {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf("\n")) !== -1) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      // Treat each line as an event (even if we don't parse it)
      ingestEventLine(line, /*fromEngine*/ true);
    }
  });

  engineSock.on("error", (e) => {
    console.error("[gateway] engine error:", e.message);
    broadcast({ type: "ERROR", message: `engine error: ${e.message}` });
  });

  engineSock.on("close", () => {
    console.error("[gateway] engine connection closed");
    broadcast({ type: "ERROR", message: "engine connection closed" });
  });
} catch (e) {
  console.error("[gateway] could not connect to engine:", e?.message || e);
}

// ---- Producer TCP server (NDJSON in) ----
// Send NDJSON here so the gateway definitely sees & counts events.
const producerServer = net.createServer((client) => {
  client.setEncoding("utf8");
  let buf = "";

  client.on("data", (chunk) => {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf("\n")) !== -1) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;

      ingestEventLine(line, /*fromEngine*/ false);

      // Optional: forward into engine if it's expecting input NDJSON
      try {
        if (engineSock && !engineSock.destroyed) {
          engineSock.write(line + "\n");
        }
      } catch {}
    }
  });
});

producerServer.listen(PRODUCER_PORT, "127.0.0.1", () => {
  console.log(`[gateway] Producer listening on tcp://127.0.0.1:${PRODUCER_PORT}`);
});

// ---- Event ingestion / counters ----
function ingestEventLine(line, fromEngine) {
  // Count event
  totalEnd += 1;
  eventsThisSecond += 1;

  // Try to parse topic/partition for nicer tables.
  // If parse fails, we fall back to "demo"/partition 0.
  let topic = "demo";
  let partition = 0;

  try {
    const obj = JSON.parse(line);
    if (typeof obj.topic === "string" && obj.topic.length) topic = obj.topic;
    if (obj.partition !== undefined && obj.partition !== null) {
      const p = Number(obj.partition);
      if (!Number.isNaN(p)) partition = p;
    }
  } catch {
    // ignore parse errors; still count
  }

  topicEnds.set(topic, (topicEnds.get(topic) || 0) + 1);

  if (!topicPartitions.has(topic)) topicPartitions.set(topic, new Set());
  topicPartitions.get(topic).add(partition);

  // (Optional) You can log first few events to confirm flow
  // if (!fromEngine && totalEnd < 5) console.log("[gateway] event:", line);
}

// ---- Emit SNAPSHOT every 1s (this drives your UI) ----
setInterval(() => {
  const now = Date.now();
  const throughput = eventsThisSecond;
  eventsThisSecond = 0;

  // Build topics[] for "Topics & end offsets" table
  const topics = [];
  for (const [topic, end] of topicEnds.entries()) {
    const parts = Array.from(topicPartitions.get(topic) || new Set([0])).sort((a, b) => a - b);
    topics.push({
      topic,
      partition_stats: parts.map((p) => ({
        partition: p,
        end_offset: end,
      })),
    });
  }
  // Stable order
  topics.sort((a, b) => a.topic.localeCompare(b.topic));

  // Build groupStats for lag table (we’ll assume committed catches up = 0 lag)
  const groupStats = {
    group,
    topics: topics.map((t) => ({
      topic: t.topic,
      partitions_stats: (t.partition_stats || []).map((p) => ({
        partition: p.partition,
        committed_offset: p.end_offset, // pretend consumer is caught up
        end_offset: p.end_offset,
        lag: 0,
      })),
    })),
  };

  const snapshot = {
    type: "SNAPSHOT",
    now,
    derived: {
      throughput_per_sec: throughput,
      totalEnd,
    },
    topics,
    groupStats,
  };

  broadcast(snapshot);
}, 1000);
