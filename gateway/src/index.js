import { WebSocketServer } from "ws";
import { sendEngineRequest } from "./engineClient.js";

const ENGINE_HOST = process.env.ENGINE_HOST || "127.0.0.1";
const ENGINE_PORT = Number(process.env.ENGINE_PORT || "9000");
const WS_PORT = Number(process.env.WS_PORT || "8080");

// UI config
const DEFAULT_GROUP = "g1";
const POLL_MS = 1000;

const wss = new WebSocketServer({ port: WS_PORT });
console.log(`Gateway WS listening on ws://localhost:${WS_PORT}`);
console.log(`Engine at ${ENGINE_HOST}:${ENGINE_PORT}`);

let lastSnapshot = null;

function safeSend(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

async function getSnapshot(group = DEFAULT_GROUP) {
  const topicsRes = await sendEngineRequest(ENGINE_HOST, ENGINE_PORT, { type: "TOPICS" });
  const groupRes = await sendEngineRequest(ENGINE_HOST, ENGINE_PORT, { type: "GROUP_STATS", group });

  const now = Date.now();

  // derive throughput: delta end_offset across all partitions / delta time
  let totalEnd = 0;
  for (const t of topicsRes.topics || []) {
    for (const p of t.partition_stats || []) totalEnd += (p.end_offset ?? 0);
  }

  let throughput = 0;
  if (lastSnapshot) {
    const dt = (now - lastSnapshot.now) / 1000;
    const d = totalEnd - lastSnapshot.totalEnd;
    throughput = dt > 0 ? d / dt : 0;
  }

  const snapshot = {
    type: "SNAPSHOT",
    now,
    group,
    topics: topicsRes.topics || [],
    groupStats: groupRes.stats || null,
    derived: {
      totalEnd,
      throughput_per_sec: throughput
    }
  };

  lastSnapshot = { now, totalEnd };
  return snapshot;
}

wss.on("connection", (ws) => {
  // allow the UI to pick group by sending: {"type":"SET_GROUP","group":"g1"}
  let currentGroup = DEFAULT_GROUP;

  safeSend(ws, { type: "HELLO", groups: [DEFAULT_GROUP], defaultGroup: DEFAULT_GROUP });

  ws.on("message", (msg) => {
    try {
      const obj = JSON.parse(msg.toString("utf8"));
      if (obj.type === "SET_GROUP" && typeof obj.group === "string") {
        currentGroup = obj.group;
      }
    } catch {}
  });

  const timer = setInterval(async () => {
    try {
      const snap = await getSnapshot(currentGroup);
      safeSend(ws, snap);
    } catch (e) {
      safeSend(ws, { type: "ERROR", message: String(e.message || e) });
    }
  }, POLL_MS);

  ws.on("close", () => clearInterval(timer));
});
