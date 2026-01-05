import React, { useEffect, useMemo, useRef, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid
} from "recharts";

const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8080";

function fmtTime(t) {
  const d = new Date(t);
  return d.toLocaleTimeString();
}

function sumLag(groupStats) {
  let s = 0;
  const topics = groupStats?.topics || [];
  for (const t of topics) {
    for (const p of t.partitions_stats || []) s += (p.lag ?? 0);
  }
  return s;
}

export default function App() {
  const [status, setStatus] = useState("connecting");
  const [err, setErr] = useState("");
  const [group, setGroup] = useState("g1");
  const [snapshot, setSnapshot] = useState(null);

  const [series, setSeries] = useState([]); // time series for charts
  const wsRef = useRef(null);

  useEffect(() => {
    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => setStatus("live");
    ws.onclose = () => setStatus("disconnected");
    ws.onerror = () => setStatus("error");

    ws.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === "ERROR") {
          setErr(msg.message || "gateway error");
          return;
        }
        if (msg.type === "SNAPSHOT") {
          setErr("");
          setSnapshot(msg);

          setSeries((prev) => {
            const point = {
              t: msg.now,
              time: fmtTime(msg.now),
              throughput: Number(msg.derived?.throughput_per_sec || 0),
              totalEnd: Number(msg.derived?.totalEnd || 0),
              totalLag: sumLag(msg.groupStats)
            };
            const next = [...prev, point].slice(-120); // last 2 minutes
            return next;
          });
        }
      } catch {}
    };

    return () => ws.close();
  }, []);

  // send group updates
  useEffect(() => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== 1) return;
    ws.send(JSON.stringify({ type: "SET_GROUP", group }));
  }, [group, status]);

  const topics = snapshot?.topics || [];
  const groupStats = snapshot?.groupStats;
  const totalLag = useMemo(() => sumLag(groupStats), [groupStats]);

  // build lag table rows
  const lagRows = useMemo(() => {
    const rows = [];
    const topicsArr = groupStats?.topics || [];
    for (const t of topicsArr) {
      for (const p of t.partitions_stats || []) {
        rows.push({
          topic: t.topic,
          partition: p.partition,
          committed: p.committed_offset,
          end: p.end_offset,
          lag: p.lag
        });
      }
    }
    // biggest lag first
    rows.sort((a,b) => (b.lag||0) - (a.lag||0));
    return rows;
  }, [groupStats]);

  return (
    <div className="container">
      <div className="header">
        <div className="brand">
          <div className="logo" />
          <div>
            <div style={{ fontSize: 18, fontWeight: 800 }}>PulseStream</div>
            <div className="small">Real-time event engine dashboard</div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 10, alignItems: "center" }}>
          <span className={status === "live" ? "badge" : "badge red"}>
            {status.toUpperCase()}
          </span>
          <input
            value={group}
            onChange={(e) => setGroup(e.target.value)}
            placeholder="consumer group (e.g., g1)"
            style={{ width: 220 }}
          />
        </div>
      </div>

      {err ? (
        <div className="card" style={{ marginTop: 14, borderColor: "rgba(239,68,68,0.35)" }}>
          <b>Gateway error:</b> {err}
        </div>
      ) : null}

      <div className="grid grid3" style={{ marginTop: 14 }}>
        <div className="card">
          <div className="kpi">{(snapshot?.derived?.throughput_per_sec ?? 0).toFixed(2)}</div>
          <div className="kpiLabel">Throughput (events/sec)</div>
        </div>
        <div className="card">
          <div className="kpi">{snapshot?.derived?.totalEnd ?? 0}</div>
          <div className="kpiLabel">Total events (all topics)</div>
        </div>
        <div className="card">
          <div className="kpi">{totalLag}</div>
          <div className="kpiLabel">Total lag (group: {group})</div>
        </div>
      </div>

      <div className="grid grid2" style={{ marginTop: 14 }}>
        <div className="card">
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Throughput trend</div>
          <div style={{ height: 240 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
                <XAxis dataKey="time" hide />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="throughput" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="small">Derived from end_offset deltas</div>
        </div>

        <div className="card">
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Lag trend</div>
          <div style={{ height: 240 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={series}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.2} />
                <XAxis dataKey="time" hide />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="totalLag" dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
          <div className="small">Sum of lag across partitions</div>
        </div>
      </div>

      <div className="grid grid2" style={{ marginTop: 14 }}>
        <div className="card">
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Topics & end offsets</div>
          <table>
            <thead>
              <tr>
                <th>Topic</th>
                <th>Partition</th>
                <th>End offset</th>
              </tr>
            </thead>
            <tbody>
              {topics.flatMap((t) =>
                (t.partition_stats || []).map((p) => (
                  <tr key={`${t.topic}-${p.partition}`}>
                    <td>{t.topic}</td>
                    <td>{p.partition}</td>
                    <td>{p.end_offset}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        <div className="card">
          <div style={{ fontWeight: 700, marginBottom: 8 }}>Consumer lag by partition</div>
          <table>
            <thead>
              <tr>
                <th>Topic</th>
                <th>Partition</th>
                <th>Committed</th>
                <th>End</th>
                <th>Lag</th>
              </tr>
            </thead>
            <tbody>
              {lagRows.map((r) => (
                <tr key={`${r.topic}-${r.partition}`}>
                  <td>{r.topic}</td>
                  <td>{r.partition}</td>
                  <td>{r.committed}</td>
                  <td>{r.end}</td>
                  <td><b>{r.lag}</b></td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="small">Tip: produce messages then consume with FETCH_GROUP</div>
        </div>
      </div>
    </div>
  );
}
