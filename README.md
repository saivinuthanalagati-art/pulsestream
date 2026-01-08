# PulseStream : Real-Time Signal Processing Engine
PulseStream is a high-performance, real-time stream processing system built to ingest, process, and visualize live signals with low latency and production-style observability.
It’s designed like an internal engineering tool: a fast C++ core for processing, a Node.js service layer, and a React dashboard for real-time monitoring.

🚀 What It Does
Ingests a continuous stream of signal/event data
Processes data in real time (aggregation, filtering, transformation)
Exposes live stats/metrics for monitoring
Visualizes throughput + system health in a web dashboard

✨ Key Features
- High-performance core (C++ engine) optimized for low-latency processing
- Streaming pipeline architecture (producer → processor → output)
- Live dashboard (React) for real-time monitoring & debugging
- Metrics + observability (throughput, processing rate, queue depth, etc.)
- Modular design : easy to add new processors/operators
- Concurrency-focused : designed around safe, scalable throughput

🛠️ Tech Stack
###Core Engine
C++ (low-level processing, performance-focused design)
###Services
Node.js (API + stream interface)
###Frontend
React (real-time dashboard)
###Tooling
CMake (build)
(Optional) Docker for consistent local setup

📊 Metrics Shown in Dashboard
Events/sec throughput
Processing latency (avg / p95 if implemented)
Queue depth / backlog
Error rates / dropped events

## Build & Run

### Engine (C++)
```bash
cd engine
cmake -S . -B build
cmake --build build -j
./build/engine
```
###Services (Node.js)
```bash
cd services
npm install
npm run dev
```
###Dashboard (React)
```bash
cd dashboard
npm install
npm run dev
```
👤 Author
Vinutha
Computer Science | Full-Stack Developer
