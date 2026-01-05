import net from "net";

export async function sendEngineRequest(host, port, obj) {
  const payload = JSON.stringify(obj) + "\n";

  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let buf = "";

    socket.connect(port, host, () => socket.write(payload));

    socket.on("data", (chunk) => {
      buf += chunk.toString("utf8");
      const idx = buf.indexOf("\n");
      if (idx !== -1) {
        const line = buf.slice(0, idx);
        socket.destroy();
        try {
          resolve(JSON.parse(line));
        } catch (e) {
          reject(new Error("Invalid JSON from engine: " + line));
        }
      }
    });

    socket.on("error", (err) => reject(err));
    socket.on("close", () => {});
    socket.setTimeout(4000, () => reject(new Error("Engine timeout")));
  });
}
