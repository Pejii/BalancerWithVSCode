addEventListener("fetch", event => {
    event.respondWith(handleRequest(event.request));
});

const WORKER_TOKEN = "PUT_YOUR_WORKER_TOKEN_HERE";
const pendingQueues = new Map();
const activeSessions = new Map();
const wsToSession = new Map();
let nextSessionId = 1;

function buildDestKey(channel, dst_addr, dst_port) {
    return `${channel}:${dst_addr}:${dst_port}`;
}

function validateToken(request) {
    if (!WORKER_TOKEN) {
        return true;
    }
    const token = request.headers.get("X-Worker-Token");
    return Boolean(token && token === WORKER_TOKEN);
}

async function handleRequest(request) {
    const url = new URL(request.url);
    if (url.pathname !== "/ws") {
        return new Response("Not found", { status: 404 });
    }

    if (!request.headers.get("Upgrade")?.toLowerCase().includes("websocket")) {
        return new Response("Expected websocket", { status: 400 });
    }

    if (!validateToken(request)) {
        return new Response("Unauthorized", { status: 401 });
    }

    const role = url.searchParams.get("role");
    const channel = url.searchParams.get("channel") || "default";
    if (!role || (role !== "front" && role !== "back")) {
        return new Response("Missing role", { status: 400 });
    }

    const [client, server] = Object.values(new WebSocketPair());
    server.accept();

    server.addEventListener("message", event => {
        try {
            if (typeof event.data === "string") {
                handleControlMessage(server, channel, event.data);
                return;
            }

            const session = wsToSession.get(server);
            if (!session) {
                return;
            }

            const pair = activeSessions.get(session.sessionKey);
            if (!pair) {
                return;
            }

            const target = session.role === "front" ? pair.back.ws : pair.front.ws;
            if (target && target.readyState === 1) {
                target.send(event.data);
            }
        } catch (err) {
            server.send(JSON.stringify({ type: "error", message: err.message }));
        }
    });

    server.addEventListener("close", () => {
        cleanupConnection(server);
    });

    return new Response(null, { status: 101, webSocket: client });
}

function handleControlMessage(server, channel, rawData) {
    let obj;
    try {
        obj = JSON.parse(rawData);
    } catch (err) {
        server.send(JSON.stringify({ type: "error", message: "Invalid JSON" }));
        return;
    }

    if (obj.type === "open") {
        addPendingConnection(server, channel, obj);
        return;
    }

    if (obj.type === "close") {
        if (obj.session_id !== undefined) {
            closeActiveSession(`${channel}:${obj.session_id}`);
            return;
        }
        removePendingConnection(server);
        return;
    }
}

function addPendingConnection(server, channel, obj) {
    if (!obj.role || !obj.client_id || !obj.dst_addr || !obj.dst_port) {
        server.send(JSON.stringify({ type: "error", message: "Invalid open payload" }));
        return;
    }

    const destKey = buildDestKey(channel, obj.dst_addr, obj.dst_port);
    let queue = pendingQueues.get(destKey);
    if (!queue) {
        queue = { front: [], back: [] };
        pendingQueues.set(destKey, queue);
    }

    queue[obj.role].push({ clientId: obj.client_id, ws: server });
    wsToSession.set(server, { destKey, role: obj.role, clientId: obj.client_id });

    if (queue.front.length > 0 && queue.back.length > 0) {
        const frontEntry = queue.front.shift();
        const backEntry = queue.back.shift();
        if (queue.front.length === 0 && queue.back.length === 0) {
            pendingQueues.delete(destKey);
        }

        const sessionId = nextSessionId++;
        const sessionKey = `${channel}:${sessionId}`;
        activeSessions.set(sessionKey, {
            sessionId,
            front: frontEntry,
            back: backEntry,
        });

        wsToSession.set(frontEntry.ws, { sessionKey, role: "front" });
        wsToSession.set(backEntry.ws, { sessionKey, role: "back" });

        frontEntry.ws.send(JSON.stringify({ type: "assigned", client_id: frontEntry.clientId, session_id: sessionId }));
        backEntry.ws.send(JSON.stringify({ type: "assigned", client_id: backEntry.clientId, session_id: sessionId }));
    }
}

function removePendingConnection(server) {
    const pending = wsToSession.get(server);
    if (!pending || !pending.destKey) {
        return;
    }

    const queue = pendingQueues.get(pending.destKey);
    if (!queue) {
        wsToSession.delete(server);
        return;
    }

    const roleQueue = queue[pending.role];
    const index = roleQueue.findIndex(entry => entry.ws === server);
    if (index !== -1) {
        roleQueue.splice(index, 1);
    }

    if (queue.front.length === 0 && queue.back.length === 0) {
        pendingQueues.delete(pending.destKey);
    }
    wsToSession.delete(server);
}

function closeActiveSession(sessionKey) {
    const session = activeSessions.get(sessionKey);
    if (!session) {
        return;
    }

    if (session.front.ws && session.front.ws.readyState === 1) {
        session.front.ws.send(JSON.stringify({ type: "close", session_id: session.sessionId }));
    }
    if (session.back.ws && session.back.ws.readyState === 1) {
        session.back.ws.send(JSON.stringify({ type: "close", session_id: session.sessionId }));
    }

    wsToSession.delete(session.front.ws);
    wsToSession.delete(session.back.ws);
    activeSessions.delete(sessionKey);
}

function cleanupConnection(server) {
    const session = wsToSession.get(server);
    if (session && session.sessionKey) {
        closeActiveSession(session.sessionKey);
    } else {
        removePendingConnection(server);
    }
    wsToSession.delete(server);
}
