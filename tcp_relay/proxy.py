import asyncio
import datetime
import json
import time

CONFIG_PATH = "/data/options.json"
LOG_PATH = "/data/traffic.jsonl"
READ_TIMEOUT = 620  # a little above the backend's 600s gunicorn timeout

with open(CONFIG_PATH) as f:
    _cfg = json.load(f)

LISTEN_PORT = _cfg["listen_port"]
TARGET_HOST = _cfg["target_host"]
TARGET_PORT = _cfg["target_port"]


def log_event(event):
    event["ts"] = datetime.datetime.utcnow().isoformat() + "Z"
    line = json.dumps(event, default=str)
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"WARN: failed to write traffic log: {e}", flush=True)


def get_header_ci(headers, name):
    name_lower = name.lower()
    for k, v in headers.items():
        if k.lower() == name_lower:
            return v
    return None


async def read_headers(reader):
    data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=READ_TIMEOUT)
    head = data[:-4]
    lines = head.split(b"\r\n")
    start_line = lines[0].decode("latin1", errors="replace")
    headers = {}
    for line in lines[1:]:
        if not line:
            continue
        name, _, value = line.partition(b":")
        headers[name.decode("latin1").strip()] = value.decode("latin1", errors="replace").strip()
    return start_line, headers


async def forward_body(reader, writer, headers):
    te = get_header_ci(headers, "Transfer-Encoding") or ""
    if "chunked" in te.lower():
        total = 0
        while True:
            size_line = await asyncio.wait_for(reader.readuntil(b"\r\n"), timeout=READ_TIMEOUT)
            writer.write(size_line)
            chunk_size = int(size_line.split(b";")[0].strip(), 16)
            if chunk_size == 0:
                trailer = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=READ_TIMEOUT)
                writer.write(trailer)
                break
            chunk = await asyncio.wait_for(reader.readexactly(chunk_size + 2), timeout=READ_TIMEOUT)
            writer.write(chunk)
            total += chunk_size
        await writer.drain()
        return total

    cl = get_header_ci(headers, "Content-Length")
    if cl is not None:
        n = int(cl)
        remaining = n
        while remaining > 0:
            chunk = await asyncio.wait_for(reader.read(min(65536, remaining)), timeout=READ_TIMEOUT)
            if not chunk:
                break
            writer.write(chunk)
            remaining -= len(chunk)
        await writer.drain()
        return n - remaining

    return 0


async def handle_client(client_reader, client_writer):
    peer = client_writer.get_extra_info("peername")
    client_ip, client_port = (peer[0], peer[1]) if peer else ("?", 0)

    try:
        backend_reader, backend_writer = await asyncio.open_connection(TARGET_HOST, TARGET_PORT)
    except Exception as e:
        log_event({
            "event": "backend_connect_error",
            "client_ip": client_ip,
            "client_port": client_port,
            "target_host": TARGET_HOST,
            "target_port": TARGET_PORT,
            "error": str(e),
        })
        client_writer.close()
        return

    try:
        while True:
            start = time.monotonic()
            try:
                request_line, req_headers = await read_headers(client_reader)
            except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError):
                break  # client closed the connection / no more pipelined requests

            parts = request_line.split(" ")
            method = parts[0] if len(parts) > 0 else ""
            path = parts[1] if len(parts) > 1 else ""
            http_version = parts[2] if len(parts) > 2 else ""

            backend_writer.write((request_line + "\r\n").encode("latin1"))
            for k, v in req_headers.items():
                backend_writer.write(f"{k}: {v}\r\n".encode("latin1"))
            backend_writer.write(b"\r\n")
            await backend_writer.drain()

            req_body_bytes = await forward_body(client_reader, backend_writer, req_headers)

            try:
                status_line, resp_headers = await read_headers(backend_reader)
            except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionResetError) as e:
                log_event({
                    "event": "backend_response_error",
                    "client_ip": client_ip,
                    "client_port": client_port,
                    "method": method,
                    "path": path,
                    "error": str(e),
                })
                break

            status_parts = status_line.split(" ", 2)
            status_code = status_parts[1] if len(status_parts) > 1 else "?"

            client_writer.write((status_line + "\r\n").encode("latin1"))
            for k, v in resp_headers.items():
                client_writer.write(f"{k}: {v}\r\n".encode("latin1"))
            client_writer.write(b"\r\n")
            await client_writer.drain()

            resp_body_bytes = await forward_body(backend_reader, client_writer, resp_headers)

            duration_ms = int((time.monotonic() - start) * 1000)

            log_event({
                "event": "request",
                "client_ip": client_ip,
                "client_port": client_port,
                "method": method,
                "path": path,
                "http_version": http_version,
                "request_headers": req_headers,
                "request_body_bytes": req_body_bytes,
                "status": status_code,
                "response_headers": resp_headers,
                "response_body_bytes": resp_body_bytes,
                "duration_ms": duration_ms,
            })

            conn_header = (get_header_ci(req_headers, "Connection") or "").lower()
            resp_conn_header = (get_header_ci(resp_headers, "Connection") or "").lower()
            if conn_header == "close" or resp_conn_header == "close" or http_version.strip() == "HTTP/1.0":
                break
    except Exception as e:
        log_event({
            "event": "proxy_error",
            "client_ip": client_ip,
            "client_port": client_port,
            "error": str(e),
        })
    finally:
        client_writer.close()
        backend_writer.close()


async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", LISTEN_PORT)
    log_event({
        "event": "startup",
        "listen_port": LISTEN_PORT,
        "target_host": TARGET_HOST,
        "target_port": TARGET_PORT,
    })
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
