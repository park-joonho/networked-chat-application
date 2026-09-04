"""
CHATPROTO/1.0 client - Stage 2 Prototype
CSC3002F Networks Assignment 2026
Group 25: Joon-ho Park, Dennis Zhu, Jordan Rix

AI Usage: client skeleton generated and code blocks explained with chat assistance.
All protocol logic reviewed and understood by the group.

This module implements a chat client for the custom CHATPROTO/1.0 protocol.
It uses TCP for reliable signalling (registration, messaging, file transfer negotiation)
and UDP for efficient group message distribution (multicast-like behaviour).
Peer-to-peer (P2P) file transfers are performed over direct TCP connections,
bypassing the server to avoid bandwidth bottlenecks.
"""

import socket
import threading
import time
import os
import random
from typing import Dict, Optional, Tuple

CRLF = "\r\n"

def now_utc() -> str:
    """
    Returns the current UTC time formatted according to the protocol's
    timestamp requirement: YYYY-MM-DDTHH:MM:SSZ (ISO 8601 with Zulu time).
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def build_request(command: str, path: str, headers: Dict[str, str],
                  body = "") -> bytes:
    """
    Constructs a complete CHATPROTO/1.0 request message as a byte string.

    :param command: The request method (e.g., "REGISTER", "MESSAGE", "QUIT").
    :param path: The resource path (e.g., "/users", "/msg", "/p2p").
    :param headers: A dictionary of header fields (name: value).
    :param body: The message payload. Can be a string (will be UTF‑8 encoded)
                 or bytes (binary data, e.g., for media files).
    :return: The fully formatted request, including headers and body, as bytes.

    The format is:
        <command> <path> CHATPROTO/1.0
        Header1: value1
        Header2: value2
        ...
        <empty line>
        <body>

    This function automatically adds a Content-Length header.
    The same function is used for both text and binary transfers, supporting
    the protocol's ability to carry arbitrary media.
    """
    body_bytes = body if isinstance(body, bytes) else body.encode("utf-8")
    headers = dict(headers)
    headers["Content-Length"] = str(len(body_bytes))
    lines = [f"{command} {path} CHATPROTO/1.0"]
    for k, v in headers.items():
        lines.append(f"{k}: {v}")
    return (CRLF.join(lines) + CRLF + CRLF).encode("utf-8") + body_bytes


class StreamParser:
    """
    A simple streaming parser for CHATPROTO/1.0 messages received over a
    TCP socket. It buffers incoming data and extracts complete messages
    based on the double-CRLF separator and the Content-Length header.
    """
    def __init__(self):
        self.buf = b""

    def feed(self, data: bytes):
        """Append newly received data to the internal buffer."""
        self.buf += data

    def next_message(self) -> Optional[Tuple[str, Dict[str, str], str]]:
        """
        Attempts to extract one complete message from the buffer.

        :return: A tuple (start_line, headers, body) if a complete message is
                 available; otherwise None. The body is returned as a bytes
                 object (raw) so that binary content is preserved.
        """
        sep = b"\r\n\r\n"
        idx = self.buf.find(sep)
        if idx == -1:
            return None
        header_part = self.buf[:idx].decode("utf-8", errors="replace")
        remainder = self.buf[idx + 4:]
        lines = header_part.split("\r\n")
        if not lines:
            return None
        start_line = lines[0].strip()
        headers: Dict[str, str] = {}
        for line in lines[1:]:
            if ": " in line:
                k, v = line.split(": ", 1)
                headers[k.strip()] = v.strip()
        try:
            content_length = int(headers.get("Content-Length", "0"))
        except ValueError:
            content_length = 0
        if len(remainder) < content_length:
            return None
        body_bytes = remainder[:content_length]
        self.buf = remainder[content_length:]
        return start_line, headers, body_bytes


class ChatClient:
    """
    The main chat client class. It manages:
      - A persistent TCP connection to the server for signalling.
      - An optional UDP socket for receiving group messages (broadcast).
      - An optional P2P TCP server socket for direct file transfers.
    All network operations are non-blocking and handled in background threads.
    """
    def __init__(self, host: str, port: int, username: str):
        """
        :param host: Server hostname or IP address.
        :param port: Server TCP port.
        :param username: The user's chosen nickname.
        """
        self.host = host
        self.port = port
        self.username = username

        # --- UDP attributes (for group messages) ---
        self.udp_sock = None
        self.udp_port = None
        self.udp_running = False
        self.udp_thread = None

        # --- TCP signalling socket (to server) ---
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_lock = threading.Lock()
        self.parser = StreamParser()
        self.running = True

        # --- P2P attributes (direct TCP file transfers) ---
        self.p2p_port = None
        self.p2p_server_socket = None
        self.p2p_running = False
        self.p2p_listener_thread = None
        self.pending_p2p = {}          # transfer_id -> local file path

    def connect(self):
        """Establish the TCP connection to the chat server."""
        self.sock.connect((self.host, self.port))
        print(f"[OK] Connected to {self.host}:{self.port}")

    def send_bytes(self, data: bytes):
        """Thread‑safe sending of raw bytes over the server TCP connection."""
        with self.sock_lock:
            self.sock.sendall(data)

    def register(self):
        """
        Send a REGISTER request to the server.
        The server will record our username and the UDP port we are listening on
        for group messages (the UDP port is obtained by calling start_udp_listener).
        """
        udp_port = self.start_udp_listener(0)
        req = build_request(
            "REGISTER", "/users",
            headers={
                "From": self.username,
                "To": "server",
                "Msg-ID": str(int(time.time() * 1000)),
                "Timestamp": now_utc(),
                "Content-Type": "text/plain",
                "UDP-Port": str(udp_port),
            },
            body=""
        )
        self.send_bytes(req)

    def quit(self):
        """
        Send a QUIT request to the server, stop all background threads,
        and close all sockets.
        """
        self.stop_p2p_listener()
        req = build_request(
            "QUIT", "/users",
            headers={
                "From": self.username,
                "To": "server",
                "Msg-ID": str(int(time.time() * 1000)),
                "Timestamp": now_utc(),
                "Content-Type": "text/plain",
            },
            body=""
        )
        self.send_bytes(req)
        self.running = False
        try:
            self.sock.close()
        except OSError:
            pass
        self.udp_running = False
        if self.udp_sock:
            try:
                self.udp_sock.close()
            except OSError:
                pass

    def creategroup(self, group_id: str):
        """Send a CREATEGROUP request to create a new chat group."""
        req = build_request("CREATEGROUP", "/groups", headers={"From": self.username, "To": "server", "Group-ID": group_id, "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body="")
        self.send_bytes(req)

    def joingroup(self, group_id: str):
        """Send a JOINGROUP request to join an existing group."""
        req = build_request("JOINGROUP", "/groups", headers={"From": self.username, "To": "server", "Group-ID": group_id, "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body="")
        self.send_bytes(req)

    def leavegroup(self, group_id: str):
        """Send a LEAVEGROUP request to leave a group."""
        req = build_request("LEAVEGROUP", "/groups", headers={"From": self.username, "To": "server", "Group-ID": group_id, "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body="")
        self.send_bytes(req)

    def dm(self, target_user: str, text: str):
        """
        Send a direct text message to another user.
        The message is routed through the server (TCP).
        """
        req = build_request("MESSAGE", "/msg", headers={"From": self.username, "To": target_user, "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body=text)
        self.send_bytes(req)

    def groupmsg(self, group_id: str, text: str):
        """
        Send a text message to a group.
        The message is sent to the server over TCP; the server then
        distributes it via UDP to all group members who have registered
        a UDP port.
        """
        req = build_request("MESSAGE", "/msg", headers={"From": self.username, "To": group_id, "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body=text)
        self.send_bytes(req)

    def ping(self):
        """Send a PING request to keep the connection alive."""
        req = build_request("PING", "/ping", headers={"From": self.username, "To": "server", "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body="")
        self.send_bytes(req)

    def listusers(self):
        """
        Sends a LISTUSERS request to the server.
        The server responds with a newline-separated list of online usernames,
        allowing the client to discover who they can DM or invite to groups.
        """
        req = build_request(
            "LISTUSERS", "/users",
            headers={
                "From": self.username,
                "To": "server",
                "Msg-ID": str(int(time.time() * 1000)),
                "Timestamp": now_utc(),
                "Content-Type": "text/plain",
            },
            body=""
        )
        self.send_bytes(req)

    def sendmedia(self, target_user: str, filepath: str):
        """
        Sends a binary media file (image, audio, video) to another user via the
        server MESSAGE path. The file is read as raw bytes and sent with the
        correct Content-Type so the receiver knows how to handle it.
        For large files use /p2p instead (direct TCP, no server relay).
        """
        ext = os.path.splitext(filepath)[1].lower()
        content_type_map = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
            ".png": "image/png",  ".gif": "image/gif",
            ".mp3": "audio/mpeg", ".wav": "audio/wav",
            ".mp4": "video/mp4",  ".ogg": "audio/ogg",
        }
        content_type = content_type_map.get(ext, "application/octet-stream")

        with open(filepath, "rb") as f:
            file_bytes = f.read()

        filename = os.path.basename(filepath)
        req = build_request(
            "MESSAGE", "/msg",
            headers={
                "From":         self.username,
                "To":           target_user,
                "Msg-ID":       str(int(time.time() * 1000)),
                "Timestamp":    now_utc(),
                "Content-Type": content_type,
                "Filename":     filename,
            },
            body=file_bytes,
        )
        self.send_bytes(req)
        print(f"[MEDIA] Sent {filename} ({len(file_bytes)} bytes) to {target_user}")

    def p2p_listen(self, port: int = 0):
        """
        Inform the server that this client is willing to accept incoming
        P2P file transfers on the given port. The server records this
        information and shares it with peers who request a transfer.
        """
        req = build_request("P2PLISTEN", "/p2p", headers={"From": self.username, "To": "server", "P2P-Port": str(port), "Msg-ID": str(int(time.time() * 1000)), "Timestamp": now_utc(), "Content-Type": "text/plain"}, body="")
        self.send_bytes(req)
        print(f"[P2P] Sent listen port {port} to server.")

    def p2p_request(self, target_user: str, filepath: str):
        """
        Request the server to provide the IP address and listening port of
        another user so we can send them a file directly via TCP.
        The server replies with a 200 OK containing Peer-IP, Peer-Port,
        and a Transfer-ID that matches this request.
        """
        transfer_id = str(int(time.time() * 1000))
        self.pending_p2p[transfer_id] = filepath
        req = build_request("P2PREQS", "/p2p", headers={"From": self.username, "To": "server", "Target-Peer": target_user, "Transfer-ID": transfer_id, "Msg-ID": transfer_id, "Timestamp": now_utc(), "Content-Type": "text/plain"}, body="")
        self.send_bytes(req)
        print(f"[P2P] Requested peer info for {target_user} (transfer {transfer_id})")

    def start_p2p_listener(self, port: int = 0):
        """
        Start a background TCP server socket that listens for incoming
        P2P file transfers. If port == 0, an ephemeral port is chosen.
        The actual port is sent to the server via p2p_listen().
        """
        if self.p2p_running:
            print("[P2P] Listener already running.")
            return
        if port == 0:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("", 0))
                port = s.getsockname()[1]
        self.p2p_port = port
        self.p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.p2p_server_socket.bind(("", self.p2p_port))
        self.p2p_server_socket.listen(5)
        self.p2p_running = True
        self.p2p_listen(self.p2p_port)
        self.p2p_listener_thread = threading.Thread(target=self._p2p_listener_loop, daemon=True)
        self.p2p_listener_thread.start()
        print(f"[P2P] Listening for incoming transfers on port {self.p2p_port}")

    def stop_p2p_listener(self):
        """Shut down the P2P listener thread and close the server socket."""
        if self.p2p_running:
            self.p2p_running = False
            if self.p2p_server_socket:
                try:
                    self.p2p_server_socket.close()
                except OSError:
                    pass
            print("[P2P] Listener stopped.")

    def _p2p_listener_loop(self):
        """
        Background thread that accepts incoming P2P TCP connections.
        Each connection is handed off to _handle_p2p_client().
        """
        while self.p2p_running:
            try:
                conn, addr = self.p2p_server_socket.accept()
                print(f"[P2P] Incoming connection from {addr}")
                handler = threading.Thread(target=self._handle_p2p_client, args=(conn,), daemon=True)
                handler.start()
            except Exception as e:
                if self.p2p_running:
                    print(f"[P2P] Listener error: {e}")

    def _handle_p2p_client(self, conn: socket.socket):
        """
        Handle a single incoming P2P file transfer.
        The protocol is simple:
          1. Read a line: "SEND <size> <filename>\n"
          2. Read exactly <size> bytes of file data.
          3. Save the file with a "received_" prefix.
        """
        try:
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(1024)
                if not chunk:
                    return
                data += chunk
            header_line, rest = data.split(b"\n", 1)
            header = header_line.decode().strip()
            if not header.startswith("SEND"):
                print("[P2P] Invalid header from peer")
                return
            parts = header.split(maxsplit=2)
            if len(parts) != 3:
                print("[P2P] Malformed SEND header")
                return
            _, size_str, filename = parts
            filesize = int(size_str)
            save_filename = f"received_{filename}"
            with open(save_filename, "wb") as f:
                if rest:
                    f.write(rest)
                    filesize -= len(rest)
                remaining = filesize
                while remaining > 0:
                    chunk = conn.recv(min(4096, remaining))
                    if not chunk:
                        break
                    f.write(chunk)
                    remaining -= len(chunk)
            print(f"[P2P] File received from {conn.getpeername()} and saved as {save_filename}")
        except Exception as e:
            print(f"[P2P] Error receiving file: {e}")
        finally:
            conn.close()

    def send_file_via_p2p(self, peer_ip: str, peer_port: int, filepath: str, transfer_id: str):
        """
        Initiate a direct TCP file transfer to a peer.
        This is called after the server has provided the peer's connection info.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((peer_ip, peer_port))
            print(f"[P2P] Connected to peer {peer_ip}:{peer_port} for transfer {transfer_id}")
            filename = os.path.basename(filepath)
            filesize = os.path.getsize(filepath)
            header = f"SEND {filesize} {filename}\n".encode()
            sock.sendall(header)
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    sock.sendall(chunk)
            print(f"[P2P] File {filename} sent successfully (transfer {transfer_id})")
        except Exception as e:
            print(f"[P2P] Error sending file: {e}")
        finally:
            sock.close()
            self.pending_p2p.pop(transfer_id, None)

    def recv_loop(self):
        """
        Background thread that continuously reads from the server TCP socket,
        feeds data to the StreamParser, and dispatches complete messages to display().
        """
        try:
            while self.running:
                data = self.sock.recv(4096)
                if not data:
                    print("[SERVER] Connection closed.")
                    self.running = False
                    break
                self.parser.feed(data)
                while True:
                    parsed = self.parser.next_message()
                    if not parsed:
                        break
                    start_line, headers, body = parsed
                    self.display(start_line, headers, body)
        except ConnectionResetError:
            print("[SERVER] Connection reset.")
        except OSError:
            pass
        finally:
            self.running = False

    def display(self, start_line: str, headers: Dict[str, str], body):
        """
        Handles all incoming messages, whether they arrive over TCP or UDP.
        body may be bytes (binary media) or a string (text).
        Binary media is saved to disk; text is printed to the terminal.
        Also handles special server responses, e.g., 200 OK with peer info.
        """
        # ── Status responses (200 OK, 404, etc.) ──────────────────────────
        if start_line[:3].isdigit():
            peer_ip     = headers.get("Peer-IP")
            peer_port   = headers.get("Peer-Port")
            transfer_id = headers.get("Transfer-ID")
            if peer_ip and peer_port and transfer_id:
                print(f"[P2P] Peer info received: {peer_ip}:{peer_port} (transfer {transfer_id})")
                filepath = self.pending_p2p.get(transfer_id)
                if filepath:
                    t = threading.Thread(target=self.send_file_via_p2p,
                                         args=(peer_ip, int(peer_port), filepath, transfer_id),
                                         daemon=True)
                    t.start()
                else:
                    print(f"[P2P] Warning: No pending file for transfer {transfer_id}")
            else:
                body_text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
                status = start_line.replace(" CHATPROTO/1.0", "")
                if body_text.strip():
                    print(f"[SERVER] {status}")
                    for line in body_text.strip().splitlines():
                        print(f"  {line}")
                else:
                    print(f"[SERVER] {status}")
            return

        # ── Incoming MESSAGE (DM or group) ────────────────────────────────
        if start_line.startswith("MESSAGE"):
            from_user    = headers.get("From", "unknown")
            group_id     = headers.get("Group-ID")
            content_type = headers.get("Content-Type", "text/plain")
            filename     = headers.get("Filename")

            # Binary media — save to disk
            if not content_type.startswith("text/"):
                save_name = f"received_{filename}" if filename else f"received_media_{int(time.time())}"
                raw = body if isinstance(body, bytes) else body.encode("latin-1")
                with open(save_name, "wb") as f:
                    f.write(raw)
                tag = f"[{group_id}]" if group_id else "[DM]"
                print(f"{tag} {from_user} sent media ({content_type}), saved as {save_name}")
            else:
                body_text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
                if group_id:
                    print(f"[{group_id}] {from_user}: {body_text}")
                else:
                    print(f"[DM] {from_user}: {body_text}")
            return

        # ── PONG ──────────────────────────────────────────────────────────
        if start_line.startswith("PONG"):
            print("[SERVER] PONG")
            return

        # ── Fallback ──────────────────────────────────────────────────────
        body_text = body.decode("utf-8", errors="replace") if isinstance(body, bytes) else body
        print(f"[IN] {start_line} {headers} {body_text}".strip())

    def start_udp_listener(self, port: int = 0):
        """
        Create a UDP socket and bind it to a port.
        The socket will be used to receive group messages from the server.
        The actual port is returned and later sent to the server during REGISTER.
        """
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("", port))
        self.udp_port = self.udp_sock.getsockname()[1]
        self.udp_running = True
        self.udp_thread = threading.Thread(target=self._udp_recv_loop, daemon=True)
        self.udp_thread.start()
        print(f"[UDP] Listening for group messages on port {self.udp_port}")
        return self.udp_port

    def _udp_recv_loop(self):
        """
        Background thread that receives UDP datagrams, parses them as
        CHATPROTO/1.0 messages (simple header+body with CRLF separation),
        and passes them to display().
        """
        while self.udp_running:
            try:
                data, _ = self.udp_sock.recvfrom(65535)
                raw = data.decode("utf-8", errors="replace")
                if "\r\n\r\n" in raw:
                    header_part, body = raw.split("\r\n\r\n", 1)
                    lines = header_part.split("\r\n")
                    start_line = lines[0]
                    headers = {}
                    for line in lines[1:]:
                        if ": " in line:
                            k, v = line.split(": ", 1)
                            headers[k] = v
                    content_length = int(headers.get("Content-Length", 0))
                    body = body[:content_length]
                    self.display(start_line, headers, body)
            except Exception as e:
                if self.udp_running:
                    print(f"[UDP] Receive error: {e}")
                break

    def run_cli(self):
        """
        Main command-line interface loop. Reads user commands and calls the
        corresponding methods. Commands are prefixed with '/' for clarity.
        """
        print("Commands:")
        print("  /listusers                   (show online users)")
        print("  /creategroup <group>")
        print("  /joingroup <group>")
        print("  /leavegroup <group>")
        print("  /dm <user> <message...>")
        print("  /group <group> <message...>")
        print("  /sendmedia <user> <filepath> (send image/audio/video via server)")
        print("  /p2plisten [port]            (start P2P listener)")
        print("  /p2p <user> <filepath>       (send file directly via P2P)")
        print("  /ping")
        print("  /quit")

        while self.running:
            try:
                line = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                self.quit()
                break
            if not line:
                continue
            if line == "/quit":
                self.quit()
                break
            if line == "/ping":
                self.ping()
                continue
            if line.startswith("/creategroup "):
                parts = line.split()
                if len(parts) != 2:
                    print("Usage: /creategroup <group>")
                    continue
                self.creategroup(parts[1])
                continue
            if line.startswith("/joingroup "):
                parts = line.split()
                if len(parts) != 2:
                    print("Usage: /joingroup <group>")
                    continue
                self.joingroup(parts[1])
                continue
            if line.startswith("/leavegroup "):
                parts = line.split()
                if len(parts) != 2:
                    print("Usage: /leavegroup <group>")
                    continue
                self.leavegroup(parts[1])
                continue
            if line.startswith("/dm "):
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    print("Usage: /dm <user> <message...>")
                    continue
                self.dm(parts[1], parts[2])
                continue
            if line.startswith("/group "):
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    print("Usage: /group <group> <message...>")
                    continue
                self.groupmsg(parts[1], parts[2])
                continue
            if line == "/listusers":
                self.listusers()
                continue

            if line.startswith("/sendmedia "):
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    print("Usage: /sendmedia <user> <filepath>")
                    continue
                target   = parts[1]
                filepath = parts[2]
                if not os.path.isfile(filepath):
                    print(f"File not found: {filepath}")
                    continue
                self.sendmedia(target, filepath)
                continue

            if line.startswith("/p2plisten"):
                parts = line.split()
                if len(parts) == 1:
                    self.start_p2p_listener(0)
                elif len(parts) == 2:
                    try:
                        port = int(parts[1])
                        self.start_p2p_listener(port)
                    except ValueError:
                        print("Usage: /p2plisten [port]  (port must be an integer)")
                else:
                    print("Usage: /p2plisten [port]")
                continue
            if line.startswith("/p2p "):
                parts = line.split(" ", 2)
                if len(parts) < 3:
                    print("Usage: /p2p <user> <filepath>")
                    continue
                target = parts[1]
                filepath = parts[2]
                if not os.path.isfile(filepath):
                    print(f"File not found: {filepath}")
                    continue
                self.p2p_request(target, filepath)
                continue
            print("Unknown command.")

def main():
    """
    Entry point: prompt for server details and username, create a ChatClient,
    connect, start the receive thread, register, and run the CLI.
    """
    host = input("Server IP (default 127.0.0.1): ").strip() or "127.0.0.1"
    port_str = input("Server Port (default 9000): ").strip() or "9000"
    username = input("Username: ").strip()
    port = int(port_str)
    c = ChatClient(host, port, username)
    c.connect()
    t = threading.Thread(target=c.recv_loop, daemon=True)
    t.start()
    c.register()
    c.run_cli()

if __name__ == "__main__":
    main()