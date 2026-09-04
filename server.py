import socket
import threading
import datetime
import uuid

# ── Global state ──────────────────────────────────────────────────────────────
user_directory = {}          # username -> {ip, port, status, conn, p2p_port, udp_port}
user_directory_lock = threading.Lock()

groups = {}                  # group_id -> set of usernames
groups_lock = threading.Lock()

udp_sock = None              # UDP socket used to forward group messages
UDP_PORT = 9001


# ── Message building ──────────────────────────────────────────────────────────

def build_response_bytes(status: str, from_: str, to: str, msg_id: str,
                         extra_headers: dict = None,
                         body: bytes = b"",
                         content_type: str = "text/plain") -> bytes:
    """
    Builds a complete CHATPROTO/1.0 response as raw bytes.
    Accepts a bytes body so binary media (images, audio, video) is never
    corrupted by string encoding. Text callers can pass body as a str and
    it will be encoded to UTF-8 automatically.
    """
    if isinstance(body, str):
        body = body.encode("utf-8")

    timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    if not msg_id:
        msg_id = uuid.uuid4().hex[:8]

    lines = [
        f"{status} CHATPROTO/1.0",
        f"From: {from_}",
        f"To: {to}",
        f"Msg-ID: {msg_id}",
        f"Timestamp: {timestamp}",
        f"Content-Type: {content_type}",
        f"Content-Length: {len(body)}",
    ]
    if extra_headers:
        for k, v in extra_headers.items():
            lines.append(f"{k}: {v}")

    header_str = "\r\n".join(lines) + "\r\n\r\n"
    return header_str.encode("utf-8") + body


def build_response(status: str, from_: str, to: str, msg_id: str,
                   extra_headers: dict = None, body: str = "") -> bytes:
    """
    Convenience wrapper for text-only responses.
    Returns bytes (same as build_response_bytes) so all callers use sendall()
    consistently without a separate .encode() call.
    """
    return build_response_bytes(status, from_, to, msg_id,
                                extra_headers=extra_headers,
                                body=body.encode("utf-8") if isinstance(body, str) else body)


# ── Message parsing ───────────────────────────────────────────────────────────

def parse_message(header_bytes: bytes, body_bytes: bytes) -> dict:
    """
    Parses a message that has already been split into header block and body.
    Both arguments are raw bytes. The header is decoded as UTF-8 (it is always
    ASCII text). The body is kept as bytes so binary payloads are preserved.

    Returns:
        {
          "command": str,
          "headers": dict[str, str],
          "body":    bytes          # raw bytes — callers decode if they need text
        }
    """
    result = {"command": None, "headers": {}, "body": body_bytes}

    header_str = header_bytes.decode("utf-8", errors="replace")
    lines = header_str.split("\r\n")

    if lines:
        first_parts = lines[0].split(" ")
        result["command"] = first_parts[0] if first_parts else None

    for line in lines[1:]:
        if ": " in line:
            key, value = line.split(": ", 1)
            result["headers"][key.strip()] = value.strip()

    return result


# ── Command handlers ──────────────────────────────────────────────────────────

def handle_register(msg: dict, conn: socket.socket, addr) -> bytes:
    """
    REGISTER – adds a user to the directory.
    Reads optional UDP-Port header so the server knows where to send UDP group msgs.
    """
    username = msg["headers"].get("From")
    if not username:
        return build_response("400 Bad Request", "server", "unknown", "",
                              body="Missing From header")

    udp_port_str = msg["headers"].get("UDP-Port")
    try:
        udp_port = int(udp_port_str) if udp_port_str else None
    except ValueError:
        udp_port = None

    with user_directory_lock:
        if username in user_directory:
            return build_response("409 Conflict", "server", username, "",
                                  body="Username already taken")
        user_directory[username] = {
            "ip":       addr[0],
            "port":     addr[1],
            "status":   "online",
            "conn":     conn,
            "p2p_port": None,
            "udp_port": udp_port,
        }

    print(f"[REGISTER] {username} from {addr} (UDP port: {udp_port})")
    return build_response("200 OK", "server", username, "",
                          body="Registration successful. Welcome.")


def handle_quit(msg: dict, conn: socket.socket) -> bytes:
    """QUIT – removes the user from the directory and all groups."""
    username = msg["headers"].get("From")
    with user_directory_lock:
        if username in user_directory:
            del user_directory[username]
    with groups_lock:
        for members in groups.values():
            members.discard(username)
    print(f"[QUIT] {username} disconnected")
    return build_response("200 OK", "server", username or "unknown", "",
                          body="Goodbye.")


def handle_list_users(msg: dict) -> bytes:
    """
    LISTUSERS – returns a newline-separated list of all currently online users.
    Allows clients to discover who they can DM or invite to groups.
    """
    username = msg["headers"].get("From", "unknown")
    with user_directory_lock:
        online = [u for u in user_directory if u != username]
    body = "\n".join(online) if online else "(no other users online)"
    return build_response("200 OK", "server", username, "", body=body)


def handle_create_group(msg: dict) -> bytes:
    username = msg["headers"].get("From")
    group_id = msg["headers"].get("Group-ID")
    if not group_id:
        return build_response("400 Bad Request", "server", username, "",
                              body="Missing Group-ID")
    with groups_lock:
        if group_id in groups:
            return build_response("409 Conflict", "server", username, "",
                                  body="Group already exists")
        groups[group_id] = {username}
    print(f"[CREATEGROUP] {username} created '{group_id}'")
    return build_response("200 OK", "server", username, "",
                          body=f"Group {group_id} created.")


def handle_join_group(msg: dict) -> bytes:
    username = msg["headers"].get("From")
    group_id = msg["headers"].get("Group-ID")
    with groups_lock:
        if group_id not in groups:
            return build_response("404 Not Found", "server", username, "",
                                  body=f"Group not found: {group_id}")
        groups[group_id].add(username)
    print(f"[JOINGROUP] {username} joined '{group_id}'")
    return build_response("200 OK", "server", username, "",
                          body=f"Joined group {group_id}.")


def handle_leave_group(msg: dict) -> bytes:
    username = msg["headers"].get("From")
    group_id = msg["headers"].get("Group-ID")
    with groups_lock:
        if group_id in groups:
            groups[group_id].discard(username)
    print(f"[LEAVEGROUP] {username} left '{group_id}'")
    return build_response("200 OK", "server", username, "",
                          body=f"Left group {group_id}.")


def handle_message(msg: dict) -> None:
    """
    MESSAGE – forwards text or binary content to a user or group.

    The body is kept as raw bytes throughout so binary media sent via the
    server path (e.g. small images) is not corrupted.

    Group messages are forwarded over UDP if the target registered a UDP port;
    otherwise they fall back to TCP.  Direct messages always use TCP.
    """
    sender    = msg["headers"].get("From")
    recipient = msg["headers"].get("To")
    body      = msg["body"]   # bytes
    content_type = msg["headers"].get("Content-Type", "text/plain")

    with groups_lock:
        is_group = recipient in groups
        members  = set(groups[recipient]) if is_group else set()

    if is_group:
        # Enforce membership
        if sender not in members:
            with user_directory_lock:
                sender_info = user_directory.get(sender)
            if sender_info:
                err = build_response("403 Forbidden", "server", sender, "",
                                     body=f"You are not a member of group: {recipient}")
                sender_info["conn"].sendall(err)
            print(f"[DENY] {sender} tried to post to '{recipient}' without joining")
            return

        targets = members - {sender}
        for member in targets:
            with user_directory_lock:
                target_info = user_directory.get(member)
            if not target_info:
                continue
            forward = build_response_bytes(
                "MESSAGE", sender, member, "",
                extra_headers={"Group-ID": recipient},
                body=body,
                content_type=content_type,
            )
            # ── UDP path for group messages ────────────────────────────────
            if target_info.get("udp_port") and udp_sock:
                try:
                    dest = (target_info["ip"], target_info["udp_port"])
                    udp_sock.sendto(forward, dest)
                    print(f"[MSG/UDP] {sender} → group '{recipient}' → {member}")
                except Exception as e:
                    print(f"[ERROR] UDP to {member}: {e}")
            else:
                # ── TCP fallback ───────────────────────────────────────────
                try:
                    target_info["conn"].sendall(forward)

                    print(f"[MSG/TCP-fallback] {sender} → group '{recipient}' → {member}")
                except Exception as e:
                    print(f"[ERROR] TCP to {member}: {e}")

    else:
        # Direct message — always TCP
        with user_directory_lock:
            target_info = user_directory.get(recipient)
            sender_info = user_directory.get(sender)

        if target_info:
            forward = build_response_bytes(
                "MESSAGE", sender, recipient, "",
                body=body,
                content_type=content_type,
            )
            try:
                target_info["conn"].sendall(forward)
                print(f"[MSG/TCP] {sender} → {recipient}")
            except Exception as e:
                print(f"[ERROR] TCP to {recipient}: {e}")
        else:
            if sender_info:
                err = build_response("404 Not Found", "server", sender, "",
                                     body=f"User not found: {recipient}")
                sender_info["conn"].sendall(err)


# ── P2P handlers ──────────────────────────────────────────────────────────────

def handle_p2p_listen(msg: dict, conn: socket.socket) -> bytes:
    username     = msg["headers"].get("From")
    p2p_port_str = msg["headers"].get("P2P-Port")
    if not p2p_port_str:
        return build_response("400 Bad Request", "server", username, "",
                              body="Missing P2P-Port header")
    try:
        p2p_port = int(p2p_port_str)
    except ValueError:
        return build_response("400 Bad Request", "server", username, "",
                              body="Invalid P2P-Port (must be integer)")

    with user_directory_lock:
        if username not in user_directory:
            return build_response("404 Not Found", "server", username, "",
                                  body="User not registered")
        user_directory[username]["p2p_port"] = p2p_port

    print(f"[P2PLISTEN] {username} P2P port → {p2p_port}")
    return build_response("200 OK", "server", username, "",
                          body="P2P port registered.")


def handle_p2p_request(msg: dict) -> bytes:
    requester   = msg["headers"].get("From")
    target      = msg["headers"].get("Target-Peer")
    transfer_id = msg["headers"].get("Transfer-ID")

    if not target:
        return build_response("400 Bad Request", "server", requester, transfer_id,
                              body="Missing Target-Peer header")
    if not transfer_id:
        return build_response("400 Bad Request", "server", requester, "",
                              body="Missing Transfer-ID header")

    with user_directory_lock:
        target_info = user_directory.get(target)

    if not target_info:
        return build_response("404 Not Found", "server", requester, transfer_id,
                              body=f"User {target} not found")
    if target_info["p2p_port"] is None:
        return build_response("503 Service Unavailable", "server", requester, transfer_id,
                              body=f"User {target} is not accepting P2P transfers")

    return build_response("200 OK", "server", requester, transfer_id,
                          extra_headers={
                              "Peer-IP":      target_info["ip"],
                              "Peer-Port":    str(target_info["p2p_port"]),
                              "Transfer-ID":  transfer_id,
                          },
                          body="Peer info follows")


# ── Per-client receive loop ───────────────────────────────────────────────────

def handle_client(conn: socket.socket, addr):
    """
    Reads bytes from the socket into a bytes buffer (not a str buffer).
    This is the key fix for binary media: we never decode the body, only
    the header block which is always ASCII/UTF-8 text.
    """
    print(f"[CONNECT] New connection from {addr}")
    buf = b""   # ← bytes buffer, not str
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data

            # Process all complete messages in the buffer
            while b"\r\n\r\n" in buf:
                sep_idx = buf.index(b"\r\n\r\n")
                header_bytes = buf[:sep_idx]
                remainder    = buf[sep_idx + 4:]

                # Parse Content-Length from the header block
                content_length = 0
                for line in header_bytes.decode("utf-8", errors="replace").split("\r\n")[1:]:
                    if line.lower().startswith("content-length:"):
                        try:
                            content_length = int(line.split(": ", 1)[1])
                        except ValueError:
                            pass
                        break

                # Wait until the full body has arrived
                if len(remainder) < content_length:
                    break

                body_bytes = remainder[:content_length]
                buf        = remainder[content_length:]

                msg     = parse_message(header_bytes, body_bytes)
                command = msg["command"]
                response = None

                if command == "REGISTER":
                    response = handle_register(msg, conn, addr)
                elif command == "QUIT":
                    response = handle_quit(msg, conn)
                    conn.sendall(response)
                    return
                elif command == "LISTUSERS":
                    response = handle_list_users(msg)
                elif command == "CREATEGROUP":
                    response = handle_create_group(msg)
                elif command == "JOINGROUP":
                    response = handle_join_group(msg)
                elif command == "LEAVEGROUP":
                    response = handle_leave_group(msg)
                elif command == "MESSAGE":
                    handle_message(msg)
                elif command == "PING":
                    sender = msg["headers"].get("From", "unknown")
                    response = build_response("PONG", "server", sender, "")
                elif command == "P2PLISTEN":
                    response = handle_p2p_listen(msg, conn)
                elif command == "P2PREQS":
                    response = handle_p2p_request(msg)
                else:
                    response = build_response("400 Bad Request", "server", "unknown", "",
                                              body=f"Unknown command: {command}")

                if response:
                    conn.sendall(response)   # response is already bytes

    except ConnectionResetError:
        print(f"[DISCONNECT] {addr} reset")
    except Exception as e:
        print(f"[ERROR] {addr}: {e}")
    finally:
        with user_directory_lock:
            to_remove = [u for u, info in user_directory.items() if info["conn"] is conn]
            for u in to_remove:
                del user_directory[u]
                print(f"[CLEANUP] Removed {u}")
        conn.close()


# ── Server startup ────────────────────────────────────────────────────────────

def start_server(host="0.0.0.0", port=9000):
    global udp_sock

    # UDP socket for group message delivery
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.bind((host, UDP_PORT))
    print(f"[SERVER] UDP bound on {host}:{UDP_PORT}")

    tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp_sock.bind((host, port))
    tcp_sock.listen(10)
    print(f"[SERVER] TCP listening on {host}:{port}")

    try:
        while True:
            conn, addr = tcp_sock.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
            print(f"[THREADS] Active: {threading.active_count() - 1}")
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down.")
    finally:
        tcp_sock.close()
        udp_sock.close()


if __name__ == "__main__":
    start_server()
