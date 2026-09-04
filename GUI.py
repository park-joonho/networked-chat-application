"""
CHATPROTO/1.0 GUI Client
CSC3002F Networks Assignment 2026
Group 25: Joon-ho Park, Dennis Zhu, Jordan Rix

AI Usage: GUI layout and Tkinter threading pattern generated with AI assistance.
All protocol logic is unchanged from the CLI client and understood by the group.
"""

import socket
import threading
import time
import os
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
from typing import Dict, Optional, Tuple

CRLF = "\r\n"

# ── Protocol helpers (identical to cli client) ────────────────────────────────

def now_utc() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def build_request(command: str, path: str, headers: Dict[str, str], body=b"") -> bytes:
    body_bytes = body if isinstance(body, bytes) else body.encode("utf-8")
    headers = dict(headers)
    headers["Content-Length"] = str(len(body_bytes))
    lines = [f"{command} {path} CHATPROTO/1.0"]
    for k, v in headers.items():
        lines.append(f"{k}: {v}")
    return (CRLF.join(lines) + CRLF + CRLF).encode("utf-8") + body_bytes


class StreamParser:
    def __init__(self):
        self.buf = b""

    def feed(self, data: bytes):
        self.buf += data

    def next_message(self) -> Optional[Tuple[str, Dict[str, str], bytes]]:
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
        return start_line, headers, body_bytes  # body stays as bytes


# ── Chat backend (network logic, no UI code) ──────────────────────────────────

class ChatBackend:
    """
    Handles all socket communication. Calls self.on_message(event_dict)
    on the main thread via Tkinter's after() whenever something arrives.
    """

    def __init__(self, host, port, username, on_message):
        self.host = host
        self.port = port
        self.username = username
        self.on_message = on_message   # callback into the GUI

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock_lock = threading.Lock()
        self.parser = StreamParser()
        self.running = False

        self.udp_sock = None
        self.udp_port = None
        self.udp_running = False

        self.p2p_port = None
        self.p2p_server_socket = None
        self.p2p_running = False
        self.pending_p2p = {}   # transfer_id -> filepath

    # ── Connection ────────────────────────────────────────────────────────────

    def connect(self):
        self.sock.connect((self.host, self.port))
        self.running = True
        threading.Thread(target=self._recv_loop, daemon=True).start()
        udp_port = self._start_udp_listener()
        self._send(build_request("REGISTER", "/users", {
            "From": self.username, "To": "server",
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
            "UDP-Port": str(udp_port),
        }))

    def disconnect(self):
        self.running = False
        self.udp_running = False
        self.p2p_running = False
        try:
            self._send(build_request("QUIT", "/users", {
                "From": self.username, "To": "server",
                "Msg-ID": str(int(time.time() * 1000)),
                "Timestamp": now_utc(), "Content-Type": "text/plain",
            }))
        except Exception:
            pass
        for s in [self.sock, self.udp_sock, self.p2p_server_socket]:
            try:
                if s: s.close()
            except Exception:
                pass

    def _send(self, data: bytes):
        with self.sock_lock:
            self.sock.sendall(data)

    # ── Receive loop ──────────────────────────────────────────────────────────

    def _recv_loop(self):
        try:
            while self.running:
                data = self.sock.recv(4096)
                if not data:
                    self._emit({"type": "server", "text": "Connection closed."})
                    break
                self.parser.feed(data)
                while True:
                    parsed = self.parser.next_message()
                    if not parsed:
                        break
                    self._handle_incoming(*parsed)
        except (ConnectionResetError, OSError):
            pass
        finally:
            self.running = False

    def _handle_incoming(self, start_line: str, headers: dict, body: bytes):
        body_text = body.decode("utf-8", errors="replace")

        # P2P peer info response
        if start_line[:3].isdigit():
            peer_ip = headers.get("Peer-IP")
            peer_port = headers.get("Peer-Port")
            transfer_id = headers.get("Transfer-ID")
            if peer_ip and peer_port and transfer_id:
                filepath = self.pending_p2p.get(transfer_id)
                if filepath:
                    threading.Thread(
                        target=self._send_file_p2p,
                        args=(peer_ip, int(peer_port), filepath, transfer_id),
                        daemon=True
                    ).start()
                return
            status = start_line.replace(" CHATPROTO/1.0", "")
            self._emit({"type": "server", "text": f"{status}: {body_text}".strip()})
            return

        if start_line.startswith("MESSAGE"):
            from_user = headers.get("From", "unknown")
            group_id = headers.get("Group-ID")
            content_type = headers.get("Content-Type", "text/plain")
            filename = headers.get("Filename")

            if not content_type.startswith("text/"):
                # Save binary media to disk
                save_name = f"received_{filename}" if filename else f"received_media_{int(time.time())}"
                with open(save_name, "wb") as f:
                    f.write(body)
                self._emit({
                    "type": "media",
                    "from": from_user,
                    "group": group_id,
                    "filename": save_name,
                    "content_type": content_type,
                })
            else:
                self._emit({
                    "type": "message",
                    "from": from_user,
                    "group": group_id,
                    "text": body_text,
                })
            return

        if start_line.startswith("PONG"):
            self._emit({"type": "server", "text": "PONG received"})
            return

    def _emit(self, event: dict):
        """Thread-safe: schedules the GUI callback on the Tkinter main thread."""
        self.on_message(event)

    # ── UDP listener (group messages) ─────────────────────────────────────────

    def _start_udp_listener(self) -> int:
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind(("", 0))
        self.udp_port = self.udp_sock.getsockname()[1]
        self.udp_running = True
        threading.Thread(target=self._udp_loop, daemon=True).start()
        return self.udp_port

    def _udp_loop(self):
        while self.udp_running:
            try:
                data, _ = self.udp_sock.recvfrom(65535)
                sep = b"\r\n\r\n"
                if sep in data:
                    idx = data.index(sep)
                    header_part = data[:idx].decode("utf-8", errors="replace")
                    body = data[idx + 4:]
                    lines = header_part.split("\r\n")
                    start_line = lines[0]
                    headers = {}
                    for line in lines[1:]:
                        if ": " in line:
                            k, v = line.split(": ", 1)
                            headers[k] = v
                    content_length = int(headers.get("Content-Length", 0))
                    body = body[:content_length]
                    self._handle_incoming(start_line, headers, body)
            except Exception:
                if self.udp_running:
                    pass
                break

    # ── P2P ───────────────────────────────────────────────────────────────────

    def start_p2p_listener(self) -> int:
        if self.p2p_running:
            return self.p2p_port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            port = s.getsockname()[1]
        self.p2p_port = port
        self.p2p_server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.p2p_server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.p2p_server_socket.bind(("", port))
        self.p2p_server_socket.listen(5)
        self.p2p_running = True
        self._send(build_request("P2PLISTEN", "/p2p", {
            "From": self.username, "To": "server",
            "P2P-Port": str(port),
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }))
        threading.Thread(target=self._p2p_accept_loop, daemon=True).start()
        return port

    def _p2p_accept_loop(self):
        while self.p2p_running:
            try:
                conn, addr = self.p2p_server_socket.accept()
                threading.Thread(target=self._p2p_receive, args=(conn,), daemon=True).start()
            except Exception:
                break

    def _p2p_receive(self, conn):
        try:
            data = b""
            while b"\n" not in data:
                chunk = conn.recv(1024)
                if not chunk:
                    return
                data += chunk
            header_line, rest = data.split(b"\n", 1)
            parts = header_line.decode().strip().split(maxsplit=2)
            if len(parts) != 3 or parts[0] != "SEND":
                return
            filesize = int(parts[1])
            filename = parts[2]
            save_name = f"received_{filename}"
            with open(save_name, "wb") as f:
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
            self._emit({"type": "p2p_received", "filename": save_name})
        except Exception as e:
            self._emit({"type": "server", "text": f"P2P receive error: {e}"})
        finally:
            conn.close()

    def _send_file_p2p(self, peer_ip, peer_port, filepath, transfer_id):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((peer_ip, peer_port))
            filename = os.path.basename(filepath)
            filesize = os.path.getsize(filepath)
            s.sendall(f"SEND {filesize} {filename}\n".encode())
            with open(filepath, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    s.sendall(chunk)
            self._emit({"type": "server", "text": f"P2P: sent {filename} successfully."})
        except Exception as e:
            self._emit({"type": "server", "text": f"P2P send error: {e}"})
        finally:
            s.close()
            self.pending_p2p.pop(transfer_id, None)

    # ── Protocol actions ──────────────────────────────────────────────────────

    def send_dm(self, target: str, text: str):
        self._send(build_request("MESSAGE", "/msg", {
            "From": self.username, "To": target,
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }, body=text))

    def send_group(self, group_id: str, text: str):
        self._send(build_request("MESSAGE", "/msg", {
            "From": self.username, "To": group_id,
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }, body=text))

    def send_media(self, target: str, filepath: str):
        ext = os.path.splitext(filepath)[1].lower()
        ct_map = {
            ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif",  ".mp3": "audio/mpeg", ".wav": "audio/wav",
            ".mp4": "video/mp4",  ".ogg": "audio/ogg",
        }
        content_type = ct_map.get(ext, "application/octet-stream")
        with open(filepath, "rb") as f:
            file_bytes = f.read()
        self._send(build_request("MESSAGE", "/msg", {
            "From": self.username, "To": target,
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": content_type,
            "Filename": os.path.basename(filepath),
        }, body=file_bytes))

    def send_p2p_request(self, target: str, filepath: str):
        transfer_id = str(int(time.time() * 1000))
        self.pending_p2p[transfer_id] = filepath
        self._send(build_request("P2PREQS", "/p2p", {
            "From": self.username, "To": "server",
            "Target-Peer": target, "Transfer-ID": transfer_id,
            "Msg-ID": transfer_id, "Timestamp": now_utc(),
            "Content-Type": "text/plain",
        }))

    def list_users(self):
        self._send(build_request("LISTUSERS", "/users", {
            "From": self.username, "To": "server",
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }))

    def create_group(self, group_id: str):
        self._send(build_request("CREATEGROUP", "/groups", {
            "From": self.username, "To": "server", "Group-ID": group_id,
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }))

    def join_group(self, group_id: str):
        self._send(build_request("JOINGROUP", "/groups", {
            "From": self.username, "To": "server", "Group-ID": group_id,
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }))

    def leave_group(self, group_id: str):
        self._send(build_request("LEAVEGROUP", "/groups", {
            "From": self.username, "To": "server", "Group-ID": group_id,
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }))

    def ping(self):
        self._send(build_request("PING", "/ping", {
            "From": self.username, "To": "server",
            "Msg-ID": str(int(time.time() * 1000)),
            "Timestamp": now_utc(), "Content-Type": "text/plain",
        }))


# ── Login window ──────────────────────────────────────────────────────────────

class LoginWindow:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("CHATPROTO — Connect")
        self.root.resizable(False, False)
        self.result = None
        self._build()
        # Centre on screen
        self.root.update_idletasks()
        w, h = 360, 260
        x = (self.root.winfo_screenwidth() - w) // 2
        y = (self.root.winfo_screenheight() - h) // 2
        self.root.geometry(f"{w}x{h}+{x}+{y}")

    def _build(self):
        bg = "#0f0f0f"
        fg = "#e0e0e0"
        accent = "#00c896"
        self.root.configure(bg=bg)

        tk.Label(self.root, text="CHATPROTO/1.0", font=("Courier", 18, "bold"),
                 bg=bg, fg=accent).pack(pady=(24, 4))
        tk.Label(self.root, text="CSC3002F · Group 25", font=("Courier", 9),
                 bg=bg, fg="#555").pack(pady=(0, 20))

        frame = tk.Frame(self.root, bg=bg)
        frame.pack(padx=32, fill="x")

        for label, attr, default in [
            ("Server IP", "ip_var", "127.0.0.1"),
            ("Port",      "port_var", "9000"),
            ("Username",  "user_var", ""),
        ]:
            tk.Label(frame, text=label, font=("Courier", 10), bg=bg, fg=fg,
                     anchor="w").pack(fill="x")
            var = tk.StringVar(value=default)
            setattr(self, attr, var)
            e = tk.Entry(frame, textvariable=var, font=("Courier", 11),
                         bg="#1e1e1e", fg=fg, insertbackground=fg,
                         relief="flat", bd=4)
            e.pack(fill="x", pady=(0, 8))

        btn = tk.Button(self.root, text="CONNECT", font=("Courier", 11, "bold"),
                        bg=accent, fg="#000", relief="flat", cursor="hand2",
                        command=self._submit)
        btn.pack(pady=12, ipadx=20, ipady=6)
        self.root.bind("<Return>", lambda e: self._submit())

    def _submit(self):
        ip   = self.ip_var.get().strip()
        port = self.port_var.get().strip()
        user = self.user_var.get().strip()
        if not ip or not port or not user:
            messagebox.showerror("Error", "All fields are required.")
            return
        try:
            port = int(port)
        except ValueError:
            messagebox.showerror("Error", "Port must be a number.")
            return
        self.result = (ip, port, user)
        self.root.destroy()


# ── Main chat window ──────────────────────────────────────────────────────────

class ChatWindow:
    BG       = "#0f0f0f"
    PANEL    = "#161616"
    BORDER   = "#2a2a2a"
    FG       = "#e0e0e0"
    FG_DIM   = "#666"
    ACCENT   = "#00c896"
    SELF_CLR = "#00c896"
    OTHER_CLR= "#888"
    SYS_CLR  = "#444"
    FONT_MONO= ("Courier", 10)
    FONT_MSG = ("Courier", 11)
    FONT_SM  = ("Courier", 9)

    def __init__(self, root: tk.Tk, host: str, port: int, username: str):
        self.root = root
        self.username = username
        self.current_chat = None   # None = no chat open; str = user/group name
        self.chat_logs: Dict[str, list] = {"[server]": []}
        self.p2p_active = False

        self.root.title(f"CHATPROTO — {username}")
        self.root.configure(bg=self.BG)
        self.root.geometry("900x620")
        self.root.minsize(700, 480)

        self._build_ui()
        self._open_chat("[server]")

        # Connect backend — use after() so the window is visible first
        self.backend = ChatBackend(host, port, username, self._on_event)
        try:
            self.backend.connect()
        except Exception as e:
            messagebox.showerror("Connection failed", str(e))
            self.root.destroy()
            return

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self._append("[server]", "system", f"Connected as {username}")

    # ── UI construction ───────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Left sidebar ──────────────────────────────────────────────────────
        sidebar = tk.Frame(self.root, bg=self.PANEL, width=200)
        sidebar.pack(side="left", fill="y")
        sidebar.pack_propagate(False)

        tk.Label(sidebar, text="CHATPROTO", font=("Courier", 12, "bold"),
                 bg=self.PANEL, fg=self.ACCENT).pack(pady=(16, 2))
        tk.Label(sidebar, text=f"@{self.username}", font=self.FONT_SM,
                 bg=self.PANEL, fg=self.FG_DIM).pack(pady=(0, 12))

        # Quick-action buttons
        for label, cmd in [
            ("＋ New DM",      self._prompt_dm),
            ("＋ New Group",   self._prompt_group),
            ("↺ List Users",   self._do_list_users),
            ("◎ P2P Listen",   self._do_p2p_listen),
            ("⌁ Ping",         self._do_ping),
        ]:
            tk.Button(sidebar, text=label, font=self.FONT_SM,
                      bg=self.PANEL, fg=self.FG, relief="flat",
                      activebackground=self.BORDER, activeforeground=self.ACCENT,
                      cursor="hand2", anchor="w", padx=12,
                      command=cmd).pack(fill="x", ipady=5)

        tk.Frame(sidebar, bg=self.BORDER, height=1).pack(fill="x", pady=8)

        tk.Label(sidebar, text="CONVERSATIONS", font=("Courier", 8),
                 bg=self.PANEL, fg=self.FG_DIM).pack(anchor="w", padx=12)

        # Conversation list
        self.conv_list = tk.Listbox(
            sidebar, bg=self.PANEL, fg=self.FG,
            selectbackground=self.BORDER, selectforeground=self.ACCENT,
            font=self.FONT_SM, relief="flat", bd=0,
            activestyle="none", cursor="hand2",
        )
        self.conv_list.pack(fill="both", expand=True, padx=4, pady=4)
        self.conv_list.bind("<<ListboxSelect>>", self._on_conv_select)
        self.conv_list.insert("end", "[server]")

        # ── Right: chat area ──────────────────────────────────────────────────
        right = tk.Frame(self.root, bg=self.BG)
        right.pack(side="right", fill="both", expand=True)

        # Header bar
        self.header = tk.Label(right, text="", font=("Courier", 12, "bold"),
                                bg=self.PANEL, fg=self.ACCENT,
                                anchor="w", padx=16, pady=10)
        self.header.pack(fill="x")
        tk.Frame(right, bg=self.BORDER, height=1).pack(fill="x")

        # Message display
        self.msg_area = scrolledtext.ScrolledText(
            right, bg=self.BG, fg=self.FG, font=self.FONT_MSG,
            relief="flat", bd=0, state="disabled",
            wrap="word", padx=16, pady=12,
            insertbackground=self.FG,
        )
        self.msg_area.pack(fill="both", expand=True)
        # Tag colours
        self.msg_area.tag_config("self",   foreground=self.SELF_CLR)
        self.msg_area.tag_config("other",  foreground=self.OTHER_CLR)
        self.msg_area.tag_config("system", foreground=self.SYS_CLR, font=("Courier", 9))
        self.msg_area.tag_config("media",  foreground="#e8a020")
        self.msg_area.tag_config("p2p",    foreground="#7090ff")

        tk.Frame(right, bg=self.BORDER, height=1).pack(fill="x")

        # Input bar
        input_bar = tk.Frame(right, bg=self.PANEL, pady=8)
        input_bar.pack(fill="x", padx=0)

        self.input_var = tk.StringVar()
        self.input_entry = tk.Entry(
            input_bar, textvariable=self.input_var,
            font=self.FONT_MSG, bg="#1e1e1e", fg=self.FG,
            insertbackground=self.FG, relief="flat", bd=0,
        )
        self.input_entry.pack(side="left", fill="x", expand=True, padx=(12, 8), ipady=6)
        self.input_entry.bind("<Return>", lambda e: self._send_text())

        tk.Button(input_bar, text="Send", font=("Courier", 10, "bold"),
                  bg=self.ACCENT, fg="#000", relief="flat",
                  cursor="hand2", command=self._send_text,
                  padx=14, pady=6).pack(side="left", padx=(0, 4))

        tk.Button(input_bar, text="📎", font=("Courier", 12),
                  bg=self.PANEL, fg=self.FG, relief="flat",
                  cursor="hand2", command=self._send_media_dialog,
                  padx=8, pady=4).pack(side="left", padx=(0, 4))

        tk.Button(input_bar, text="P2P", font=("Courier", 10),
                  bg=self.PANEL, fg=self.FG, relief="flat",
                  cursor="hand2", command=self._send_p2p_dialog,
                  padx=8, pady=4).pack(side="left", padx=(0, 12))

    # ── Conversation management ───────────────────────────────────────────────

    def _open_chat(self, name: str):
        if name not in self.chat_logs:
            self.chat_logs[name] = []
            self.conv_list.insert("end", name)
        self.current_chat = name
        # Highlight in list
        items = list(self.conv_list.get(0, "end"))
        if name in items:
            self.conv_list.selection_clear(0, "end")
            self.conv_list.selection_set(items.index(name))
        self.header.config(text=f"  {name}")
        self._redraw_messages()

    def _on_conv_select(self, _event):
        sel = self.conv_list.curselection()
        if sel:
            self._open_chat(self.conv_list.get(sel[0]))

    def _redraw_messages(self):
        self.msg_area.config(state="normal")
        self.msg_area.delete("1.0", "end")
        for tag, line in self.chat_logs.get(self.current_chat, []):
            self.msg_area.insert("end", line + "\n", tag)
        self.msg_area.config(state="disabled")
        self.msg_area.see("end")

    def _append(self, chat: str, tag: str, line: str):
        """Add a line to a chat log and refresh if it's the active chat."""
        if chat not in self.chat_logs:
            self.chat_logs[chat] = []
            self.conv_list.insert("end", chat)
        self.chat_logs[chat].append((tag, line))
        if self.current_chat == chat:
            self.msg_area.config(state="normal")
            self.msg_area.insert("end", line + "\n", tag)
            self.msg_area.config(state="disabled")
            self.msg_area.see("end")

    # ── Event handler (called from backend via thread-safe emit) ─────────────

    def _on_event(self, event: dict):
        # Must run on the Tkinter main thread
        self.root.after(0, lambda: self._process_event(event))

    def _process_event(self, event: dict):
        t = event.get("type")

        if t == "message":
            sender   = event["from"]
            group    = event.get("group")
            text     = event["text"]
            chat_key = group if group else sender
            tag      = "other"
            prefix   = f"[{group}] " if group else ""
            self._open_chat(chat_key)
            self._append(chat_key, tag, f"{prefix}{sender}: {text}")

        elif t == "media":
            sender   = event["from"]
            group    = event.get("group")
            filename = event["filename"]
            ct       = event.get("content_type", "file")
            chat_key = group if group else sender
            self._open_chat(chat_key)
            self._append(chat_key, "media",
                         f"📎 {sender} sent {ct} → saved as {filename}")

        elif t == "p2p_received":
            self._append("[server]", "p2p",
                         f"⇣ P2P file received → {event['filename']}")

        elif t == "server":
            self._append("[server]", "system", f"  {event['text']}")

    # ── Send actions ──────────────────────────────────────────────────────────

    def _send_text(self):
        text = self.input_var.get().strip()
        if not text or not self.current_chat:
            return
        if self.current_chat == "[server]":
            self._append("[server]", "system", "  (open a DM or group to send messages)")
            self.input_var.set("")
            return

        target = self.current_chat
        # Detect if target is a group (starts with '#') or user
        if target.startswith("#"):
            self.backend.send_group(target, text)
        else:
            self.backend.send_dm(target, text)

        self._append(target, "self", f"you: {text}")
        self.input_var.set("")

    def _send_media_dialog(self):
        if not self.current_chat or self.current_chat == "[server]":
            messagebox.showinfo("Info", "Open a DM or group chat first.")
            return
        path = filedialog.askopenfilename(
            title="Send media file",
            filetypes=[("Media files", "*.jpg *.jpeg *.png *.gif *.mp3 *.wav *.mp4 *.ogg"),
                       ("All files", "*.*")]
        )
        if not path:
            return
        self.backend.send_media(self.current_chat, path)
        self._append(self.current_chat, "media",
                     f"📎 you sent {os.path.basename(path)}")

    def _send_p2p_dialog(self):
        if not self.current_chat or self.current_chat == "[server]":
            messagebox.showinfo("Info", "Open a DM first.")
            return
        if not self.p2p_active:
            messagebox.showinfo("P2P", "Start P2P listener first (◎ P2P Listen button).")
            return
        path = filedialog.askopenfilename(title="Send file via P2P")
        if not path:
            return
        self.backend.send_p2p_request(self.current_chat, path)
        self._append(self.current_chat, "p2p",
                     f"⇡ P2P sending {os.path.basename(path)} → {self.current_chat}...")

    # ── Sidebar button actions ────────────────────────────────────────────────

    def _prompt_dm(self):
        self._simple_dialog("New DM", "Username to message:", self._open_chat)

    def _prompt_group(self):
        def handle(name):
            if not name.startswith("#"):
                name = "#" + name
            # Ask create or join
            choice = messagebox.askyesno("Group", f"Create new group '{name}'?\n(No = join existing)")
            if choice:
                self.backend.create_group(name)
            else:
                self.backend.join_group(name)
            self._open_chat(name)
        self._simple_dialog("Group", "Group name (# added automatically):", handle)

    def _do_list_users(self):
        self._open_chat("[server]")
        self.backend.list_users()

    def _do_p2p_listen(self):
        if self.p2p_active:
            self._append("[server]", "system", "  P2P listener already running.")
            return
        port = self.backend.start_p2p_listener()
        self.p2p_active = True
        self._append("[server]", "p2p", f"⇣ P2P listener started on port {port}")

    def _do_ping(self):
        self._open_chat("[server]")
        self.backend.ping()

    def _simple_dialog(self, title: str, prompt: str, callback):
        """A minimal input popup."""
        win = tk.Toplevel(self.root)
        win.title(title)
        win.configure(bg=self.BG)
        win.resizable(False, False)
        win.geometry("320x130")
        win.grab_set()

        tk.Label(win, text=prompt, font=self.FONT_MONO,
                 bg=self.BG, fg=self.FG).pack(pady=(16, 6), padx=20, anchor="w")
        var = tk.StringVar()
        e = tk.Entry(win, textvariable=var, font=self.FONT_MSG,
                     bg="#1e1e1e", fg=self.FG, insertbackground=self.FG,
                     relief="flat", bd=4)
        e.pack(fill="x", padx=20)
        e.focus()

        def submit():
            val = var.get().strip()
            if val:
                win.destroy()
                callback(val)

        e.bind("<Return>", lambda _: submit())
        tk.Button(win, text="OK", font=("Courier", 10, "bold"),
                  bg=self.ACCENT, fg="#000", relief="flat",
                  command=submit, padx=16, pady=4).pack(pady=12)

    # ── Close ─────────────────────────────────────────────────────────────────

    def _on_close(self):
        try:
            self.backend.disconnect()
        except Exception:
            pass
        self.root.destroy()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    # Login screen
    login_root = tk.Tk()
    login = LoginWindow(login_root)
    login_root.mainloop()

    if not login.result:
        return   # user closed the window

    host, port, username = login.result

    # Main chat window
    chat_root = tk.Tk()
    ChatWindow(chat_root, host, port, username)
    chat_root.mainloop()


if __name__ == "__main__":
    main()