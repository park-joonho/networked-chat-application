"""Launch the local chat server and GUI with: python main.py."""

from pathlib import Path
import socket
import subprocess
import sys
import time


PROJECT_DIR = Path(__file__).resolve().parent
SERVER_ADDRESS = ("127.0.0.1", 9000)
LOG_PATH = PROJECT_DIR / "server.log"


def server_is_ready():
    """Check whether the local TCP port accepts connections."""
    try:
        with socket.create_connection(SERVER_ADDRESS, timeout=0.25):
            return True
    except OSError:
        return False


def stop_server(process):
    """Stop only the server process owned by this launcher."""
    if process is not None and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def main():
    server_process = None
    try:
        if not server_is_ready():
            # Unbuffered output keeps startup failures visible in server.log.
            with LOG_PATH.open("a", encoding="utf-8") as log:
                server_process = subprocess.Popen(
                    [sys.executable, "-u", str(PROJECT_DIR / "server.py")],
                    cwd=PROJECT_DIR,
                    stdin=subprocess.DEVNULL,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    creationflags=(subprocess.CREATE_NO_WINDOW
                                   if sys.platform == "win32" else 0),
                )

            deadline = time.monotonic() + 10
            while True:
                if server_process.poll() is not None:
                    raise RuntimeError(f"Server exited during startup. See {LOG_PATH}")
                if server_is_ready():
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"Server was not ready within 10 seconds. See {LOG_PATH}")
                time.sleep(0.1)

        # Tkinter runs on the main thread. Its login defaults match the server.
        import GUI
        GUI.main()
    except Exception as exc:
        from tkinter import Tk, messagebox
        root = Tk()
        root.withdraw()
        try:
            messagebox.showerror("Chat launcher", str(exc), parent=root)
        finally:
            root.destroy()
        return 1
    finally:
        stop_server(server_process)
    return 0


if __name__ == "__main__":
    sys.exit(main())
