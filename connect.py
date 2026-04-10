# -*- coding: utf-8 -*-
"""
SSH 隧道連線腳本
透過 SSH 安全隧道存取遠端伺服器上的明星照片下載器 Web 版
其他區網電腦也可以透過本機的 port 存取
"""

import os
import sys
import time
import threading
import webbrowser
import socket
import socketserver
import select

try:
    import paramiko
except ImportError:
    print("需要安裝 paramiko: pip install paramiko")
    sys.exit(1)

# ── 設定（從環境變數讀取，或使用預設值） ──
SSH_HOST = os.environ.get("SSH_HOST", "YOUR_SERVER_IP")
SSH_PORT = int(os.environ.get("SSH_PORT", "22"))
SSH_USER = os.environ.get("SSH_USER", "YOUR_USERNAME")
SSH_PASS = os.environ.get("SSH_PASS", "YOUR_PASSWORD")
REMOTE_PORT = int(os.environ.get("REMOTE_PORT", "5000"))
LOCAL_PORT = int(os.environ.get("LOCAL_PORT", "5000"))
LISTEN_HOST = "0.0.0.0"  # 0.0.0.0 = 允許其他電腦連入


class TunnelHandler(socketserver.BaseRequestHandler):
    """轉發 TCP 流量到 SSH 隧道"""
    ssh_transport = None

    def handle(self):
        try:
            chan = self.ssh_transport.open_channel(
                "direct-tcpip",
                ("127.0.0.1", REMOTE_PORT),
                self.request.getpeername(),
            )
        except Exception as e:
            print(f"  隧道建立失敗: {e}")
            return

        while True:
            r, _, _ = select.select([self.request, chan], [], [], 5)
            if self.request in r:
                data = self.request.recv(8192)
                if not data:
                    break
                chan.sendall(data)
            if chan in r:
                data = chan.recv(8192)
                if not data:
                    break
                self.request.sendall(data)
        chan.close()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main():
    if SSH_HOST == "YOUR_SERVER_IP":
        print("請先設定連線資訊！")
        print("方法 1: 設定環境變數 SSH_HOST, SSH_USER, SSH_PASS")
        print("方法 2: 建立 .env 檔案（參考 .env.example）")
        input("按 Enter 結束...")
        return

    print("=" * 50)
    print("  明星照片下載器 — SSH 隧道連線")
    print("=" * 50)
    print()

    # 建立 SSH 連線
    print(f"連線到 {SSH_HOST}...")
    ssh = paramiko.SSHClient()
    # NOTE: AutoAddPolicy accepts any host key. For production use,
    # consider paramiko.RejectPolicy() with a known_hosts file.
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(SSH_HOST, SSH_PORT, SSH_USER, SSH_PASS, timeout=10)
    except Exception as e:
        print(f"SSH 連線失敗: {e}")
        input("按 Enter 結束...")
        return

    transport = ssh.get_transport()
    TunnelHandler.ssh_transport = transport
    print("SSH 連線成功!")
    print()

    # 啟動本地 TCP 轉發伺服器
    try:
        server = ThreadedTCPServer((LISTEN_HOST, LOCAL_PORT), TunnelHandler)
    except OSError as e:
        print(f"無法啟動本地伺服器 (port {LOCAL_PORT}): {e}")
        print("可能已有另一個程式佔用此 port")
        ssh.close()
        input("按 Enter 結束...")
        return

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    local_ip = socket.gethostbyname(socket.gethostname())
    print(f"  本機存取:   http://localhost:{LOCAL_PORT}")
    print(f"  區網存取:   http://{local_ip}:{LOCAL_PORT}")
    print()
    print("  其他電腦也可用上面的「區網存取」網址開啟！")
    print()
    print("  按 Ctrl+C 或關閉視窗結束")
    print("=" * 50)

    # 自動開啟瀏覽器
    time.sleep(1)
    webbrowser.open(f"http://localhost:{LOCAL_PORT}")

    try:
        while True:
            time.sleep(1)
            if not transport.is_active():
                print("\nSSH 連線中斷，重新連線中...")
                ssh.close()
                ssh.connect(SSH_HOST, SSH_PORT, SSH_USER, SSH_PASS, timeout=10)
                transport = ssh.get_transport()
                TunnelHandler.ssh_transport = transport
                print("重新連線成功!")
    except KeyboardInterrupt:
        print("\n正在關閉...")
    finally:
        server.shutdown()
        ssh.close()
        print("已關閉")


if __name__ == "__main__":
    main()
