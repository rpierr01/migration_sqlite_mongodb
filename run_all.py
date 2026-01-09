import subprocess
import sys
import time
import signal
import os

processes = []

def signal_handler(sig, frame):
    print("\n🛑 Arrêt des serveurs...")
    for p in processes:
        p.terminate()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

if __name__ == "__main__":
    print("🚀 Démarrage de l'application principale (port 8050)...")
    main_app = subprocess.Popen([sys.executable, "main.py"])
    processes.append(main_app)
    
    time.sleep(2)
    
    print("📊 Démarrage du dashboard (port 8051)...")
    dashboard_app = subprocess.Popen([sys.executable, "dashboard/dashboard.py"])
    processes.append(dashboard_app)
    
    print("\n✅ Applications démarrées !")
    print("   - Application principale : http://127.0.0.1:8050")
    print("   - Dashboard              : http://127.0.0.1:8051")
    print("\nAppuyez sur Ctrl+C pour arrêter les serveurs.\n")
    
    try:
        main_app.wait()
        dashboard_app.wait()
    except KeyboardInterrupt:
        signal_handler(None, None)
