import sys

from server import main

if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "serve"
    if cmd == "serve":
        main()
    else:
        print("Comandos: serve")
