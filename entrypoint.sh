#!/bin/bash

echo "starting watcher"
python main.py

# Use 'exec "$@"' to replace the shell with the command specified in CMD or passed as arguments.
# This ensures that the container runs the specified command as the main process.
# It also allows signals to be properly forwarded to the command, handling termination and
# other signals gracefully.
exec "$@"
