import json

# Iterate directory
import os

dir_path = './app/tgbot/messages'
messages = {}

for path in os.listdir(dir_path):
    # check if current path is a file

    with open(os.path.join(dir_path, path), encoding='utf-8') as f:
        messages.update(json.load(f))