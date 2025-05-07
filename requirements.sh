#!/usr/bin/env bash

python3 -m venv .venv
source .venv/bin/activate
brew install uv
uv pip install -r requirements.txt
