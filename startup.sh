#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
cd "$BASEDIR"

echo -n -e "\033]0;Client $1\007"
echo "Starting client $1..." 
python3 client.py $1