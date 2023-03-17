#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
cd "$BASEDIR"

echo -n -e "\033]0;Client $1\007"

if [[ $2 -eq 0 ]]; then
    echo "Starting client $1..."
elif [[ $2 -eq 1 ]]; then
    echo "Reviving client $1..."
fi

python3 client.py $1 $2