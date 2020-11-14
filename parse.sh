#!/usr/bin/env sh

if [[ $(docker images -q oceans-1876-parser | wc -c) -eq 0 ]]; then
  docker build . -t oceans-1876-parser
fi

docker run --rm -it -v "$PWD":/mnt oceans-1876-parser sh -c "cd /mnt && python3 main.py $@"
