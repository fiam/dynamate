#!/bin/sh
set -eu

case "${DYNAMATE_USE_TTYD:-}" in
  1|true|TRUE|yes|YES|on|ON)
    port="${TTYD_PORT:-7681}"
    if [ -n "${DYNAMATE_TABLE:-}" ]; then
      table_arg_present=0
      for arg in "$@"; do
        case "$arg" in
          --table|--table=*|-t|-t*)
            table_arg_present=1
            break
            ;;
        esac
      done
      if [ "$table_arg_present" -eq 0 ]; then
        set -- "$@" --table "$DYNAMATE_TABLE"
      fi
    fi
    exec ttyd -p "$port" -W -- dynamate "$@"
    ;;
esac

exec dynamate "$@"
