from __future__ import annotations

import socket


def force_ipv4_conninfo(database_url: str) -> str:
    """Return ``database_url`` with ``hostaddr`` pinned to a resolved IPv4 address.

    Supabase's direct Postgres endpoint is often IPv6-only while many egress
    environments are IPv4-only; resolving the host to an A record and passing it
    as ``hostaddr`` avoids connection timeouts. Returns the DSN unchanged when
    psycopg is unavailable, the DSN can't be parsed, the host already has
    ``hostaddr``, or the host has no IPv4 address. Never raises.
    """
    raw = str(database_url or "").strip()
    if not raw:
        return database_url
    try:
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
    except Exception:
        return database_url
    try:
        params = dict(conninfo_to_dict(raw))
    except Exception:
        return database_url
    host = str(params.get("host") or "").strip()
    if not host or str(params.get("hostaddr") or "").strip():
        return database_url
    try:
        port = int(params.get("port") or 5432)
    except Exception:
        port = 5432
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
    except Exception:
        return database_url
    ipv4 = ""
    for info in infos or []:
        sockaddr = info[4] if len(info) > 4 else None
        if sockaddr and sockaddr[0]:
            ipv4 = str(sockaddr[0])
            break
    if not ipv4:
        return database_url
    params["hostaddr"] = ipv4
    try:
        return make_conninfo(**params)
    except Exception:
        return database_url
