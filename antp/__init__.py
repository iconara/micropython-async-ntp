import asyncio
import machine
import select
import socket
import struct
import time

from micropython import const

host = 'time.aws.com'

MIN_NTP_TIMESTAMP = const(3913056000)

async def _connect() -> socket.socket:
    addr = socket.getaddrinfo(host, 123)[0][-1]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.connect(addr)
    s.setblocking(False)
    return s

async def _send_query(s: socket.socket, poller: select.poll, query: bytearray) -> None:
    poller.register(s, select.POLLOUT)
    sent = False
    while not sent:
        for event in poller.ipoll():
            if event[1] == select.POLLOUT:
                s.write(query)
                sent = True
            else:
                raise Exception('Socket error while sending NTP query')
        if not sent:
            await asyncio.sleep_ms(10)
    poller.unregister(s)

async def _await_response(s: socket.socket, poller: select.poll, buffer: bytearray) -> None:
    poller.register(s, select.POLLIN)
    received = False
    while not received:
        for event in poller.ipoll():
            if event[1] == select.POLLIN:
                n = s.readinto(buffer)
                if n == 48:
                    received = True
                else:
                    raise Exception(f'Invalid NTP response length: {n} (expected 48)')
            else:
                raise Exception('Socket error while reading response to NTP query')
        if not received:
            await asyncio.sleep_ms(10)
    poller.unregister(s)

def _decode(buffer: bytearray) -> int:
    val = struct.unpack('!I', buffer[40:44])[0]
    if val < MIN_NTP_TIMESTAMP:
        val += 0x100000000
    epoch_year = time.gmtime(0)[0]
    if epoch_year == 2000:
        return val - 3155673600
    elif epoch_year == 1970:
        return val - 2208988800
    else:
        raise Exception(f'Unsupported epoch: {epoch_year}')

async def _get_time():
    buffer = bytearray(48)
    buffer[0] = 0x1B
    poller = select.poll()
    s = await _connect()
    try:
        await _send_query(s, poller, buffer)
        await _await_response(s, poller, buffer)
    finally:
        s.close()
    return _decode(buffer)

async def sync() -> None:
    t = await _get_time()
    tm = time.gmtime(t)
    machine.RTC().datetime((tm[0], tm[1], tm[2], tm[6] + 1, tm[3], tm[4], tm[5], 0))
