# code lazily transferred from varint_bench.c
# warning, here be dragons

def encode_varint(x):
    negative = x < 0
    mask = 0xFFFFFFFFFFFFFFFF
    if negative:
        x = ~x
    mask <<= 6
    n_bytes = 1
    while (x & mask) != 0:
        mask <<= 7
        n_bytes += 1
    
    k = n_bytes - 1

    o = bytearray(n_bytes)

    if n_bytes == 9:
        o[k] = x & 0xff
        x >>= 8
        k -= 1

    while k > 0:
        byte = x & 0x7F
        x >>= 7
        if k < n_bytes - 1:
            byte |= 0x80
        o[k] = byte
        k -= 1

    byte = x & 0x3F
    if n_bytes > 1:
        byte |= 0x80
    if negative:
        byte |= 0x40
    o[0] = byte

    return o
