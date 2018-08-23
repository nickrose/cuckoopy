"""
Hash utilities for cuckoo filters to generate fingerprints.

Generate FNV64 hash based on http://isthe.com/chongo/tech/comp/fnv/
"""
import numpy as np
import xxhash
from tqdm import tqdm

FNV64_OFFSET_BASIS = 0xcbf29ce484222325
FNV64_PRIME = 0x100000001b3
MAX_64_INT = 2 ** 64
MAX_32_INT = 2 ** 32
acceptable_dtypes = tuple(
    [bytes, str, int, bool, float, tuple, np.integer, np.ndarray])


def _fnv64(data):
    """
    Generate FNV64 hash for data in bytes

    :param data: Data to generate FNV hash for
    """
    assert isinstance(data, acceptable_dtypes), (
        f'data type of input [{type(data)}] must be one of {acceptable_dtypes}'
        )

    if isinstance(data, str):
        # print('used .encode()')
        encoded = data.encode()
    elif type(data).__module__ == np.__name__:
        # print('used .tobytes()')
        encoded = data.data.tobytes()
    else:
        # print('used bytes( * )')
        encoded = bytes(data)
    # print(f'input was {type(data)}: encoded bytes len = {len(encoded)}')

    h = FNV64_OFFSET_BASIS
    for byte in encoded:
        h = (h * FNV64_PRIME) % MAX_64_INT
        h ^= byte
    return abs(h)


def _int_to_bytes(x):
    return x.to_bytes((x.bit_length() + 7) // 8, byteorder='big')


def _bytes_to_int(x):
    return int.from_bytes(x, byteorder='big')


def fingerprint(data, size):
    """
    Get fingerprint of a string using FNV 64-bit hash and truncate it to
    'size' bytes.

    :param data: Data to get fingerprint for
    :param size: Size in bytes to truncate the fingerprint
    :return: fingerprint of 'size' bytes
    """
    fp = _int_to_bytes(_fnv64(data))
    return _bytes_to_int(fp[:size])


def hash_code(data, dtype=str):
    """Generate hash code using builtin hash() function.

    :param data: Data to generate hash code for
    """
    # h = 0
    # for c in data:
    #     h = (ord(c) + (31 * h)) % MAX_32_INT
    # return h
    if not(isinstance(data, np.ndarray)):
        return abs(hash(data))
    else:
        return abs(hash(data.data.tobytes()))
    # return xxhash.xxh32(data).digest()
