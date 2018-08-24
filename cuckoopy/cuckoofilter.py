"""
Cuckoo Filter
"""

import random
import numpy as np
from . import bucket
from . import exceptions
from . import hashutils


class CuckooFilter(object):
    """
    Cuckoo Filter class.

    Implements insert, delete and contains operations for the filter.
    """

    def __init__(self, capacity, bucket_size=4, fingerprint_size=1,
                 max_displacements=500, dtype=str, has_values=False,
                 has_unique_values=False, debug=0):
        """
        Initialize CuckooFilter object.

        :param capacity: Size of the Cuckoo Filter
        :param bucket_size: Number of entries in a bucket
        :param fingerprint_size: Fingerprint size in bytes
        :param max_displacements: Maximum number of evictions before filter is
        considered full
        """
        assert dtype in hashutils.acceptable_dtypes, (
            f'dtype input must be one of {hashutils.acceptable_dtypes}')
        self.debug = debug
        self.dtype = dtype
        self.capacity = capacity
        self.bucket_size = bucket_size
        self.fingerprint_size = fingerprint_size
        self.max_displacements = max_displacements
        self.has_values = has_values
        self.has_unique_values = has_unique_values
        self.buckets = [bucket.Bucket(
            size=bucket_size, has_values=has_values,
            has_unique_values=has_unique_values)
            for _ in range(self.capacity)]
        self.size = 0

    def __repr__(self):
        return '<CuckooFilter: capacity=' + str(self.capacity) + \
               ', size=' + str(self.size) + ', fingerprint size=' + \
               str(self.fingerprint_size) + ' byte(s), values stored: ' + \
               str(self.has_values) + '>'

    def __len__(self):
        return self.size

    def __contains__(self, item):
        return self.contains(item)

    def __setitem__(self, key, value):
        assert (self.has_values or self.has_unique_values), (
            "dict-like __setitem__() cannot be called unless has_values or "
            "has_unique_values is True")
        return self.insert(key, value)

    def __getitem__(self, item):
        return self.getitem(item)

    def __delete__(self, item):
        return self.delete(item)

    def _get_index(self, item):
        index = hashutils.hash_code(item) % self.capacity
        return index

    def _get_alternate_index(self, index, fingerprint):
        alt_index = (index ^ hashutils.hash_code(fingerprint)) % self.capacity
        return alt_index

    def insert(self, item, value=None):
        """
        Insert an item into the filter.

        :param item: Item to be inserted.
        :return: True if insert is successful; CuckooFilterFullException if
        filter is full.
        """
        fingerprint = hashutils.fingerprint(item, self.fingerprint_size)
        i = self._get_index(item)
        j = self._get_alternate_index(i, fingerprint)

        if self.has_values or self.has_unique_values:
            if self.buckets[i].insert(fingerprint, value) \
                    or self.buckets[j].insert(fingerprint, value):
                self.size += 1
                return True

            eviction_index = random.choice([i, j])
            for _ in range(self.max_displacements):
                f, v = self.buckets[eviction_index].swap(fingerprint, value)
                eviction_index = self._get_alternate_index(eviction_index, f)
                if self.buckets[eviction_index].insert(f, v):
                    self.size += 1
                    return True
        else:
            if self.buckets[i].insert(fingerprint) \
                    or self.buckets[j].insert(fingerprint):
                self.size += 1
                return True
            eviction_index = random.choice([i, j])
            for _ in range(self.max_displacements):
                f = self.buckets[eviction_index].swap(fingerprint)
                eviction_index = self._get_alternate_index(eviction_index, f)
                if self.buckets[eviction_index].insert(f):
                    self.size += 1
                    return True

        # Filter is full
        raise exceptions.CuckooFilterFullException('Insert operation failed. '
                                                   'Filter is full.')

    def contains(self, item):
        """
        Check if the filter contains the item.

        :param item: Item to check its presence in the filter.
        :return: True, if item is in the filter; False, otherwise.
        """
        fingerprint = hashutils.fingerprint(item, size=self.fingerprint_size)
        i = self._get_index(item)
        j = self._get_alternate_index(i, fingerprint)

        return (fingerprint in self.buckets[i]) or (
            fingerprint in self.buckets[j])

    def getitem(self, item):
        """ return the item specified by the input (un-hashed) item id """
        if not(self.has_values or self.has_unique_values):
            return None
        fingerprint = hashutils.fingerprint(item, size=self.fingerprint_size)

        i = self._get_index(item)
        j = self._get_alternate_index(i, fingerprint)
        if self.debug:
            print(f'get() item: {item}, fingerprint: {fingerprint}, '
                  f'bucket[i]: {i}, bucket[j]: {j}')
        values = self.buckets[i][fingerprint]
        if len(values):
            return values
        else:
            return self.buckets[j][fingerprint]

    def delete(self, item):
        """
        Delete an item from the filter.

        To delete an item safely, it must have been previously inserted.
        Otherwise, deleting a non-inserted item might unintentionally remove
        a real, different item that happens to share the same fingerprint.

        :param item: Item to delete from the filter.
        :return: True, if item is found and deleted; False, otherwise.
        """
        fingerprint = hashutils.fingerprint(item, size=self.fingerprint_size)
        i = self._get_index(item)
        if self.buckets[i].delete(fingerprint):
            self.size -= 1
            return True
        else:
            j = self._get_alternate_index(i, fingerprint)
            if self.buckets[j].delete(fingerprint):
                self.size -= 1
                return True
        return False
