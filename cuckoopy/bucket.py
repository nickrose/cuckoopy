import random
import warnings


class Bucket(object):

    def __init__(self, size=4, has_values=False, has_unique_values=False):
        self.size = size
        self.has_values = has_values
        self.has_unique_values = has_unique_values
        if has_values:
            self.unique = set()
            self.bucket = []
        elif has_unique_values:
            self.bucket = {}
        else:
            self.bucket = []

    def __repr__(self):
        return '<Bucket: ' + str(self.bucket) + '>'

    def __contains__(self, item):
        """
        Check if the bucket contains the item.

        :param item: Item to check its presence in the filter.
        :return: True, if item is in the filter; False, otherwise.
        """
        if self.has_values:
            return (item in self.unique)
        else:
            return (item in self.bucket)

    def __len__(self):
        return len(self.bucket)

    def insert(self, item, value=None):
        """
        Insert a fingerprint into the bucket
        :param item:
        :return:
        """
        if not self.is_full:
            if self.has_values:
                self.bucket.append((item, value))
                self.unique.add(item)
            elif self.has_unique_values:
                self.bucket[item] = value
            else:
                self.bucket.append(item)
            return True
        return False

    def __getitem__(self, item):
        """
        Get a value from a bucket via specified fingerprint
        :param item:
        :return value associated with item:
        """
        if not(self.has_values or self.has_unique_values):
            return None
        try:
            if self.has_values:
                item_paired = [it for it in self.bucket if item == it[0]]
                print(item in self.unique, item_paired)
                if len(item_paired) > 1:
                    print("NOTICE: found more than one value associated with "
                          "the specified fingerprint, returning multiple")
                    return [self.bucket[self.bucket.index(it_paired)][1]
                            for it_paired in item_paired]
                else:
                    item_paired = item_paired[0]
                    return [self.bucket[self.bucket.index(item_paired)][1]]
            else:
                return [self.bucket[item]]
        except (ValueError, IndexError):
            # warnings.warn('specified item does not exist in bucket')
            return []

    def delete(self, item):
        """
        Delete a fingerprint from the bucket.
        :param item:
        :return:
        """
        if self.has_values:
            self.unique.discard(item)
            try:
                item_paired = [it for it in self.bucket if item == it[0]][0]
                del self.bucket[self.bucket.index(item_paired)]
            except (ValueError, IndexError):
                return False
        elif self.has_unique_values:
            try:
                del self.bucket[item]
            except KeyError:
                return False
        else:
            try:
                del self.bucket[self.bucket.index(item)]
                return True
            except ValueError:
                return False

    @property
    def is_full(self):
        return len(self.bucket) == self.size

    def swap(self, item, value=None):
        """
        Swap fingerprint with a random entry stored in the bucket and return
        the swapped fingerprint
        :param item:
        :return:
        """
        index = random.choice(range(len(self.bucket)))
        if self.has_values:
            swapped_item = self.bucket[index]
            self.unique.discard(swapped_item[0])
            self.unique.add(item)
            self.bucket[index] = (item, value)
        elif self.has_unique_values:
            swapped_key = list(self.bucket.keys())[index]
            swapped_item = (swapped_key, self.bucket[swapped_key])
            del self.bucket[swapped_key]
            self.bucket[item] = value
        else:
            swapped_item = self.bucket[index]
            self.bucket[index] = item
        return swapped_item
