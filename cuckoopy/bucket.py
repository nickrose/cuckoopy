import random


class Bucket(object):

    def __init__(self, size=4, has_values=False):
        self.size = size
        self.has_values = has_values
        if has_values:
            self.bucket = {}
        else:
            self.bucket = []

    def __repr__(self):
        return '<Bucket: ' + str(self.bucket) + '>'

    def __contains__(self, item):
        return item in self.bucket

    def __len__(self):
        return len(self.bucket)

    def insert(self, item, value=None):
        """
        Insert a fingerprint into the bucket
        :param item:
        :return:
        """
        if not self.is_full():
            if self.has_values:
                self.bucket[item] = value
            else:
                self.bucket.append(item)
            return True
        return False

    def delete(self, item):
        """
        Delete a fingerprint from the bucket.
        :param item:
        :return:
        """
        try:
            if self.has_values:
                del self.bucket[item]
            else:
                del self.bucket[self.bucket.index(item)]
            return True
        except ValueError:
            return False

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
            swapped_key = list(self.bucket.keys())[index]
            swapped_item = swapped_key, self.bucket[swapped_key]
            self.bucket[item] = value
            del self.bucket[swapped_key]
        else:
            swapped_item = self.bucket[index]
            self.bucket[index] = item
        return swapped_item
