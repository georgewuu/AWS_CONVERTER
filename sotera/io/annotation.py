import six
import collections
from intervaltree import Interval, IntervalTree


def apply_delays(atree, on_delay, off_delay, tags):
    new_tree = IntervalTree()
    for iv in sorted(atree):
        if new_tree.overlaps(iv.begin, iv.end):
            new_tree.addi(iv.begin, iv.end + off_delay, tags)
            new_tree.merge_overlaps(data_reducer=lambda x, y: tags)
        elif iv.length() > on_delay:
            new_tree.addi(iv.begin + on_delay, iv.end + off_delay, tags)
    return new_tree
