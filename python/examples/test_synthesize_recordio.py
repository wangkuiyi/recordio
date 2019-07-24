#!/usr/bin/env python

from contextlib import closing
import recordio
import argparse
import os.path
from .image_label import synthesize


def test_synthesize_data():
    if not os.path.exists("/tmp/mnist"):
        synthesize(argparse.Namespace(
            dir='/tmp',
            records_per_shard=16*1024,
            dataset="mnist",
            fraction=1.0))

    tasks = [
        ("/tmp/mnist/train/data-00000", 100, 256, 256),
        ("/tmp/mnist/train/data-00001", 1024, 2048, 2048),
        ("/tmp/mnist/train/data-00002", 0, -1, 16384),
    ]

    for t in tasks:
        counts = 0
        with closing(recordio.Scanner(t[0], t[1], t[2])) as reader:
            r = reader.record()
            while r:
                counts += 1
                r = reader.record()
            assert counts == t[3]
