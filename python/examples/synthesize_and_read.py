#!/usr/bin/env python


from contextlib import closing
import argparse
import os
import recordio
import recordio
import sys
import tempfile
import tensorflow as tf


def convert(x, y, args, subdir):
    """Convert pairs of image and label in NumPy arrays into a set of
    RecordIO files.
    """
    row = 0
    shard = 0
    w = None
    while row < x.shape[0] * args.fraction:
        if row % args.records_per_shard == 0:
            if w:
                w.close()
            dn = os.path.join(args.dir, args.dataset, subdir)
            fn = os.path.join(dn, "data-%05d" % (shard))
            if not os.path.exists(dn):
                os.makedirs(os.path.dirname(fn))
            print("Writing {} ...".format(fn))
            w = recordio.Writer(fn)
            shard = shard + 1

        w.write(
            tf.train.Example(
                features=tf.train.Features(
                    feature={
                        "image": tf.train.Feature(
                            float_list=tf.train.FloatList(
                                value=x[row].flatten()
                            )
                        ),
                        "label": tf.train.Feature(
                            int64_list=tf.train.Int64List(
                                value=y[row].flatten()
                            )
                        ),
                    }
                )
            ).SerializeToString()
        )
        row = row + 1
    w.close()
    print(
        "Wrote {} of total {} records into {} files".format(
            row, x.shape[0], shard
        )
    )


def synthesize(args):
    if args.dataset == "mnist":
        from tensorflow.python.keras.datasets import mnist

        (x_train, y_train), (x_test, y_test) = mnist.load_data()
    elif args.dataset == "fashion_mnist":
        from tensorflow.python.keras.datasets import fashion_mnist

        (x_train, y_train), (x_test, y_test) = fashion_mnist.load_data()
    elif args.dataset == "cifar10":
        from tensorflow.python.keras.datasets import cifar10

        (x_train, y_train), (x_test, y_test) = cifar10.load_data()
    else:
        sys.exit("Unknown dataset {}".format(args.dataset))

    convert(x_train, y_train, args, "train")
    convert(x_test, y_test, args, "test")


def synthesize_and_read_data():
    if not os.path.exists(os.path.join(os.getcwd(), "mnsit")):
        synthesize(argparse.Namespace(
            dir=os.getcwd(),
            records_per_shard=16*1024,
            dataset="mnist",
            fraction=1.0))

    tasks = [
        (os.path.join(os.getcwd(), "mnist/train/data-00000"), 100, 256, 256),
        (os.path.join(os.getcwd(), "mnist/train/data-00001"), 1024, 2048, 2048),
        (os.path.join(os.getcwd(), "mnist/train/data-00002"), 0, -1, 16384),
    ]

    for t in tasks:
        counts = 0
        with closing(recordio.Scanner(t[0], t[1], t[2])) as reader:
            r = reader.record()
            while r:
                counts += 1
                r = reader.record()
                print(counts, t[3])
                assert counts == t[3]


if __name__ == "__main__":
    synthesize_and_read_data()
