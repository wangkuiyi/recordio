import unittest

import tempfile
import os.path
import random
import recordio


class TestAll(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp_dir = tempfile.TemporaryDirectory()

    @classmethod
    def tearDownClass(cls):
        cls.tmp_dir.cleanup()

    def test_write_read(self):
        path = os.path.join(self.tmp_dir.name, "1.record")
        w = recordio.Writer(path)
        w.write(b"1")
        w.write(b"2")
        w.write(b"")
        w.close()

        r = recordio.Scanner(path)
        self.assertEqual(r.record(), b"1")
        self.assertEqual(r.record(), b"2")
        self.assertEqual(r.record(), b"")
        self.assertEqual(r.record(), None)
        self.assertEqual(r.record(), None)
        r.close()

        r = recordio.Scanner(path, 1, 1)
        self.assertEqual(r.record(), b"2")
        self.assertEqual(r.record(), None)
        self.assertEqual(r.record(), None)
        r.close()

    def test_big_write_read(self):
        gen_rec = lambda: bytes(
            bytearray(random.getrandbits(8) for _ in range(4 * 1024 * 1024))
        )
        # 10 records, each has size 4Mi bytes.
        records = [gen_rec() for _ in range(10)]

        path = os.path.join(self.tmp_dir.name, "big.recordio")
        w = recordio.Writer(path)
        for r in records:
            w.write(r)
        w.close()

        s = recordio.Scanner(path)
        for r in records:
            self.assertEqual(r, s.record())
        s.close()

    def test_index(self):
        path = os.path.join(self.tmp_dir.name, "1.record")
        w = recordio.Writer(path)
        w.write(b"1")
        w.write(b"2")
        w.write(b"")
        w.close()

        idx = recordio.Index(path)
        self.assertEqual(3, idx.num_records())

        r = recordio.Scanner(path, index=idx)
        self.assertEqual(r.record(), b"1")
        self.assertEqual(r.record(), b"2")
        self.assertEqual(r.record(), b"")
        self.assertEqual(r.record(), None)
        self.assertEqual(r.record(), None)
        r.close()

        r = recordio.Scanner(path, 1, 1, index=idx)
        self.assertEqual(r.record(), b"2")
        self.assertEqual(r.record(), None)
        self.assertEqual(r.record(), None)
        r.close()
        idx.close()

    def test_io_failures(self):
        # Writer open error.
        with self.assertRaises(RuntimeError):
            w = recordio.Writer("/")

        # Index open error.
        with self.assertRaises(RuntimeError):
            idx = recordio.Index("/")

        with self.assertRaises(RuntimeError):
            s = recordio.Scanner("/")

        # Scanner open error. Here we pass in an index. so the opening of
        # recordio file is tested.
        path = os.path.join(self.tmp_dir.name, "1.record")
        w = recordio.Writer(path)
        w.write(b"1")
        w = recordio.Writer(path)
        w.close()

        idx = recordio.Index(path)
        with self.assertRaises(RuntimeError):
            recordio.Scanner("", index=idx)
        idx.close()

    def test_utf8_values(self):
        # filename can be in UTF-8
        path = os.path.join(self.tmp_dir.name, "ファイル.recordio")
        w = recordio.Writer(path)
        # UTF-8 characters need to be encoded explicitly.
        w.write("你好世界".encode())
        w.write("שלום בעולם".encode())
        # ASCII characters don't need encoding.
        w.write(b"Hello world")

        # Non-encoded string will be rejected.
        with self.assertRaises(ValueError):
            w.write("你好世界")
        with self.assertRaises(ValueError):
            w.write("שלום בעולם")
        with self.assertRaises(ValueError):
            w.write("Hello world")

        w.close()

        idx = recordio.Index(path)
        self.assertEqual(3, idx.num_records())

        r = recordio.Scanner(path, index=idx)
        self.assertEqual(r.record().decode(), "你好世界")
        self.assertEqual(r.record().decode(), "שלום בעולם")
        self.assertEqual(r.record().decode(), "Hello world")
        self.assertEqual(r.record(), None)
        r.close()
        idx.close()


if __name__ == "__main__":
    unittest.main()
