import unittest

import recordio

class TestAll(unittest.TestCase):
    def test_write_read(self):
        path = "/tmp/1.record"
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

    def test_index(self):
        path = "/tmp/1.record"
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

if __name__ == '__main__':
    unittest.main()
