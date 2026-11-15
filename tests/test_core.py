"""Tests for shard_aware_log_tail_follower.core.

These tests drive the follower with a fake clock and real temporary files.
No sleeping, no wall-clock assertions, no network.
"""

import os
import tempfile
import unittest

from shard_aware_log_tail_follower import (
    FileShard,
    LogEvent,
    ShardAwareLogTailFollower,
    follow,
)


class TempDirCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def path(self, name):
        return os.path.join(self.dir, name)

    def write(self, name, text, mode="a"):
        p = self.path(name)
        with open(p, mode, encoding="utf-8") as fp:
            fp.write(text)
        return p


class TestBasicReading(TempDirCase):
    def test_reads_appended_lines(self):
        self.write("a.log", "one\ntwo\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        events = f.poll()
        self.assertEqual([e.line for e in events], ["one", "two"])
        self.assertEqual([e.offset for e in events], [0, 4])
        f.close()

    def test_second_poll_only_returns_new_lines(self):
        self.write("a.log", "one\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        first = f.poll()
        self.assertEqual([e.line for e in first], ["one"])
        self.write("a.log", "two\n")
        second = f.poll()
        self.assertEqual([e.line for e in second], ["two"])
        f.close()

    def test_partial_line_is_held_until_complete(self):
        self.write("a.log", "hel")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        self.assertEqual(f.poll(), [])
        self.write("a.log", "lo\n")
        events = f.poll()
        self.assertEqual([e.line for e in events], ["hello"])
        f.close()

    def test_crlf_is_stripped(self):
        self.write("a.log", "one\r\ntwo\r\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        events = f.poll()
        self.assertEqual([e.line for e in events], ["one", "two"])
        f.close()

    def test_invalid_utf8_is_replaced_not_raised(self):
        p = self.path("a.log")
        with open(p, "wb") as fp:
            fp.write(b"ok\n\xff\xfe\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        events = f.poll()
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].line, "ok")
        self.assertIn("\ufffd", events[1].line)
        f.close()


class TestRotation(TempDirCase):
    def test_rotation_by_rename_is_followed(self):
        p = self.write("a.log", "first\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        self.assertEqual([e.line for e in f.poll()], ["first"])
        # Rotate: rename current file, create a new one at the same path.
        os.rename(p, self.path("a.log.1"))
        self.write("a.log", "second\n")
        events = f.poll()
        lines = [e.line for e in events]
        self.assertIn("second", lines)
        f.close()

    def test_rotation_does_not_replay_old_bytes(self):
        p = self.write("a.log", "first\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        f.poll()
        os.rename(p, self.path("a.log.1"))
        self.write("a.log", "second\n")
        events = f.poll()
        self.assertEqual([e.line for e in events], ["second"])
        f.close()

    def test_truncation_restarts_from_zero(self):
        p = self.write("a.log", "first line\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        f.poll()
        # Truncate in place: same inode, smaller size.
        with open(p, "w", encoding="utf-8") as fp:
            fp.write("new\n")
        events = f.poll()
        self.assertEqual([e.line for e in events], ["new"])
        f.close()


class TestMerging(TempDirCase):
    def test_events_are_merged_by_timestamp(self):
        import re

        def parse(line):
            m = re.match(r"t=(\d+) ", line)
            return float(m.group(1)) if m else None

        self.write("a.log", "t=5 a\n")
        self.write("b.log", "t=1 b\n")
        f = ShardAwareLogTailFollower(self.path("*.log"), timestamp_parser=parse)
        events = f.poll()
        self.assertEqual([e.line for e in events], ["t=1 b", "t=5 a"])
        f.close()

    def test_events_without_timestamps_sort_first(self):
        self.write("a.log", "t=5 a\n")
        self.write("b.log", "no timestamp\n")

        def parse(line):
            if line.startswith("t="):
                return float(line.split()[0][2:])
            return None

        f = ShardAwareLogTailFollower(self.path("*.log"), timestamp_parser=parse)
        events = f.poll()
        self.assertEqual(events[0].line, "no timestamp")
        self.assertEqual(events[1].line, "t=5 a")
        f.close()

    def test_ties_broken_deterministically_by_shard(self):
        self.write("a.log", "x\n")
        self.write("b.log", "x\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        events = f.poll()
        # Both have no timestamp; order is by (device, inode, offset).
        self.assertEqual(len(events), 2)
        keys = [(e.device, e.inode, e.offset) for e in events]
        self.assertEqual(keys, sorted(keys))
        f.close()


class TestShardTracking(TempDirCase):
    def test_shards_are_tracked_by_inode(self):
        self.write("a.log", "one\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        f.poll()
        shards = f.shards()
        self.assertEqual(len(shards), 1)
        self.assertIsInstance(shards[0], FileShard)
        self.assertEqual(shards[0].inode, os.stat(self.path("a.log")).st_ino)
        f.close()

    def test_new_file_matching_pattern_is_picked_up(self):
        self.write("a.log", "one\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        f.poll()
        self.write("b.log", "two\n")
        events = f.poll()
        self.assertEqual([e.line for e in events], ["two"])
        f.close()


class TestLifecycle(TempDirCase):
    def test_close_is_idempotent_and_poll_after_close_raises(self):
        self.write("a.log", "one\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        f.close()
        f.close()
        with self.assertRaises(RuntimeError):
            f.poll()

    def test_context_manager_closes(self):
        self.write("a.log", "one\n")
        with ShardAwareLogTailFollower(self.path("*.log")) as f:
            f.poll()
        with self.assertRaises(RuntimeError):
            f.poll()

    def test_log_event_shard_id_is_device_inode(self):
        self.write("a.log", "one\n")
        f = ShardAwareLogTailFollower(self.path("*.log"))
        ev = f.poll()[0]
        self.assertIsInstance(ev, LogEvent)
        st = os.stat(self.path("a.log"))
        self.assertEqual(ev.shard_id, (st.st_dev, st.st_ino))
        f.close()


class TestFollowGenerator(TempDirCase):
    def test_follow_yields_available_events_then_stops(self):
        self.write("a.log", "one\ntwo\n")
        got = list(follow(self.path("*.log")))
        self.assertEqual([e.line for e in got], ["one", "two"])


if __name__ == "__main__":
    unittest.main()
