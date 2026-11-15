"""Core implementation of the shard-aware log tail follower.

Design decisions worth calling out:

* A "shard" is a single file on disk. Rotation is detected by comparing the
  (st_dev, st_ino) pair of the path we are following against the pair we last
  saw. When the pair changes, the old file is drained to EOF and the new file
  starts from offset 0.

* We deliberately do NOT use ``os.stat`` on the path name only. The inode is
  what distinguishes "same file, more bytes" from "a new file reused the
  same name". This is the whole point of the library.

* Ordering across shards is by (timestamp, shard_id, offset). We do not try to
  invent a global ordering from file contents; the caller supplies a timestamp
  parser, and ties are broken deterministically by shard id and byte offset.

* The follower is a pure iterator. It never sleeps on its own; the caller
  drives it. This keeps tests deterministic and lets callers decide their own
  polling strategy.
"""

from __future__ import annotations

import glob as _glob
import os
from dataclasses import dataclass, field
from typing import Callable, Dict, Iterable, Iterator, List, Optional, Tuple


@dataclass
class LogEvent:
    """A single line read from a shard.

    Attributes:
        shard_path: The path the event was read from at read time. When a file
            rotates the path is reused; the inode is what identifies the shard.
        inode: The ``st_ino`` of the file at read time.
        device: The ``st_dev`` of the file at read time.
        offset: Byte offset in the file where the line started.
        line: The line, without the trailing newline.
        timestamp: The caller-supplied timestamp for ordering. ``None`` when
            the caller did not provide a parser.
    """

    shard_path: str
    inode: int
    device: int
    offset: int
    line: str
    timestamp: Optional[float] = None

    @property
    def shard_id(self) -> Tuple[int, int]:
        """Stable identifier for the shard, independent of path reuse."""
        return (self.device, self.inode)


@dataclass
class FileShard:
    """Bookkeeping for one file we are following.

    ``offset`` is the number of bytes consumed so far. ``partial`` holds bytes
    read past the last newline; they are kept until the rest of the line
    arrives, so a line split across two reads is emitted exactly once.
    """

    path: str
    inode: int
    device: int
    offset: int = 0
    partial: bytes = b""
    _fp: Optional[object] = field(default=None, repr=False, compare=False)

    def close(self) -> None:
        fp = self._fp
        self._fp = None
        if fp is not None:
            try:
                fp.close()
            except OSError:
                pass


Clock = Callable[[], float]
TimestampParser = Callable[[str], Optional[float]]


def _stat_identity(path: str) -> Optional[Tuple[int, int]]:
    """Return (device, inode) for *path*, or None if it is gone."""
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_dev, st.st_ino)


class ShardAwareLogTailFollower:
    """Follow a glob of log files with per-shard inode/offset tracking.

    The follower is driven by :meth:`poll`. Each call to :meth:`poll` returns
    the events that became available since the previous call, merged in
    (timestamp, shard_id, offset) order. Events without a timestamp sort
    before events with one, using ``float('-inf')`` as the sort key.

    Args:
        pattern: A glob pattern, e.g. ``"/var/log/app-*.log"``.
        timestamp_parser: Optional callable mapping a line to a float. When
            omitted, events have ``timestamp=None`` and ordering is by
            (shard_id, offset) only.
        clock: Optional callable returning the current time. Kept as a
            constructor argument so tests can pass a fake clock; the library
            itself never reads the wall clock.
    """

    def __init__(
        self,
        pattern: str,
        timestamp_parser: Optional[TimestampParser] = None,
        clock: Optional[Clock] = None,
    ) -> None:
        self.pattern = pattern
        self.timestamp_parser = timestamp_parser
        self._clock: Clock = clock if clock is not None else _monotonic
        self._shards: Dict[Tuple[int, int], FileShard] = {}
        self._path_to_id: Dict[str, Tuple[int, int]] = {}
        self._closed = False

    # -- discovery ---------------------------------------------------------

    def _discover(self) -> List[str]:
        """Return sorted paths currently matching the pattern.

        Sorting keeps shard iteration deterministic, which matters because
        the tie-break order between shards with equal timestamps is part of
        the contract.
        """
        return sorted(_glob.glob(self.pattern))

    def _sync_shards(self) -> None:
        """Reconcile the on-disk files with our tracked shards.

        A path may now point at a different inode (rotation). A tracked shard
        whose path no longer resolves to its inode is drained and retired.
        """
        seen_ids: set = set()
        for path in self._discover():
            ident = _stat_identity(path)
            if ident is None:
                continue
            seen_ids.add(ident)
            existing = self._shards.get(ident)
            if existing is None:
                shard = FileShard(path=path, inode=ident[1], device=ident[0])
                self._shards[ident] = shard
                self._path_to_id[path] = ident
            else:
                # Path may have been reused; keep the shard's original path
                # for reporting but refresh nothing else.
                self._path_to_id[path] = ident

        # Retire shards whose inode is no longer present under any path.
        stale = [ident for ident in self._shards if ident not in seen_ids]
        for ident in stale:
            shard = self._shards.pop(ident)
            shard.close()
            for p, i in list(self._path_to_id.items()):
                if i == ident:
                    del self._path_to_id[p]

    # -- reading -----------------------------------------------------------

    def _read_shard(self, shard: FileShard) -> List[LogEvent]:
        """Read new bytes from *shard* and return complete lines.

        Handles two awkward cases:

        * A line split across reads: bytes after the last newline are kept in
          ``shard.partial`` and prepended on the next read.
        * Truncation: if the file is now shorter than our offset, assume the
          writer truncated it and restart from zero.
        """
        events: List[LogEvent] = []
        try:
            st = os.stat(shard.path)
        except OSError:
            return events
        if (st.st_dev, st.st_ino) != (shard.device, shard.inode):
            # Path was rotated away; nothing more to read here.
            return events
        if st.st_size < shard.offset:
            shard.offset = 0
            shard.partial = b""
        if st.st_size == shard.offset:
            return events

        try:
            with open(shard.path, "rb") as fp:
                fp.seek(shard.offset)
                chunk = fp.read()
        except OSError:
            return events

        if not chunk:
            return events

        base = shard.offset
        data = shard.partial + chunk
        # Bytes consumed from the file proper: chunk length. The partial
        # prefix came from an earlier read and was already accounted for.
        new_offset = base + len(chunk)

        start = 0
        while True:
            nl = data.find(b"\n", start)
            if nl == -1:
                break
            raw = data[start:nl]
            # Byte offset of this line's start within the file.
            line_offset = base - len(shard.partial) + start
            if line_offset < 0:
                line_offset = 0
            line = raw.decode("utf-8", errors="replace")
            if line.endswith("\r"):
                line = line[:-1]
            ts = None
            if self.timestamp_parser is not None:
                ts = self.timestamp_parser(line)
            events.append(
                LogEvent(
                    shard_path=shard.path,
                    inode=shard.inode,
                    device=shard.device,
                    offset=line_offset,
                    line=line,
                    timestamp=ts,
                )
            )
            start = nl + 1

        shard.partial = data[start:]
        shard.offset = new_offset
        return events

    # -- public API --------------------------------------------------------

    def poll(self) -> List[LogEvent]:
        """Return new events since the previous poll, in merged order.

        Calling :meth:`poll` after :meth:`close` raises ``RuntimeError``; that
        is a programming error, not a transient condition.
        """
        if self._closed:
            raise RuntimeError("follower is closed")
        self._sync_shards()
        collected: List[LogEvent] = []
        for ident in sorted(self._shards):
            shard = self._shards[ident]
            collected.extend(self._read_shard(shard))
        collected.sort(key=_event_sort_key)
        return collected

    def shards(self) -> List[FileShard]:
        """Return a snapshot of the currently tracked shards."""
        return [self._shards[i] for i in sorted(self._shards)]

    def close(self) -> None:
        """Release file handles. Idempotent."""
        if self._closed:
            return
        self._closed = True
        for shard in self._shards.values():
            shard.close()
        self._shards.clear()
        self._path_to_id.clear()

    def __enter__(self) -> "ShardAwareLogTailFollower":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


_NEG_INF = float("-inf")


def _event_sort_key(ev: LogEvent) -> Tuple[float, int, int, int]:
    """Deterministic merge key.

    Events without a timestamp sort before events with one. Ties are broken
    by (device, inode, offset), which is stable across polls and across
    process restarts as long as the files are the same.
    """
    ts = ev.timestamp if ev.timestamp is not None else _NEG_INF
    return (ts, ev.device, ev.inode, ev.offset)


def _monotonic() -> float:
    import time

    return time.monotonic()


def follow(
    pattern: str,
    timestamp_parser: Optional[TimestampParser] = None,
    clock: Optional[Clock] = None,
) -> Iterator[LogEvent]:
    """Convenience generator that yields events from :meth:`poll` forever.

    This is a thin wrapper; it does not sleep. Callers that need polling
    should drive :class:`ShardAwareLogTailFollower` directly so they control
    their own pacing. The generator is provided for the common case of
    ``for ev in follow(pattern): ...`` where the caller is already in a loop.
    """
    follower = ShardAwareLogTailFollower(
        pattern, timestamp_parser=timestamp_parser, clock=clock
    )
    try:
        while True:
            events = follower.poll()
            if not events:
                return
            for ev in events:
                yield ev
    finally:
        follower.close()
