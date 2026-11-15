# shard-aware-log-tail-follower

Follows multiple rotating log files matching a glob pattern, tracking inode and
offset per shard to produce a merged, ordered event stream.

```python
import re
from shard_aware_log_tail_follower import ShardAwareLogTailFollower


def parse_ts(line):
    m = re.match(r"t=(\d+) ", line)
    return float(m.group(1)) if m else None


follower = ShardAwareLogTailFollower(
    "/var/log/app-*.log",
    timestamp_parser=parse_ts,
)

for event in follower.poll():
    print(event.shard_path, event.offset, event.line)

follower.close()
```

## Why this exists

A naive tailer that keys on the path name loses data the moment a log rotates:
the path is reused for a new file, and the tailer either replays bytes it has
already seen or skips the first bytes of the new file. The fix is to treat the
`(st_dev, st_ino)` pair as the identity of a shard and keep a byte offset per
shard, not per path.

The trade-off: ordering across shards is by `(timestamp, device, inode,
offset)`. If your lines do not carry a timestamp, events are ordered purely by
shard identity, which is deterministic but not chronological. Supply a
`timestamp_parser` if you need real merge order.

## The awkward edge

A line split across two reads is held in memory until the newline arrives, so
the follower can return an empty list even though bytes were written. Do not
treat an empty `poll()` as "no more data ever" — it means "no complete line
right now". A file truncated in place (same inode, smaller size) is detected
and re-read from offset zero.

## API

- `ShardAwareLogTailFollower(pattern, timestamp_parser=None, clock=None)`
  - `poll() -> list[LogEvent]`
  - `shards() -> list[FileShard]`
  - `close()`
- `LogEvent` with `.shard_path`, `.inode`, `.device`, `.offset`, `.line`,
  `.timestamp`, and the derived `.shard_id`.
- `FileShard` with `.path`, `.inode`, `.device`, `.offset`.
- `follow(pattern, timestamp_parser=None, clock=None)` — a generator that
  yields events from a single `poll()` and then stops when no more are
  available. It does not sleep.
