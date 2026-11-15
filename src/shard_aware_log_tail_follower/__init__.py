"""Shard-aware log tail follower.

Follows multiple rotating log files matching a glob pattern, tracking inode and
offset per shard to provide a merged, ordered event stream.
"""

from .core import (
    FileShard,
    ShardAwareLogTailFollower,
    LogEvent,
    follow,
)

__all__ = [
    "FileShard",
    "ShardAwareLogTailFollower",
    "LogEvent",
    "follow",
]
