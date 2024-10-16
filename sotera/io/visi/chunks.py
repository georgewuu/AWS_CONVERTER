import os
import logging

logger = logging.getLogger(__name__)


class VisiChunker:
    def __init__(self, filename_root, chunksize=512000):
        self.last_segment = None
        self.filename_root = filename_root
        self.chunksize = chunksize
        self.filesize = 0
        self.num = None
        self.fn = None
        self.fp = None
        self.map_ = []
        self.chunk_min_sn = None
        self.chunk_max_sn = None
        self.chunk_min_ts = None
        self.chunk_max_ts = None
        self._next_chunk(init=True)

    def _reset_on_chunk(self):
        self.chunk_min_sn = None
        self.chunk_max_sn = None
        self.chunk_min_ts = None
        self.chunk_max_ts = None
        self.filesize = 0

    def _next_chunk(self, init=False, get_next=True):
        if init:
            self.num = -1
        elif self.filesize > 0:
            self.map_.append(
                {
                    "num": self.num,
                    "size": self.filesize,
                    "file": os.path.basename(self.fn),
                    "segment": int(self.last_segment),
                    "ts": (self.chunk_min_ts, self.chunk_max_ts),
                    "sn": (self.chunk_min_sn, self.chunk_max_sn),
                }
            )
            self.fp.close()
        else:
            self.fp.close()
            os.remove(self.fn)

        if get_next:
            self.num += 1
            self.fn = "{}-{:02d}.vchk".format(self.filename_root, self.num)
            logger.info("Writting chunk {}".format(self.fn))
            self.fp = open(self.fn, "wb")
            self._reset_on_chunk()

    def consume_packet(self, segment, sn, timestamp, packet_string):
        try:
            if sn is not None and segment is not None:
                if self.last_segment is None:
                    self.last_segment = segment
                if self.last_segment != segment:
                    self._next_chunk()
                    self.last_segment = segment

            self.fp.write(packet_string)
            self.filesize += len(packet_string)
            if timestamp is not None:
                self.chunk_min_ts = (
                    min(self.chunk_min_ts, timestamp)
                    if self.chunk_min_ts is not None
                    else timestamp
                )
                self.chunk_max_ts = (
                    max(self.chunk_max_ts, timestamp)
                    if self.chunk_max_ts is not None
                    else timestamp
                )
            if sn is not None:
                self.chunk_min_sn = (
                    min(self.chunk_min_sn, sn) if self.chunk_min_sn is not None else sn
                )
                self.chunk_max_sn = (
                    max(self.chunk_max_sn, sn) if self.chunk_max_sn is not None else sn
                )
            if self.filesize >= self.chunksize:
                self._next_chunk()
        except:  # noqa
            logger.exception("error consuming packet")

    def complete(self):
        self._next_chunk(get_next=False)
        if self.fp:
            self.fp.close()
