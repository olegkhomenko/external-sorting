import atexit
import logging
import multiprocessing
import os
import queue
import shutil
import sys
import tempfile
import time
from functools import partial
from heapq import merge

from external_sorting.constants import BUF_SIZE, SORT_MEMORY

log = multiprocessing.log_to_stderr(level=logging.INFO)


def cleanup(tmpdir: str):
    log.info(f'Removing dir {dir}')
    shutil.rmtree(tmpdir)


def save(fout, data: list):
    for line in data:
        fout.write(line)


class Sorter(multiprocessing.Process):
    def __init__(self, filename, queue_, pill, tmpdir, sort_mem):
        self.filename = filename
        self.queue = queue_
        self.tmpdir = tmpdir
        self.sort_mem = sort_mem
        self.poison_pill = pill
        super().__init__()

    def run(self):
        with open(self.filename, 'rb') as fin:
            self._do_loop(fin)

    def _do_loop(self, fin):
        try:
            while not self.poison_pill.is_set():
                try:
                    data = fin.readlines(self.sort_mem)
                except MemoryError:
                    self.poison_pill.set()
                    log.error('Cannot allocate memory for sorting. Try lower -smc setting.')
                    return
                if not len(data):
                    break

                data.sort()
                fd, pathname = tempfile.mkstemp(dir=self.tmpdir)
                with os.fdopen(fd, 'wb') as fout:
                    save(fout, data)
                self.queue.put(pathname)
        except Exception as ex:
            log.exception(ex)
            self.poison_pill.set()


class Merger(multiprocessing.Process):
    QUEUE_TIMEOUT = 0.5

    def __init__(self, queue_, lock, pill, counter, max_chunks, tmpdir, bufsize):
        self.queue = queue_
        self.lock = lock
        self.poison_pill = pill
        self.counter = counter
        self.max_chunks = max_chunks
        self.tmpdir = tmpdir
        self.bs = bufsize
        super().__init__()

    def run(self):
        try:
            while not self.poison_pill.is_set() and self.counter.value < self.max_chunks:
                with self.lock:
                    try:
                        a = self.queue.get(timeout=self.QUEUE_TIMEOUT)
                    except queue.Empty:
                        continue
                    try:
                        b = self.queue.get(timeout=self.QUEUE_TIMEOUT)
                    except queue.Empty:
                        self.queue.put(a)
                        continue

                self.counter.value += 1
                log.info(f'Started merge {self.counter.value} / {self.max_chunks}')
                new = self._merge(a, b)
                os.unlink(a)
                os.unlink(b)
                self.queue.put(new)

        except Exception as ex:
            log.exception(ex)
            self.poison_pill.set()

    def _merge(self, file1, file2):
        with open(file1, 'r') as f1, open(file2, 'r') as f2:
            fd, pathname = tempfile.mkstemp(dir=self.tmpdir)
            with os.fdopen(fd, 'w') as fout:
                self._write(fout, merge(self._read(f1), self._read(f2)))

        return pathname

    def _write(self, f, iterable):
        try:
            line = next(iterable)
            while line:
                f.writelines(line)
                line = next(iterable)
        except StopIteration:
            pass

    def _read(self, f):
        for chunk in iter(lambda: f.readlines(self.bs), ''):
            if len(chunk) == 0:
                return  # see https://www.python.org/dev/peps/pep-0479/#id34
            for line in chunk:
                yield line


class SortRunner:
    def __init__(self, input, output, cpus=None, bufsize=BUF_SIZE, sort_mem=SORT_MEMORY):
        self.input, self.output = input, output

        self.tmpdir = tempfile.mkdtemp()
        atexit.register(partial(cleanup, tmpdir=self.tmpdir))

        self.queue = multiprocessing.Queue()
        self.lock = multiprocessing.Lock()
        self.counter = multiprocessing.Value('I', 0)
        self.sort_mem = sort_mem
        self.chunks = self._get_chunks()
        self.cpus = cpus or multiprocessing.cpu_count()
        self.bs = bufsize
        self.poison_pill = multiprocessing.Event()

    def run(self):
        """One Sorter, many Mergers"""
        self.sorter = Sorter(self.input, self.queue, self.poison_pill, self.tmpdir, self.sort_mem)

        self.pool = [Merger(self.queue, self.lock, self.poison_pill, self.counter, self.chunks, self.tmpdir, self.bs)
                     for i in range(self.cpus)]

        self.pool.append(self.sorter)
        [p.start() for p in self.pool]

        def join_process(p):
            p.join(1)
            if p.is_alive():
                p.terminate()

        while len(self.pool):
            for p in self.pool:
                if not p.is_alive():
                    if p.exitcode:
                        self.poison_pill.set()
                        for d in self.pool:
                            join_process(d)
                        sys.exit(f'Worker finished with non-zero exit code {p.xitcode}')
                    join_process(p)
                    self.pool.remove(p)
            time.sleep(1)

        if not self.poison_pill.is_set():
            self._put_result()

    def _get_chunks(self):
        chunks = (os.path.getsize(self.input) // self.sort_mem)
        if os.path.getsize(self.input) % self.sort_mem == 0:
            chunks -= 1
        log.info(f'Chunks: {chunks}')
        return chunks

    def _put_result(self):
        try:
            final = self.queue.get(block=False)
        except queue.Empty:
            sys.exit('File not found.')
        else:
            log.info(f'Sorted file {final} size {os.path.getsize(final)}')
            shutil.move(final, self.output)
