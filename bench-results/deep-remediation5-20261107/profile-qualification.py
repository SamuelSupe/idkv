#!/usr/bin/env python3
"""Run the unchanged qualification load while timing fsync on its own servers."""
from pathlib import Path
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "scripts"))
import qualification


class ProfileLab(qualification.Lab):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tracers = {}

    def start(self, node, env_extra=None):
        super().start(node, env_extra)
        if node < 2:
            name = f"fsync-node-{node}-{self.starts[node]}"
            diagnostic = (self.root / (name + "-stderr.log")).open("w")
            process = subprocess.Popen(["sudo", "-n", "strace", "-f", "-ttt", "-T", "-yy",
                "-e", "trace=fsync,fdatasync", "-o", str(self.root / (name + ".log")),
                "-p", str(self.processes[node].pid)], stdout=diagnostic, stderr=diagnostic)
            self.tracers[node] = (process, diagnostic)
            time.sleep(.05)
            assert process.poll() is None, "strace could not attach; inspect its diagnostic"

    def stop(self, node, kill=False):
        super().stop(node, kill)
        tracer = self.tracers.pop(node, None)
        if tracer:
            process, diagnostic = tracer
            process.wait(timeout=5)
            diagnostic.close()


qualification.Lab = ProfileLab
qualification.main()
