#!/usr/bin/env python3
"""Copy current sources and append review-only reproductions; product files stay unchanged."""
import argparse
from pathlib import Path
import shutil
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--root',type=Path,required=True)
args=p.parse_args()
repo=Path(__file__).resolve().parents[2]
probes=Path(__file__).resolve().parent
args.root.mkdir(parents=True,exist_ok=False)
for name in ['Cargo.toml','Cargo.lock']:shutil.copyfile(repo/name,args.root/name)
for name in ['src','tests','examples']:shutil.copytree(repo/name,args.root/name)
for target,probe in [('tests/replication.rs','replication-global-probe.rs'),('src/tenant/manager.rs','quota-reopen-probe.rs')]:
    p=args.root/target
    p.write_text(p.read_text()+(probes/probe).read_text())
shutil.copyfile(probes/'create-orphan-probe.rs',args.root/'tests/audit_lifecycle.rs')
