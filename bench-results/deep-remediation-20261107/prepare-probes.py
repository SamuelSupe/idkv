#!/usr/bin/env python3
"""Make an isolated source copy for the same instrumented maintenance probes."""
from pathlib import Path
import argparse
import shutil

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
args = p.parse_args()
repo = Path(__file__).resolve().parents[2]
args.root.mkdir(parents=True, exist_ok=False)
for name in ['Cargo.toml', 'Cargo.lock']:
    shutil.copyfile(repo / name, args.root / name)
for name in ['src', 'tests', 'examples']:
    shutil.copytree(repo / name, args.root / name)
probes = Path(__file__).resolve().parent
shutil.copyfile(probes / 'maintenance-probe.rs', args.root / 'tests' / 'idle_probe.rs')
p = args.root / 'src' / 'tenant' / 'manager.rs'
s = p.read_text()
needle = 'fn maintain_tenant(inner: &Arc<Inner>, slot: &Arc<Mutex<Slot>>) {'
assert s.count(needle) == 1
hook = """
    #[cfg(test)]
    if AUDIT_MAINT_DELAY.load(Ordering::Acquire) {
        thread::sleep(Duration::from_millis(20));
        if let Some(core) = &slot.lock().core {
            *AUDIT_MAINT_COUNTS.get_or_init(|| Mutex::new(BTreeMap::new())).lock().entry(core.engine.info.id).or_default() += 1;
        }
    }
"""
s = s.replace(needle, needle + hook)
s += (probes / 'scheduler-probe.rs').read_text()
p.write_text(s)
