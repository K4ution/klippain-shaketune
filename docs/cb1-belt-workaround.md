# Experimental CB1/CB2 belt comparison workaround

This fork addresses the memory/processing overlap described in
[upstream issue #285](https://github.com/Frix-x/klippain-shaketune/issues/285).
It is an experimental mitigation, not a confirmed fix for every MCU timing error.

`COMPARE_BELTS_RESPONSES` now transfers each completed recording to the existing
writer process and clears the main process's measurement list. It waits for a
FIFO acknowledgement after JSON serialization and a Zstandard block flush before
starting the next belt test. The wait yields to Klipper's reactor. It does not
rely on `Queue.empty()`, which can report an empty queue before work is finished.

Both recordings remain in one `.stdata` file. The combined comparison graph is
still computed after all motion has finished. Unlike the literal suggestion in
the issue, this does **not** generate separate per-belt PNGs or run the PSD
analysis between belts. The comparison needs both recordings, and moving the
raw-data serialization out of the second test is the narrower initial change.

The motion limits and input shaper are restored in a `finally` block if the test
or intermediate flush raises an exception. No Klipper MCU timeout is changed.
Other measurement commands keep their existing chunk-flush behavior.

## Use an existing installation

Run while the printer is idle, over SSH as the usual printer user:

```bash
cd ~/klippain_shaketune &&
git remote set-url origin https://github.com/K4ution/klippain-shaketune.git &&
git fetch origin &&
git merge --ff-only origin/main &&
sudo systemctl restart klipper
```

This assumes the existing installation follows `main`. If Git reports local
changes or a divergent branch, preserve those changes and resolve that before
continuing; do not use a hard reset. The Klipper symlink still points to the same
directory. No dependencies or printer configuration changes are needed.

If Moonraker already has `[update_manager Klippain-ShakeTune]`, change its `origin`
to `https://github.com/K4ution/klippain-shaketune.git`, keep `primary_branch: main`,
and restart Moonraker. Otherwise its updater will still refer to upstream.

Run the usual `COMPARE_BELTS_RESPONSES` command. New `Saving belt_... measurements
before continuing...` messages should appear between the recordings.

## Validation and limits

Run `python -m unittest discover -s tests -v` for the regression tests. They cover
recording order, the writer acknowledgement, retention of both measurements in
one file, timeout/failure handling, and restoration of motion settings. They use
Klipper doubles and a passthrough compressor, so they do not validate real-time
behavior, real multiprocessing on the CB1, or Zstandard compression itself.

Hardware validation is still required. If the MCU error occurs during the first
belt test, before any intermediate save, this particular change may not help.
Retain `klippy.log` and the full error message for further diagnosis.

To undo only this workaround, revert the fork's workaround commit with
`git revert <commit-sha>`, then restart Klipper. This preserves other local commits.
