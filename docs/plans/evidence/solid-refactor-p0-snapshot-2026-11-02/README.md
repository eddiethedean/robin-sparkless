# P0 working tree snapshot

This retained snapshot reconstructs the pre-closeout working tree recorded in
[`solid-refactor-4.19.md`](../../solid-refactor-4.19.md).

- `metadata.txt` identifies the source commit and tracked patch digest.
- `tracked.patch.gz` contains the tracked changes from that snapshot.
- `untracked-manifest.tsv` records the untracked paths and their file digests.
- `untracked-files.tar.gz` contains those untracked files.
- `status.txt` records the original working tree status.

The artifact is evidence only; it does not change the current checkout.
