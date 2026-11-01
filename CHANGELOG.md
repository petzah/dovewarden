# dovewarden 0.1.1-homelab.3

- fix: run one sync per user at a time. Overlapping syncs could store state claiming mail was copied when it was not, so later syncs reported success and left the sides different (upstream #10)
- fix: the background pass is now a real full sync, and its threshold counts only full syncs, so a busy user cannot avoid it

# dovewarden 0.1.1-homelab.2

- fix: drop replication state on any failure Doveadm reports: dsync also rejects stale state with exit code 2, which still left a user stuck

# dovewarden 0.1.1-homelab.1

Fork of 0.1.1.

- fix: drop replication state when dsync rejects it with exit code 75, so a user no longer stops syncing for good (upstream #14, #16)
- fix: do not store state larger than Doveadm accepts (upstream #15)
- feat: per-user retry backoff, 1 s doubling to 30 s (upstream #7)
- ci: publish the image to ghcr.io/petzah/dovewarden on tags

# dovewarden 0.1.1

Released on 2026-02-01

- fix: correctly replicate on mail delivery events

# dovewarden 0.1.0

Released on 2026-01-31

- initial release