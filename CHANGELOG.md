# dovewarden 0.1.1-homelab.5

- fix: every sync is now a real full sync (`fullSync`, as `doveadm sync -f`). A sync without state still skipped mailboxes whose counters matched, so one delivery or flag change on each side between two syncs was never copied, and homelab.4's "every sync is full" was not true. `DOVEWARDEN_FULL_SYNC_INTERVAL` set to a duration limits full syncs to after a failure, each user's first sync after start, and that interval
- fix: when Doveadm is unreachable, pause all syncs and probe it every 2 s instead of retrying each user; on recovery every waiting user is synced at once
- feat: `dovewarden_syncs_total{result,full}` and `dovewarden_doveadm_paused` metrics
- change: requests to Doveadm send `User-Agent: dovewarden/<version>` instead of `Go-http-client/1.1`

# dovewarden 0.1.1-homelab.4

- fix: every sync is now a full sync and no state is kept. Incremental state could go wrong silently: dsync exited 0 while the sides stayed different. This replaces the state handling of homelab.1 to homelab.3 and also avoids the 32 KiB state limit (upstream #14, #15, #16)

# dovewarden 0.1.1-homelab.3

- fix: run one sync per user at a time. Overlapping syncs could store state claiming mail was copied when it was not, so later syncs reported success and left the sides different (upstream #10)

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