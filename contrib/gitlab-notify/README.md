# gitlab-notify

Desktop notifications for GitLab webhooks, fed by `hookstr drain --exec`.
A single stdlib-only Python 3 script: no install, no dependencies.

```
hookstr drain --config ~/.config/hookstr/notify.toml \
    --exec 'contrib/gitlab-notify/gitlab-notify.py --me alice' gitlab
```

Give the drain its own config with its own `redb_path` and `db_path` (see
"Delivering to a command" in the top-level README): replay state is keyed by
event id alone, so sharing them with another drain would steal its acks.

## What it notifies

| signal | from | icon |
|---|---|---|
| Pipeline failed / canceled, with the failed job names | Pipeline Hook on an MR (`source: merge_request_event`) | `pipeline_failed` |
| Pipeline passed ("fixed" after a failure) | the same | `pipeline_passed` |
| Default-branch pipeline failed, then fixed | Pipeline Hook, `source: push` on the default branch | the same two |
| New / reopened MR | Merge Request Hook `open`, `reopen` | `new_mr` |
| Approved | `approved` / `approval` | actor's avatar, else `approved` |
| Merged, closed | `merge`, `close` | `merged`, `closed` |
| Review requested (the added reviewers) | `update` with `changes.reviewers` | actor's avatar, else `review` |
| Changes requested | `detailed_merge_status` becomes `requested_changes` | `changes_requested` |
| Merge conflict | `detailed_merge_status` becomes `conflict`, or `merge_status` becomes `cannot_be_merged` | `conflict` |
| Comments, digested per MR | Note Hook `create` on a MergeRequest | commenter's avatar, else `comments` |
| Pushes to the default branch (`--push` only) | Push Hook | `push` |

Ignored: job hooks, child pipelines (`source: parent_pipeline`; the parent
reports the outcome), running/pending pipelines, note edits (a review bot
edits its notes in place constantly), system notes, notes on issues and
commits, and with `--me <username>` your own comments, approvals and pushes.

No hook announces a conflict or a change request on its own, so every
MR-bearing delivery (MR, pipeline and note hooks, including description-only
edits) is compared against the last status stored for that MR, and only a
transition notifies. Transient statuses GitLab passes through while
rechecking (`unchecked`, `checking`, `approvals_syncing`, `preparing`) don't
replace the stored one, so `conflict → unchecked → conflict` is not news.

## Batches, phases and acks

- **Exit 0 always**, once the batch is handled. Events it doesn't care
  about, malformed lines, a hook that trips a bug, a failed notifier or
  avatar fetch: all logged to stderr, none fail the batch, since a retry
  would only replay the same batch.
- **Dedupe** on `x-gitlab-event-uuid` (the envelope `id` when it's missing),
  kept for 14 days, so a redelivered or retried batch doesn't notify twice.
- **`HOOKSTR_PHASE=backfill`**: one summary notification for the batch
  ("1 pipeline failed, 1 merged, 12 comments on 4 MRs") instead of one per
  event. `--phase` overrides the variable; unset means `live`.
- **`--max-age`** (default `24h`): events received longer ago than this
  don't notify in either phase. They still update the per-MR state, so a
  conflict learned during backfill isn't announced again when live starts.
- **`--budget`** (default `40s`): past it, no more avatar fetches or
  notifications, so the batch still acks inside the drain's
  `--exec-timeout` (60s by default).

## Comment digests, without a timer

A busy review leaves many comments on one MR over several minutes, more
than the drain's 2s `--batch-window` can merge. The script runs once per
batch and has no timer, so the digest lives in the state file:

- **With in-place replacement** (terminal-notifier `-group`, notify-send
  `-r`): the first comment notifies at once ("Bob: looks good"). Each
  comment within `--window` (default `60s`) of the previous one replaces it
  with a running count: "15 new comments (Alice, Bob)". After a quiet
  window, the next comment starts a fresh digest.
- **Without it** (osascript): a cooldown. The first comment notifies; the
  ones after it are held until a comment arrives `--window` after that
  notification, which then carries the held ones: "4 new comments (…)".
  Held comments surface only when another comment arrives.

Within one batch only the last notification per MR and kind goes out.
Pipeline failures and conflicts are never folded into a digest: each gets
its own notification.

## Notifiers

`--notifier auto` (the default) picks, in order: `terminal-notifier` on
macOS when it's on PATH, else `osascript`; `notify-send` elsewhere. If
none is available, it prints the notifications to stdout.

What works on macOS 26.6 (checked by hand, with terminal-notifier 2.0.0):

- **terminal-notifier** is the one to install (`brew install
  terminal-notifier`). `-group` replaces a notification in place across
  batches, `-open <url>` opens the MR on click, and `-contentImage <png>`
  shows the signal icon or avatar as the thumbnail on the right. The
  left-hand icon is terminal-notifier's own (a Terminal icon). The script
  doesn't pass `-appIcon`, which relies on private API.
- **osascript** `display notification` works with no install, but shows
  the Script Editor icon, takes no image and no click URL, and can't
  replace, hence the cooldown digest.
- **notify-send** (Linux, not tested here): `-i <png>` for the image,
  `-p`/`-r` to replace in place (libnotify ≥ 0.7.9), `-u critical` for
  pipeline failures and conflicts. The replace ids live in the state file.

A proper left-hand icon on macOS would need a small signed helper app of
our own (UNUserNotificationCenter, its own bundle icon, image attachments,
click-to-open). That is not built here.

## Icons and avatars

`icons/*.png` are drawn for this project by `make-icons.py` (stdlib only;
rerun it after changing a shape or colour). There's one per signal, plus a
summary that uses the worst signal's icon.

For comments, approvals and review requests, the actor's `avatar_url` is
fetched (3s timeout, at most 2 MB, image content types only) and cached as
`~/.cache/gitlab-notify/avatars/<user id>.<ext>` for 7 days. A failed fetch
is remembered for an hour and falls back to the signal icon.

## Trying it out

`hookstr peek --exec` runs the same loop as `drain --exec` with the acks kept
in memory, so nothing is marked delivered:

```
# Replay everything stored as backfill batches, printing instead of notifying.
hookstr peek --all --exec \
    'contrib/gitlab-notify/gitlab-notify.py --dry-run --state-dir /tmp/gn-state' gitlab

# The same, but per-event live delivery, including events older than a day.
hookstr peek --all --exec \
    'contrib/gitlab-notify/gitlab-notify.py --dry-run --phase live --max-age 3650d --state-dir /tmp/gn-state' gitlab

# Or pipe the envelopes in directly.
hookstr peek --all --json gitlab | contrib/gitlab-notify/gitlab-notify.py --dry-run
```

`--dry-run` prints one JSON line per notification. It never fetches avatars.
It reads the real state (`$XDG_STATE_HOME/gitlab-notify/state.json`, else
`~/.local/state/gitlab-notify/`) but writes state only to a `--state-dir`
you pass explicitly, so a dry run never changes what the real notifier will
consider seen. Delete that directory to start over.

## Tests

```
python3 -m unittest discover -s contrib/gitlab-notify
```

All payloads in the tests are synthetic.
