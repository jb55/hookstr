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

`--notifier auto` (the default) picks, in order: on macOS the
GitLabNotify helper app when it's been built (see below), else
`terminal-notifier` when it's on PATH, else `osascript`; `notify-send`
elsewhere. If none is available, it prints the notifications to stdout.

What works on macOS 26.6 (checked by hand):

- **GitLabNotify.app** (`--notifier app`), a small signed helper in
  `macos/`, is the one to use. Our own icon on the left, the signal icon
  or avatar as a thumbnail on the right, in-place replacement across
  batches, and a click opens the MR, also after the helper has exited.
- **terminal-notifier** 2.0.0 (`brew install terminal-notifier`):
  `-group` replaces in place, `-open <url>` opens the MR on click, and
  `-contentImage <png>` shows the thumbnail. The left-hand icon is always
  terminal-notifier's own (a Terminal icon): `-appIcon` was tried on 26.6
  and has no effect, and `-sender` borrows another app's icon but breaks
  click-to-open.
- **osascript** `display notification` works with no install, but shows
  the Script Editor icon, takes no image and no click URL, and can't
  replace, hence the cooldown digest.
- **notify-send** (Linux, not tested here): `-i <png>` for the image,
  `-p`/`-r` to replace in place (libnotify ≥ 0.7.9), `-u critical` for
  pipeline failures and conflicts. The replace ids live in the state file.

### The macOS helper app

```
contrib/gitlab-notify/macos/build.sh
```

builds `macos/build/GitLabNotify.app` (not committed) with the Command Line
Tools' `swiftc`, no Xcode project: `main.swift`, `Info.plist` (`LSUIElement`,
so no Dock icon), and an `.icns` made from `icons/app.png` with `sips` and
`iconutil`. It signs the bundle with `$CODESIGN_IDENTITY` (default: the
author's Apple Development certificate; `security find-identity -v -p
codesigning` lists yours, `-` signs ad hoc) and registers it with
LaunchServices. Until it's registered, posting fails with "Notifications
are not allowed for this application". `$SWIFT_DEVELOPER_DIR` picks the
toolchain (default `/Library/Developer/CommandLineTools`; a nix shell's
`DEVELOPER_DIR` has no `swiftc`).

The script runs the bundle's executable once per notification:

```
GitLabNotify.app/Contents/MacOS/GitLabNotify --title T --message M \
    [--subtitle S] [--url U] [--image PNG] [--group G] [--thread T]
```

It posts through `UNUserNotificationCenter` with the group as the request
identifier, so a later post with the same group replaces the earlier one
(with a fresh banner). The thread is the MR (or the branch, for pushes and
default-branch pipelines), so one MR's pipeline, approval and comment
digest stack together in Notification Center, while each keeps its own
group and replaces only itself. The image goes in as an attachment, from a temp copy,
since attaching moves the file. The url rides in `userInfo`; on a click
macOS relaunches the app with no arguments, and it opens the url and exits.
Exit status: 0 posted, 2 notifications not allowed, 3 failed, 64 usage;
the script logs a non-zero exit and carries on.

The first post asks for permission (`GitLabNotify --authorize` asks without
posting); allow it. A Focus mode holds the banners back but still
updates Notification Center.

## Icons and avatars

`icons/*.png` are drawn for this project by `make-icons.py` (stdlib only;
rerun it after changing a shape or colour). There's one per signal, plus a
summary that uses the worst signal's icon, and `app.png`, the helper app's
icon.

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
