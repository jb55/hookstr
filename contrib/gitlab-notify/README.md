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

`--notifier auto` (the default) picks, in order: on macOS
nudge when it's installed (see below), else
`terminal-notifier` when it's on PATH, else `osascript`; `notify-send`
elsewhere. If none is available, it prints the notifications to stdout.

What works on macOS 26.6 (checked by hand):

- **nudge** (`--notifier nudge`), a small signed notifier app of our own,
  is the one to use. Its own icon on the left, the signal icon or avatar
  as a thumbnail on the right, in-place replacement across batches, a
  click opens the MR (also after nudge has exited), and action buttons.
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

### nudge

nudge used to live here as `macos/` (GitLabNotify.app) and is now its own
repo: build and install it with its `build.sh`, which puts `Nudge.app` in
`~/Applications`. The script looks for it at `$NUDGE_APP`, then
`~/Applications/Nudge.app`, then `/Applications/Nudge.app`, and runs its
executable once per notification:

```
Nudge.app/Contents/MacOS/Nudge --title T --message M --group G --thread T \
    [--subtitle S] [--url U] [--image PNG] [--avatar PNG --sender NAME] \
    [--action ID=LABEL]... [--env KEY=VALUE]...
```

The group is the notification's identity, so a later one with the same
group replaces it (with a fresh banner). The thread is the MR (or the
branch, for pushes and default-branch pipelines), so one MR's pipeline,
approval and comment digest stack together in Notification Center, while
each keeps its own group and replaces only itself. A non-zero exit is
logged and the batch carries on.

For comments, approvals and review requests with a fetched avatar (below),
the script passes it as `--avatar` with the person's name as `--sender`,
and the signal icon as the thumbnail. A nudge built with its provisioning
profile shows the avatar as the main icon, with the person's name as the
first line and the MR below it (see nudge's README); without one, it
posts a plain notification.

`--avatar`, `--action` and `--env` are passed only when used, so a nudge
built before them still posts everything else; when it rejects them (exit
64) the script logs "update it" and posts again without them.

### Action buttons

nudge notifications can carry buttons that run a command of
yours with the MR's context, e.g. a "Handle Review" button on comment and
change-request notifications. Configure them in
nudge's `$XDG_CONFIG_HOME/nudge/actions.json` (default
`~/.config/nudge/actions.json`):

```json
{
  "handle-review": {
    "label": "Handle Review",
    "kinds": ["comments", "changes_requested"],
    "command": ["/Users/alice/bin/handle-review", "--verbose"]
  }
}
```

- The key is the action's id (letters, digits, `.`, `_`, `-`); `label`
  is the button text; `kinds` picks the notifications that get the button,
  from the signal kinds in the table above (`comments`, `changes_requested`,
  `pipeline_failed`, …).
- `command` is an argv list, not a shell string, and its first element
  must be an absolute path. The command runs from your home directory with
  the environment LaunchServices gives apps, which is minimal: no shell
  profile and a bare `PATH`. Use absolute paths throughout, or wrap it:
  `["/bin/bash", "-lc", "handle-review"]` (the string is yours and fixed;
  the MR data only arrives as env vars).
- It gets these environment variables, each only when known:

  | variable | value |
  |---|---|
  | `NUDGE_ACTION` | the action id (set by nudge) |
  | `GITLAB_NOTIFY_KIND` | the notification's kind |
  | `GITLAB_PROJECT` | project path, e.g. `acme/widgets` |
  | `GITLAB_MR_IID` | the MR's iid |
  | `GITLAB_SOURCE_BRANCH` | the MR's source branch (for pushes and default-branch pipelines, the branch) |
  | `GITLAB_MR_URL` | the MR's web URL |
  | `GITLAB_NOTE_URL` | the comment's URL (comment notifications; in a digest, the latest comment) |

On a click, macOS relaunches nudge, which reads `actions.json` again,
looks the id up, starts the command detached and exits; the command's
output is appended to `~/Library/Logs/nudge-actions.log`, and nudge's own
complaints (unknown id, bad command) go to the unified log:
`log show --last 10m --predicate 'process == "Nudge"'`. The command
only ever comes from this file at click time, never from a webhook or the
notification, and webhook data only ever reaches it as env vars. Since
nudge resolves the path from its own environment, which doesn't carry a
shell's `XDG_CONFIG_HOME`, keep the file at the default path unless you set
that variable for GUI apps too.

A missing file means no buttons. A malformed file or entry is logged by the
script and skipped; the notification still goes out, with whatever buttons
remain valid. Edits take effect on the next notification, and a click
always runs what the file says now.

With the default Banners style, buttons show on hover (several fold into
an "Options" menu); set Nudge to Alerts in System Settings >
Notifications to keep them visible. Backfill summaries, terminal-notifier,
osascript and notify-send get no buttons.

## Icons and avatars

`icons/*.png` are drawn for this project by `make-icons.py` (stdlib only;
rerun it after changing a shape or colour). There's one per signal, plus a
summary that uses the worst signal's icon.

For comments, approvals and review requests, the actor's `avatar_url` is
fetched (and shown as the main icon by nudge, or as the thumbnail by the
other notifiers) (3s timeout, at most 2 MB, image content types only) and cached as
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
