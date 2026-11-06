#!/usr/bin/env python3
"""Desktop notifications for GitLab webhooks, fed by `hookstr drain --exec`.

    hookstr drain --config notify.toml \
        --exec 'contrib/gitlab-notify/gitlab-notify.py --me alice' gitlab

Reads a batch of NDJSON envelopes on stdin (see "Envelope format" in the
hookstr README), turns the GitLab hooks in it into a few desktop
notifications, and exits 0, which acks the whole batch. Events it doesn't
care about are ignored, not failed: a non-zero exit would only make the
drain retry the same batch. Notifier and avatar failures are logged to
stderr and never fail the batch.

Try it on everything stored without acking anything:

    hookstr peek --all --exec \
        'contrib/gitlab-notify/gitlab-notify.py --dry-run --state-dir /tmp/gn' gitlab

Stdlib only. See README.md next to this file.
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ICON_DIR = os.path.join(HERE, "icons")
# Where to look for Nudge.app (the nudge repo), the macOS
# notifier: $NUDGE_APP first, then where its build.sh installs it.
NUDGE_APPS = ("~/Applications/Nudge.app", "/Applications/Nudge.app")

DAY_MS = 24 * 3600 * 1000
SEEN_TTL_MS = 14 * DAY_MS
MR_TTL_MS = 30 * DAY_MS

# detailed_merge_status values GitLab passes through while it rechecks an
# MR. They say nothing about the MR itself, so they never replace the
# stored status: conflict -> unchecked -> conflict is not a new conflict.
TRANSIENT_STATUS = {"unchecked", "checking", "approvals_syncing", "preparing", None, ""}

# Signal kinds, each with its own icon in icons/<kind>.png. Comments are
# the only kind folded into a per-MR digest.
KINDS = (
    "pipeline_failed", "pipeline_passed", "approved", "changes_requested",
    "conflict", "new_mr", "merged", "closed", "review", "comments", "push",
)
# Kinds that show the actor's avatar instead of the signal icon.
AVATAR_KINDS = {"comments", "approved", "review"}

SUMMARY_LABELS = (
    ("pipeline_failed", "pipeline failed", "pipelines failed"),
    ("conflict", "conflict", "conflicts"),
    ("changes_requested", "change request", "change requests"),
    ("pipeline_passed", "pipeline passed", "pipelines passed"),
    ("approved", "approval", "approvals"),
    ("review", "review request", "review requests"),
    ("new_mr", "new MR", "new MRs"),
    ("merged", "merged", "merged"),
    ("closed", "closed", "closed"),
    ("push", "push", "pushes"),
)


def log(msg):
    print("gitlab-notify: " + msg, file=sys.stderr, flush=True)


def plural(n, one, many):
    return "%d %s" % (n, one if n == 1 else many)


def first_line(text, limit=140):
    line = (text or "").strip().splitlines()[0] if (text or "").strip() else ""
    return line if len(line) <= limit else line[: limit - 1] + "…"


def actor_name(user):
    user = user or {}
    return user.get("name") or user.get("username") or "someone"


class Signal:
    """One thing worth telling the user about, before delivery decides how."""

    def __init__(self, kind, key, ts, title, subtitle, message, url=None, actor=None, ctx=None):
        self.kind = kind
        self.key = key          # "<project path>!<iid>" or "<project path>@<ref>"
        self.ts = ts            # received_at_ms of the envelope
        self.title = title
        self.subtitle = subtitle
        self.message = message
        self.url = url
        self.actor = actor or {}
        # What an action button's command gets as GITLAB_* env vars: project,
        # iid, source_branch, mr_url, note_url (only what's known).
        self.ctx = ctx

    def __repr__(self):
        return "Signal(%s %s %r)" % (self.kind, self.key, self.message)


# --- state -----------------------------------------------------------------


def empty_state():
    return {"version": 1, "seen": {}, "mrs": {}, "replace_ids": {}}


def load_state(path):
    try:
        with open(path) as f:
            state = json.load(f)
    except FileNotFoundError:
        return empty_state()
    except (OSError, ValueError) as e:
        log("state unreadable (%s), starting fresh" % e)
        return empty_state()
    base = empty_state()
    base.update(state if isinstance(state, dict) else {})
    return base


def save_state(path, state, now_ms):
    state["seen"] = {k: v for k, v in state["seen"].items() if now_ms - v < SEEN_TTL_MS}
    state["mrs"] = {
        k: v for k, v in state["mrs"].items() if now_ms - v.get("updated_ms", 0) < MR_TTL_MS
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".state.")
    with os.fdopen(fd, "w") as f:
        json.dump(state, f, separators=(",", ":"))
    os.replace(tmp, path)


# --- turning hooks into signals --------------------------------------------


class Processor:
    """Updates per-MR state from each hook and yields the signals it implies.

    Every MR-bearing delivery (MR, pipeline and note hooks all carry the
    MR) feeds the merge-status tracker, since no hook announces a conflict
    or a change request as an event of its own.
    """

    def __init__(self, state, me=None, push=False):
        self.state = state
        self.me = me
        self.push = push

    def mr(self, key, ts):
        m = self.state["mrs"].setdefault(key, {})
        m["updated_ms"] = max(ts, m.get("updated_ms", 0))
        return m

    def is_me(self, user):
        return bool(self.me) and (user or {}).get("username") == self.me

    def handle(self, env):
        """Signals for one envelope; an empty list when it's ignored."""
        if env.get("body_encoding") != "json" or not isinstance(env.get("body"), dict):
            return []
        headers = env.get("headers") or {}
        uuid = headers.get("x-gitlab-event-uuid") or env.get("id")
        ts = int(env.get("received_at_ms") or 0)
        if uuid:
            if uuid in self.state["seen"]:
                return []
            self.state["seen"][uuid] = ts
        body = env["body"]
        kind = body.get("object_kind")
        if kind == "merge_request":
            return self.merge_request(body, ts)
        if kind == "pipeline":
            return self.pipeline(body, ts)
        if kind == "note":
            return self.note(body, ts)
        if kind == "push" and self.push:
            return self.push_hook(body, ts)
        return []

    def track_status(self, key, ts, mr, project, url):
        """Signals for a transition into conflict or requested_changes."""
        if not mr or mr.get("state", "opened") != "opened":
            return []
        m = self.mr(key, ts)
        out = []
        dms = mr.get("detailed_merge_status")
        conflict = dms == "conflict" or mr.get("merge_status") == "cannot_be_merged"
        if conflict and not m.get("conflict"):
            out.append(Signal(
                "conflict", key, ts, mr_title(project, mr), mr.get("title"),
                "Merge conflict with %s" % (mr.get("target_branch") or "the target branch"),
                url))
        if dms not in TRANSIENT_STATUS or mr.get("merge_status") in ("can_be_merged", "cannot_be_merged"):
            m["conflict"] = conflict
        if dms not in TRANSIENT_STATUS:
            if dms == "requested_changes" and m.get("dms") != "requested_changes":
                out.append(Signal(
                    "changes_requested", key, ts, mr_title(project, mr), mr.get("title"),
                    "Changes requested", url))
            m["dms"] = dms
        return out

    def merge_request(self, body, ts):
        oa = body.get("object_attributes") or {}
        project = body.get("project") or {}
        key = mr_key(project, oa)
        if key is None:
            return []
        user = body.get("user") or {}
        url = oa.get("url")
        title, sub = mr_title(project, oa), oa.get("title")
        action = oa.get("action")
        out = []
        if action in ("open", "reopen"):
            verb = "Opened" if action == "open" else "Reopened"
            out.append(Signal("new_mr", key, ts, title, sub,
                              "%s by %s" % (verb, actor_name(user)), url, user))
        elif action in ("approved", "approval") and not self.is_me(user):
            out.append(Signal("approved", key, ts, title, sub,
                              "Approved by %s" % actor_name(user), url, user))
        elif action == "merge":
            out.append(Signal("merged", key, ts, title, sub, "Merged into %s by %s"
                              % (oa.get("target_branch") or "?", actor_name(user)), url, user))
        elif action == "close":
            out.append(Signal("closed", key, ts, title, sub,
                              "Closed by %s" % actor_name(user), url, user))
        elif action == "update":
            reviewers = (body.get("changes") or {}).get("reviewers")
            if reviewers:
                before = {r.get("id") for r in reviewers.get("previous") or []}
                added = [r for r in reviewers.get("current") or [] if r.get("id") not in before]
                if added:
                    names = ", ".join(actor_name(r) for r in added)
                    out.append(Signal("review", key, ts, title, sub,
                                      "Review requested: %s" % names, url, user))
        out.extend(self.track_status(key, ts, oa, project, url))
        return with_ctx(out, mr_ctx(project, oa))

    def pipeline(self, body, ts):
        oa = body.get("object_attributes") or {}
        project = body.get("project") or {}
        source = oa.get("source")
        status = oa.get("status")
        mr = body.get("merge_request")
        out = []
        if mr:
            key = mr_key(project, mr)
            if key is not None:
                out.extend(self.track_status(key, ts, mr, project, mr.get("url")))
            with_ctx(out, mr_ctx(project, mr))
        # Child pipelines report into their parent, which is what we notify on.
        if source == "parent_pipeline" or status not in ("success", "failed", "canceled"):
            return out
        if mr and source == "merge_request_event":
            key = mr_key(project, mr)
            if key is None:
                return out
            title, sub, url = mr_title(project, mr), mr.get("title"), mr.get("url")
            ctx = mr_ctx(project, mr)
        elif source == "push" and oa.get("ref") == project.get("default_branch"):
            key = "%s@%s" % (project.get("path_with_namespace"), oa.get("ref"))
            title, sub = project.get("name") or project.get("path_with_namespace"), oa.get("ref")
            url = oa.get("url")
            ctx = branch_ctx(project, oa.get("ref"))
        else:
            return out
        m = self.mr(key, ts)
        prev = m.get("pipeline") or {}
        if prev.get("id") == oa.get("id") and prev.get("status") == status:
            return out
        m["pipeline"] = {"id": oa.get("id"), "status": status}
        was_failed = prev.get("status") in ("failed", "canceled")
        if status == "success":
            if source == "push" and not was_failed:
                return out  # the default branch passing is news only when it was broken
            msg = "Pipeline fixed" if was_failed else "Pipeline passed"
            out.append(Signal("pipeline_passed", key, ts, title, sub, msg, url))
        else:
            failed = [b for b in body.get("builds") or []
                      if b.get("status") == "failed" and not b.get("allow_failure")]
            if status == "canceled":
                msg = "Pipeline canceled"
            elif failed:
                jobs = ", ".join(
                    "%s (%s)" % (b.get("name"), b.get("failure_reason") or b.get("stage"))
                    for b in failed[:4])
                more = " +%d more" % (len(failed) - 4) if len(failed) > 4 else ""
                msg = "Pipeline failed: %s%s" % (jobs, more)
            else:
                msg = "Pipeline failed"
            out.append(Signal("pipeline_failed", key, ts, title, sub, msg, url))
        return with_ctx(out, ctx)

    def note(self, body, ts):
        oa = body.get("object_attributes") or {}
        project = body.get("project") or {}
        mr = body.get("merge_request")
        if oa.get("noteable_type") != "MergeRequest" or not mr:
            return []
        key = mr_key(project, mr)
        if key is None:
            return []
        out = with_ctx(self.track_status(key, ts, mr, project, mr.get("url")), mr_ctx(project, mr))
        # The review bot edits its notes in place constantly: only creations count.
        if oa.get("action", "create") != "create" or oa.get("system"):
            return out
        user = body.get("user") or {}
        if self.is_me(user):
            return out
        sig = Signal("comments", key, ts, mr_title(project, mr), mr.get("title"),
                     "%s: %s" % (actor_name(user), first_line(oa.get("note"))),
                     oa.get("url") or mr.get("url"), user,
                     dict(mr_ctx(project, mr), note_url=oa.get("url")))
        out.append(sig)
        return out

    def push_hook(self, body, ts):
        project = body.get("project") or {}
        ref = body.get("ref") or ""
        branch = ref[len("refs/heads/"):] if ref.startswith("refs/heads/") else None
        if not branch or branch != project.get("default_branch"):
            return []
        n = body.get("total_commits_count") or len(body.get("commits") or [])
        if not n:
            return []
        user = {"name": body.get("user_name"), "username": body.get("user_username"),
                "id": body.get("user_id"), "avatar_url": body.get("user_avatar")}
        if self.is_me(user):
            return []
        key = "%s@%s" % (project.get("path_with_namespace"), branch)
        return [Signal("push", key, ts, project.get("name") or key, branch,
                       "%s pushed %s" % (actor_name(user), plural(n, "commit", "commits")),
                       project.get("web_url"), user, branch_ctx(project, branch))]


def mr_key(project, mr):
    path = (project or {}).get("path_with_namespace")
    iid = (mr or {}).get("iid")
    if not path or iid is None:
        return None
    return "%s!%s" % (path, iid)


def mr_ctx(project, mr):
    return {"project": (project or {}).get("path_with_namespace"), "iid": (mr or {}).get("iid"),
            "source_branch": (mr or {}).get("source_branch"), "mr_url": (mr or {}).get("url")}


def branch_ctx(project, branch):
    return {"project": (project or {}).get("path_with_namespace"), "source_branch": branch}


def with_ctx(signals, ctx):
    """Give the signals that have no context yet this one."""
    for s in signals:
        if s.ctx is None:
            s.ctx = ctx
    return signals


def mr_title(project, mr):
    return "%s !%s" % ((project or {}).get("name") or (project or {}).get("path_with_namespace"),
                       (mr or {}).get("iid"))


# --- notifiers ---------------------------------------------------------------


def run(argv, timeout=5):
    try:
        return subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True,
                              text=True, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        log("%s failed: %s" % (argv[0], e))
        return None


def nudge_bin():
    apps = ([os.environ["NUDGE_APP"]] if os.environ.get("NUDGE_APP") else []) + list(NUDGE_APPS)
    for app in apps:
        p = os.path.join(os.path.expanduser(app), "Contents", "MacOS", "Nudge")
        if os.access(p, os.X_OK):
            return p
    return None


class Nudge:
    """Nudge.app: its own icon on the left (or the person's avatar, with the
    icon as a badge), the image as an attachment, click to open, and the
    action buttons."""

    name = "nudge"
    replace = True  # the group is the request identifier: a repost replaces it

    def __init__(self, path):
        self.path = path

    # Notification context -> the env var an action button's command gets it as.
    CONTEXT_ENV = (("project", "GITLAB_PROJECT"), ("iid", "GITLAB_MR_IID"),
                   ("source_branch", "GITLAB_SOURCE_BRANCH"), ("mr_url", "GITLAB_MR_URL"),
                   ("note_url", "GITLAB_NOTE_URL"))

    def send(self, n, state):
        argv = [self.path, "--title", n["title"], "--message", n["message"],
                "--group", n["group"]]
        for k in ("subtitle", "url", "thread"):
            if n.get(k):
                argv += ["--" + k, n[k]]
        # With the avatar on the left, the thumbnail goes back to the signal icon.
        image = (icon(n["kind"]) or n.get("image")) if n.get("avatar") else n.get("image")
        if image:
            argv += ["--image", image]
        # Avatar and button flags only when used, so a nudge built before
        # them still posts everything else.
        extra = []
        if n.get("avatar"):
            extra += ["--avatar", n["avatar"], "--sender", n.get("sender") or n["title"]]
        for a in n.get("actions") or ():
            extra += ["--action", "%s=%s" % (a["id"], a["label"])]
        if n.get("actions"):
            extra += ["--env", "GITLAB_NOTIFY_KIND=" + n["kind"]]
            ctx = n.get("context") or {}
            for k, var in self.CONTEXT_ENV:
                if ctx.get(k) not in (None, ""):
                    extra += ["--env", "%s=%s" % (var, ctx[k])]
        # Longer than the helper's own 8s wait on a first-run permission prompt.
        r = run(argv + extra, timeout=10)
        if r and r.returncode == 64 and extra:
            log("nudge doesn't know avatars or action buttons (%s); update it. "
                "Posting without them." % r.stderr.strip())
            r = run(argv, timeout=10)
        if r and r.returncode != 0:
            log("nudge exited %d: %s" % (r.returncode, r.stderr.strip()))


class TerminalNotifier:
    name = "terminal-notifier"
    replace = True  # -group replaces the previous notification in place

    def __init__(self, path):
        self.path = path

    def send(self, n, state):
        argv = [self.path, "-title", n["title"], "-message", n["message"],
                "-group", n["group"]]
        if n.get("subtitle"):
            argv += ["-subtitle", n["subtitle"]]
        if n.get("url"):
            argv += ["-open", n["url"]]
        if n.get("image"):
            argv += ["-contentImage", n["image"]]
        r = run(argv)
        if r and r.returncode != 0:
            log("terminal-notifier exited %d: %s" % (r.returncode, r.stderr.strip()))


class Osascript:
    name = "osascript"
    replace = False  # no in-place replacement: digests fall back to a cooldown

    SCRIPT = ("on run argv\n"
              "display notification (item 1 of argv) with title (item 2 of argv)"
              " subtitle (item 3 of argv)\n"
              "end run")

    def send(self, n, state):
        # Text goes in as argv, never spliced into the script.
        r = run(["osascript", "-e", self.SCRIPT, n["message"], n["title"],
                 n.get("subtitle") or ""])
        if r and r.returncode != 0:
            log("osascript exited %d: %s" % (r.returncode, r.stderr.strip()))


class NotifySend:
    name = "notify-send"
    replace = True  # -p prints the id, -r replaces it (libnotify >= 0.7.9)

    def __init__(self, path):
        self.path = path

    def send(self, n, state):
        body = n["message"]
        if n.get("subtitle"):
            body = "%s\n%s" % (n["subtitle"], body)
        argv = [self.path, "-a", "gitlab-notify", "-p"]
        if n.get("image"):
            argv += ["-i", n["image"]]
        if n["kind"] in ("pipeline_failed", "conflict"):
            argv += ["-u", "critical"]
        old = state["replace_ids"].get(n["group"])
        if old:
            argv += ["-r", str(old)]
        r = run(argv + [n["title"], body])
        if r is None:
            return
        if r.returncode != 0:
            log("notify-send exited %d: %s" % (r.returncode, r.stderr.strip()))
            return
        out = r.stdout.strip()
        if out.isdigit():
            state["replace_ids"][n["group"]] = int(out)


class Printer:
    """--dry-run: print what would be sent, digesting as `inner` would."""

    name = "dry-run"

    def __init__(self, inner, out=sys.stdout):
        self.inner = inner
        self.replace = inner.replace if inner else True
        self.out = out

    def send(self, n, state):
        self.out.write(json.dumps({k: n.get(k) for k in (
            "kind", "group", "thread", "title", "subtitle", "message", "url", "image", "avatar",
            "actions")}) + "\n")


class Collector:
    """Records notifications; for tests."""

    name = "collector"

    def __init__(self, replace=True):
        self.replace = replace
        self.sent = []

    def send(self, n, state):
        self.sent.append(n)


def pick_notifier(choice):
    if choice == "none":
        return None
    nb = nudge_bin() if sys.platform == "darwin" else None
    if choice == "nudge" or (choice == "auto" and nb):
        return Nudge(nb or "Nudge")
    tn, ns = shutil.which("terminal-notifier"), shutil.which("notify-send")
    if choice == "terminal-notifier" or (choice == "auto" and sys.platform == "darwin" and tn):
        return TerminalNotifier(tn or "terminal-notifier")
    if choice == "osascript" or (choice == "auto" and sys.platform == "darwin"):
        return Osascript()
    if choice == "notify-send" or (choice == "auto" and ns):
        return NotifySend(ns or "notify-send")
    return None


# --- avatars -------------------------------------------------------------------


class Avatars:
    """The actor's avatar as a local file, or None. Never raises, never waits long."""

    TTL = 7 * 24 * 3600
    FAIL_TTL = 3600

    def __init__(self, cache_dir, fetch=True, timeout=3.0, deadline=None):
        self.dir = os.path.join(cache_dir, "avatars")
        self.fetch = fetch
        self.timeout = timeout
        self.deadline = deadline
        self.memo = {}

    def get(self, user):
        uid, url = (user or {}).get("id"), (user or {}).get("avatar_url")
        if uid is None or not url or not str(url).startswith(("http://", "https://")):
            return None
        uid = str(uid)
        if not uid.isdigit():
            return None
        if uid not in self.memo:
            self.memo[uid] = self.lookup(uid, url)
        return self.memo[uid]

    def lookup(self, uid, url):
        now = time.time()
        for ext in ("png", "jpg", "gif", "webp"):
            p = os.path.join(self.dir, "%s.%s" % (uid, ext))
            if os.path.exists(p) and now - os.path.getmtime(p) < self.TTL:
                return p
        fail = os.path.join(self.dir, uid + ".fail")
        if os.path.exists(fail) and now - os.path.getmtime(fail) < self.FAIL_TTL:
            return None
        if not self.fetch or (self.deadline and now > self.deadline):
            return None
        try:
            os.makedirs(self.dir, exist_ok=True)
            req = urllib.request.Request(url, headers={"User-Agent": "gitlab-notify"})
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                ctype = r.headers.get_content_type()
                data = r.read(2 * 1024 * 1024)
            ext = {"image/png": "png", "image/jpeg": "jpg", "image/gif": "gif",
                   "image/webp": "webp"}.get(ctype)
            if not ext or not data:
                raise ValueError("not an image: %s" % ctype)
            path = os.path.join(self.dir, "%s.%s" % (uid, ext))
            with open(path + ".tmp", "wb") as f:
                f.write(data)
            os.replace(path + ".tmp", path)
            return path
        except Exception as e:  # any failure just means the signal icon
            log("avatar %s: %s" % (uid, e))
            try:
                open(fail, "w").close()
            except OSError:
                pass
            return None


# --- action buttons --------------------------------------------------------------

ACTION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def actions_path():
    """nudge's actions.json: nudge runs a button's `command` from it when
    clicked; the `label` and `kinds` here pick which notifications get it."""
    base = os.environ.get("XDG_CONFIG_HOME") or os.path.expanduser("~/.config")
    return os.path.join(base, "nudge", "actions.json")


def load_actions(path):
    """The valid entries of actions.json, as [{id, label, kinds, command}].

    Never raises: a missing file means no buttons, and a broken file or
    entry is logged and skipped, so the notifications still go out.
    """
    try:
        with open(path) as f:
            conf = json.load(f)
    except FileNotFoundError:
        return []
    except (OSError, ValueError) as e:
        log("%s unreadable (%s); no action buttons" % (path, e))
        return []
    if not isinstance(conf, dict):
        log("%s: expected an object of actions; no action buttons" % path)
        return []
    out = []
    for aid, a in conf.items():
        a = a if isinstance(a, dict) else {}
        label, kinds, command = a.get("label"), a.get("kinds"), a.get("command")
        problem = None
        if not ACTION_ID.match(aid):
            problem = "id must be letters, digits, '.', '_' or '-'"
        elif not isinstance(label, str) or not label.strip():
            problem = "needs a label"
        elif not isinstance(kinds, list) or not all(isinstance(k, str) for k in kinds):
            problem = "kinds must be a list of notification kinds"
        elif (not isinstance(command, list) or not command
              or not all(isinstance(c, str) for c in command)
              or not os.path.isabs(command[0])):
            problem = "command must be an argv list starting with an absolute path"
        if problem:
            log("%s: action %r skipped: %s" % (path, aid, problem))
            continue
        unknown = [k for k in kinds if k not in KINDS]
        if unknown:
            log("%s: action %r: unknown kinds %s ignored" % (path, aid, ", ".join(unknown)))
        out.append({"id": aid, "label": label, "kinds": [k for k in kinds if k in KINDS],
                    "command": command})
    return out


def attach_actions(notes, actions):
    """Give each notification the buttons configured for its kind."""
    for n in notes:
        if n.get("context"):  # backfill summaries have none, and get no buttons
            n["actions"] = [{"id": a["id"], "label": a["label"]}
                            for a in actions if n["kind"] in a["kinds"]]


# --- delivery ------------------------------------------------------------------


def icon(kind):
    p = os.path.join(ICON_DIR, kind + ".png")
    return p if os.path.exists(p) else None


def notification(sig, group, avatars, message=None, actor=None):
    actor = actor if actor is not None else sig.actor
    image = None
    if sig.kind in AVATAR_KINDS and avatars:
        image = avatars.get(actor)
    return {
        # avatar: the person behind it, for notifiers that can show them
        # apart from the image (nudge). Others show the avatar as the image.
        "avatar": image, "sender": actor_name(actor) if image else None,
        # thread: one Notification Center stack per MR (or branch), across kinds.
        "kind": sig.kind, "group": "gitlab-notify:" + group,
        "thread": "gitlab-notify:" + sig.key, "title": sig.title,
        "subtitle": sig.subtitle, "message": message or sig.message, "url": sig.url,
        "image": image or icon(sig.kind), "context": sig.ctx,
    }


def digest_comments(sig, state, window_ms, replace):
    """Fold a comment into its MR's digest; the notification text, or None.

    With in-place replacement, the first comment notifies at once and each
    one after it within `window_ms` of the last replaces it with a running
    count. Without it, the first notifies and the rest are held until a
    comment arrives `window_ms` after that notification, which then carries
    the held ones too. No timer: the next batch moves the digest along.
    """
    m = state["mrs"].setdefault(sig.key, {})
    m["updated_ms"] = max(sig.ts, m.get("updated_ms", 0))
    d = m.get("digest")
    author = actor_name(sig.actor)
    if replace:
        if not (d and sig.ts - d["last_ms"] <= window_ms):
            d = m["digest"] = {"count": 0, "authors": []}
        d["last_ms"] = sig.ts
        d["count"] += 1
        add_author(d["authors"], author)
        count, authors = d["count"], d["authors"]
    else:
        if d and sig.ts - d["notified_ms"] < window_ms:
            d["held"] += 1
            add_author(d["held_authors"], author)
            return None
        count = (d["held"] if d else 0) + 1
        authors = add_author(list(d["held_authors"]) if d else [], author)
        m["digest"] = {"notified_ms": sig.ts, "held": 0, "held_authors": []}
    if count == 1:
        return sig.message
    names = ", ".join(authors[:3]) + (" +%d" % (len(authors) - 3) if len(authors) > 3 else "")
    return "%d new comments (%s)" % (count, names)


def add_author(authors, name):
    if name not in authors:
        authors.append(name)
    return authors


def deliver_live(signals, state, notifier, avatars, window_ms):
    """Send live signals. Comments digest per MR; the rest notify on their own.

    Within one batch only the last notification per group goes out, so a
    burst on one MR replaces rather than stacks.
    """
    pending = {}
    replace = bool(notifier and notifier.replace)
    for sig in signals:
        if sig.kind == "comments":
            msg = digest_comments(sig, state, window_ms, replace)
            if msg is None:
                continue
            group = sig.key + ":comments"
            if not replace:
                group += ":%d" % sig.ts
            pending.pop(group, None)
            pending[group] = (sig, msg)
        else:
            group = "%s:%s" % (sig.key, sig.kind)
            if sig.kind == "pipeline_failed" or not replace:
                group += ":%d" % sig.ts
            pending.pop(group, None)
            pending[group] = (sig, None)
    out = []
    for group, (sig, msg) in pending.items():
        out.append(notification(sig, group, avatars, msg))
    return out


def summarize(signals):
    """One backfill notification for the whole batch, or None."""
    if not signals:
        return None
    counts = {}
    for s in signals:
        counts[s.kind] = counts.get(s.kind, 0) + 1
    parts = [plural(counts[k], one, many) for k, one, many in SUMMARY_LABELS if counts.get(k)]
    comments = [s for s in signals if s.kind == "comments"]
    if comments:
        mrs = len({s.key for s in comments})
        parts.append("%s on %s" % (plural(len(comments), "comment", "comments"),
                                   plural(mrs, "MR", "MRs")))
    mrs = len({s.key for s in signals})
    worst = next((k for k in ("pipeline_failed", "conflict", "changes_requested")
                  if counts.get(k)), "comments")
    return {
        "kind": "summary", "group": "gitlab-notify:backfill",
        "title": "GitLab: caught up",
        "subtitle": "%s across %s" % (plural(len(signals), "update", "updates"),
                                      plural(mrs, "MR", "MRs")),
        "message": ", ".join(parts), "url": None, "image": icon(worst),
    }


# --- main ------------------------------------------------------------------------


def read_envelopes(stream):
    envs = []
    for n, line in enumerate(stream, 1):
        line = line.strip()
        if not line:
            continue
        try:
            env = json.loads(line)
        except ValueError as e:
            log("line %d: not JSON (%s), skipped" % (n, e))
            continue
        if isinstance(env, dict):
            envs.append(env)
    envs.sort(key=lambda e: e.get("received_at_ms") or 0)
    return envs


def parse_duration(s):
    s = str(s).strip()
    for suffix, mult in (("ms", 0.001), ("s", 1), ("m", 60), ("h", 3600), ("d", 86400)):
        if s.endswith(suffix) and s[: -len(suffix)].replace(".", "", 1).isdigit():
            return float(s[: -len(suffix)]) * mult
    return float(s)


def default_dir(env, fallback):
    return os.path.join(os.environ.get(env) or os.path.expanduser(fallback), "gitlab-notify")


def main(argv=None, stdin=None, notifier=None, now_ms=None):
    p = argparse.ArgumentParser(description="Desktop notifications for GitLab webhooks.")
    p.add_argument("--dry-run", action="store_true",
                   help="print notifications instead of sending; writes state only "
                        "to an explicit --state-dir")
    p.add_argument("--phase", choices=("backfill", "live"),
                   help="override $HOOKSTR_PHASE (default live)")
    p.add_argument("--notifier", default="auto",
                   choices=("auto", "nudge", "terminal-notifier", "osascript", "notify-send", "none"))
    p.add_argument("--me", help="your GitLab username: skip your own comments and approvals")
    p.add_argument("--window", default="60s",
                   help="per-MR comment digest quiet window (default 60s)")
    p.add_argument("--max-age", default="24h",
                   help="don't notify events received longer ago than this (default 24h)")
    p.add_argument("--push", action="store_true",
                   help="also notify pushes to a project's default branch")
    p.add_argument("--no-avatars", action="store_true", help="never fetch avatars")
    p.add_argument("--budget", default="40s",
                   help="stop sending after this long so the batch acks before "
                        "--exec-timeout (default 40s)")
    p.add_argument("--state-dir")
    p.add_argument("--cache-dir", default=default_dir("XDG_CACHE_HOME", "~/.cache"))
    args = p.parse_args(argv)

    start = time.time()
    now_ms = now_ms if now_ms is not None else int(start * 1000)
    phase = args.phase or os.environ.get("HOOKSTR_PHASE") or "live"
    window_ms = int(parse_duration(args.window) * 1000)
    max_age_ms = int(parse_duration(args.max_age) * 1000)
    deadline = start + parse_duration(args.budget)

    # A dry run reads the real state but only writes a state dir it was given.
    save = not args.dry_run or args.state_dir is not None
    state_dir = args.state_dir or default_dir("XDG_STATE_HOME", "~/.local/state")
    state_path = os.path.join(state_dir, "state.json")
    state = load_state(state_path)
    if notifier is None:
        notifier = pick_notifier(args.notifier)
        if args.dry_run:
            notifier = Printer(notifier)
        elif notifier is None:
            log("no notifier available; printing instead")
            notifier = Printer(None)
    avatars = Avatars(args.cache_dir, fetch=not (args.dry_run or args.no_avatars),
                      deadline=deadline)

    proc = Processor(state, me=args.me, push=args.push)
    signals = []
    envs = read_envelopes(stdin if stdin is not None else sys.stdin)
    for env in envs:
        try:
            sigs = proc.handle(env)
        except Exception as e:  # one odd hook never fails the batch
            log("event %s: %s: %s, skipped" % (env.get("id"), type(e).__name__, e))
            continue
        # State still learns from old events; they just don't notify.
        signals.extend(s for s in sigs if now_ms - s.ts <= max_age_ms)

    if phase == "backfill":
        n = summarize(signals)
        notes = [n] if n else []
    else:
        notes = deliver_live(signals, state, notifier, avatars, window_ms)
    attach_actions(notes, load_actions(actions_path()))

    for i, n in enumerate(notes):
        if time.time() > deadline:
            log("over budget, %d notifications dropped" % (len(notes) - i))
            break
        try:
            notifier.send(n, state)
        except Exception as e:
            log("notify failed: %s: %s" % (type(e).__name__, e))

    if save:
        try:
            save_state(state_path, state, now_ms)
        except OSError as e:
            log("could not save state: %s" % e)
    log("%s: %d events, %d signals, %d notifications"
        % (phase, len(envs), len(signals), len(notes)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
