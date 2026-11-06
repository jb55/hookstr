"""Tests for gitlab-notify.py. All payloads here are synthetic.

    python3 -m unittest discover -s contrib/gitlab-notify
"""

import importlib.machinery
import importlib.util
import io
import json
import os
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
_loader = importlib.machinery.SourceFileLoader("gitlab_notify", os.path.join(HERE, "gitlab-notify.py"))
_spec = importlib.util.spec_from_loader("gitlab_notify", _loader)
gn = importlib.util.module_from_spec(_spec)
_loader.exec_module(gn)
gn.log = lambda msg: None

T0 = 1_700_000_000_000
PROJECT = {"id": 1, "name": "widgets", "path_with_namespace": "acme/widgets",
           "default_branch": "main", "web_url": "https://git.example/acme/widgets"}
ALICE = {"id": 11, "name": "Alice", "username": "alice", "avatar_url": "https://avatars.example/11"}
BOB = {"id": 12, "name": "Bob", "username": "bob", "avatar_url": "https://avatars.example/12"}
_n = [0]


def env(event, body, ts=T0, uuid=None):
    _n[0] += 1
    return {"id": "%064x" % _n[0], "path": "gitlab", "received_at_ms": ts, "provider": "gitlab",
            "headers": {"x-gitlab-event": event, "x-gitlab-event-uuid": uuid or "uuid-%d" % _n[0]},
            "body": body, "body_encoding": "json"}


def mr_attrs(iid=7, **kw):
    a = {"iid": iid, "title": "Add sprockets", "state": "opened", "target_branch": "main",
         "source_branch": "sprockets",
         "url": "https://git.example/acme/widgets/-/merge_requests/%d" % iid,
         "detailed_merge_status": "not_approved", "merge_status": "can_be_merged"}
    a.update(kw)
    return a


def mr_hook(action, user=ALICE, changes=None, ts=T0, **kw):
    oa = mr_attrs(**kw)
    oa["action"] = action
    return env("Merge Request Hook", {"object_kind": "merge_request", "user": user,
                                      "project": PROJECT, "object_attributes": oa,
                                      "changes": changes or {}}, ts)


def pipeline_hook(status, pid=100, source="merge_request_event", builds=(), ts=T0, mr=None, ref="feature"):
    body = {"object_kind": "pipeline", "project": PROJECT, "user": ALICE,
            "object_attributes": {"id": pid, "status": status, "source": source, "ref": ref,
                                  "url": "https://git.example/p/%d" % pid},
            "builds": list(builds)}
    if source != "push":
        body["merge_request"] = mr or mr_attrs()
    return env("Pipeline Hook", body, ts)


def note_hook(text="looks good", user=BOB, action="create", ts=T0, iid=7, ntype=None, **mr_kw):
    return env("Note Hook", {"object_kind": "note", "user": user, "project": PROJECT,
                             "object_attributes": {"action": action, "noteable_type": "MergeRequest",
                                                   "note": text, "type": ntype, "system": False,
                                                   "url": "https://git.example/n"},
                             "merge_request": mr_attrs(iid=iid, **mr_kw)}, ts)


class Harness:
    def __init__(self, replace=True, extra=()):
        self.dir = tempfile.mkdtemp()
        self.notifier = gn.Collector(replace=replace)
        self.extra = list(extra)

    def run(self, envs, phase="live", now=None, extra=()):
        stdin = io.StringIO("".join(json.dumps(e) + "\n" for e in envs))
        before = len(self.notifier.sent)
        rc = gn.main(["--phase", phase, "--state-dir", self.dir, "--cache-dir", self.dir,
                      "--no-avatars"] + self.extra + list(extra),
                     stdin=stdin, notifier=self.notifier,
                     now_ms=now if now is not None else max([e["received_at_ms"] for e in envs] + [T0]))
        assert rc == 0
        return self.notifier.sent[before:]


def kinds(notes):
    return [n["kind"] for n in notes]


class PipelineTests(unittest.TestCase):
    def test_failed_names_failed_jobs_and_skips_allowed_failures(self):
        h = Harness()
        out = h.run([pipeline_hook("failed", builds=[
            {"name": "unit", "stage": "test", "status": "failed", "failure_reason": "script_failure"},
            {"name": "lint", "stage": "test", "status": "failed", "allow_failure": True},
            {"name": "build", "stage": "build", "status": "success"}])])
        self.assertEqual(kinds(out), ["pipeline_failed"])
        self.assertEqual(out[0]["message"], "Pipeline failed: unit (script_failure)")
        self.assertEqual(out[0]["title"], "widgets !7")
        self.assertTrue(out[0]["image"].endswith("icons/pipeline_failed.png"))

    def test_running_and_child_pipelines_are_ignored(self):
        h = Harness()
        self.assertEqual(h.run([pipeline_hook("running"),
                                pipeline_hook("failed", pid=101, source="parent_pipeline")]), [])

    def test_failed_then_success_says_fixed(self):
        h = Harness()
        h.run([pipeline_hook("failed", pid=1)])
        out = h.run([pipeline_hook("success", pid=2, ts=T0 + 1000)])
        self.assertEqual([n["message"] for n in out], ["Pipeline fixed"])

    def test_same_pipeline_status_twice_notifies_once(self):
        h = Harness()
        h.run([pipeline_hook("failed", pid=1)])
        self.assertEqual(h.run([pipeline_hook("failed", pid=1, ts=T0 + 5)]), [])

    def test_default_branch_push_pipeline_success_only_after_failure(self):
        h = Harness()
        self.assertEqual(h.run([pipeline_hook("success", pid=1, source="push", ref="main")]), [])
        self.assertEqual(kinds(h.run([pipeline_hook("failed", pid=2, source="push", ref="main", ts=T0 + 1)])),
                         ["pipeline_failed"])
        self.assertEqual(kinds(h.run([pipeline_hook("success", pid=3, source="push", ref="main", ts=T0 + 2)])),
                         ["pipeline_passed"])
        self.assertEqual(h.run([pipeline_hook("failed", pid=4, source="push", ref="feature", ts=T0 + 3)]), [])


class MergeRequestTests(unittest.TestCase):
    def test_lifecycle_actions(self):
        h = Harness()
        out = h.run([mr_hook("open", ts=T0), mr_hook("approved", user=BOB, ts=T0 + 1),
                     mr_hook("merge", user=BOB, ts=T0 + 2, iid=8), mr_hook("close", iid=9, ts=T0 + 3)])
        self.assertEqual(kinds(out), ["new_mr", "approved", "merged", "closed"])
        self.assertEqual(out[1]["message"], "Approved by Bob")

    def test_review_requested_names_only_added_reviewers(self):
        h = Harness()
        out = h.run([mr_hook("update", changes={"reviewers": {
            "previous": [{"id": 11, "name": "Alice"}],
            "current": [{"id": 11, "name": "Alice"}, {"id": 12, "name": "Bob"}]}})])
        self.assertEqual([n["message"] for n in out], ["Review requested: Bob"])

    def test_conflict_is_a_transition_and_survives_unchecked(self):
        h = Harness()
        self.assertEqual(h.run([mr_hook("update", ts=T0)]), [])
        out = h.run([mr_hook("update", ts=T0 + 1, detailed_merge_status="conflict")])
        self.assertEqual(kinds(out), ["conflict"])
        # A recheck in between, then a description edit that still says conflict.
        self.assertEqual(h.run([mr_hook("update", ts=T0 + 2, detailed_merge_status="unchecked",
                                        merge_status="unchecked"),
                                mr_hook("update", ts=T0 + 3, detailed_merge_status="conflict")]), [])
        # Resolved, then conflicting again: a new conflict.
        h.run([mr_hook("update", ts=T0 + 4, detailed_merge_status="mergeable")])
        self.assertEqual(kinds(h.run([mr_hook("update", ts=T0 + 5, merge_status="cannot_be_merged")])),
                         ["conflict"])

    def test_changes_requested_seen_via_note_hook(self):
        h = Harness()
        out = h.run([note_hook(action="update", detailed_merge_status="requested_changes")])
        self.assertEqual(kinds(out), ["changes_requested"])
        self.assertEqual(h.run([note_hook(action="update", ts=T0 + 1,
                                          detailed_merge_status="requested_changes")]), [])

    def test_closed_mr_status_is_not_tracked(self):
        h = Harness()
        self.assertEqual(h.run([mr_hook("update", state="merged", detailed_merge_status="conflict")]), [])

    def test_me_skips_own_approval_and_comments(self):
        h = Harness(extra=["--me", "alice"])
        self.assertEqual(h.run([mr_hook("approved", user=ALICE), note_hook(user=ALICE)]), [])


class CommentDigestTests(unittest.TestCase):
    def test_note_updates_and_other_noteables_are_ignored(self):
        h = Harness()
        issue_note = note_hook()
        issue_note["body"]["object_attributes"]["noteable_type"] = "Issue"
        self.assertEqual(h.run([note_hook(action="update"), issue_note]), [])

    def test_replace_mode_burst_in_one_batch_is_one_notification(self):
        h = Harness(replace=True)
        out = h.run([note_hook(user=BOB, ts=T0), note_hook(user=ALICE, ts=T0 + 1),
                     note_hook(user=BOB, ts=T0 + 2)])
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["message"], "3 new comments (Bob, Alice)")
        self.assertEqual(out[0]["group"], "gitlab-notify:acme/widgets!7:comments")

    def test_replace_mode_digest_spans_batches_until_quiet(self):
        h = Harness(replace=True)
        first = h.run([note_hook(text="first point\nmore", ts=T0)])
        self.assertEqual(first[0]["message"], "Bob: first point")
        second = h.run([note_hook(ts=T0 + 30_000)])
        self.assertEqual(second[0]["message"], "2 new comments (Bob)")
        self.assertEqual(first[0]["group"], second[0]["group"])
        # 60s quiet: a fresh digest.
        third = h.run([note_hook(text="again", ts=T0 + 30_000 + 61_000)])
        self.assertEqual(third[0]["message"], "Bob: again")

    def test_cooldown_mode_holds_then_folds(self):
        h = Harness(replace=False)
        self.assertEqual(len(h.run([note_hook(ts=T0)])), 1)
        self.assertEqual(h.run([note_hook(user=ALICE, ts=T0 + 10_000), note_hook(ts=T0 + 20_000)]), [])
        out = h.run([note_hook(user=ALICE, ts=T0 + 70_000)])
        self.assertEqual([n["message"] for n in out], ["3 new comments (Alice, Bob)"])

    def test_pipeline_failure_is_never_folded_into_a_digest(self):
        h = Harness(replace=True)
        out = h.run([note_hook(ts=T0), pipeline_hook("failed", ts=T0 + 1), note_hook(ts=T0 + 2)])
        self.assertEqual(sorted(kinds(out)), ["comments", "pipeline_failed"])

    def test_digests_are_per_mr(self):
        h = Harness(replace=True)
        out = h.run([note_hook(iid=1), note_hook(iid=2, ts=T0 + 1)])
        self.assertEqual(len(out), 2)


class BatchTests(unittest.TestCase):
    def test_dedupe_on_event_uuid_across_batches(self):
        h = Harness()
        e = pipeline_hook("failed")
        self.assertEqual(len(h.run([e])), 1)
        retry = dict(e, id="f" * 64)  # same GitLab delivery, different hookstr event
        self.assertEqual(h.run([retry]), [])

    def test_backfill_is_one_summary(self):
        h = Harness()
        out = h.run([pipeline_hook("failed", ts=T0), note_hook(iid=1, ts=T0 + 1),
                     note_hook(iid=2, ts=T0 + 2), mr_hook("merge", iid=3, ts=T0 + 3)],
                    phase="backfill")
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["kind"], "summary")
        self.assertEqual(out[0]["message"], "1 pipeline failed, 1 merged, 2 comments on 2 MRs")
        self.assertTrue(out[0]["image"].endswith("pipeline_failed.png"))

    def test_backfill_learns_state_without_notifying_old_events(self):
        h = Harness()
        day = 24 * 3600 * 1000
        out = h.run([mr_hook("update", detailed_merge_status="conflict", ts=T0)],
                    phase="backfill", now=T0 + 2 * day)
        self.assertEqual(out, [])
        # The conflict was learned, so it isn't news when live starts.
        self.assertEqual(h.run([mr_hook("update", detailed_merge_status="conflict",
                                        ts=T0 + 2 * day)]), [])

    def test_ignored_and_malformed_input_still_exits_zero(self):
        h = Harness()
        job = env("Job Hook", {"object_kind": "build"})
        raw = {"id": "0" * 64, "received_at_ms": T0, "headers": {}, "body": "aGk=",
               "body_encoding": "base64"}
        broken = pipeline_hook("failed")
        broken["body"]["builds"] = "not a list"
        stdin = io.StringIO("not json\n" + json.dumps(job) + "\n" + json.dumps(raw) + "\n"
                            + json.dumps(broken) + "\n")
        d = tempfile.mkdtemp()
        rc = gn.main(["--state-dir", d, "--cache-dir", d, "--no-avatars"], stdin=stdin,
                     notifier=gn.Collector(), now_ms=T0)
        self.assertEqual(rc, 0)

    def test_notifier_exception_does_not_fail_batch(self):
        class Boom(gn.Collector):
            def send(self, n, state):
                raise RuntimeError("no display")
        d = tempfile.mkdtemp()
        stdin = io.StringIO(json.dumps(pipeline_hook("failed")) + "\n")
        self.assertEqual(gn.main(["--state-dir", d, "--cache-dir", d, "--no-avatars"],
                                 stdin=stdin, notifier=Boom(), now_ms=T0), 0)
        # State was still saved: the delivery is deduped next time.
        with open(os.path.join(d, "state.json")) as f:
            self.assertEqual(len(json.load(f)["seen"]), 1)

    def test_dry_run_prints_and_writes_only_an_explicit_state_dir(self):
        d = tempfile.mkdtemp()
        os.environ["XDG_STATE_HOME"] = d
        try:
            out = io.StringIO()
            stdin = io.StringIO(json.dumps(pipeline_hook("failed")) + "\n")
            gn.main(["--dry-run", "--cache-dir", d], stdin=stdin,
                    notifier=gn.Printer(gn.Collector(), out=out), now_ms=T0)
        finally:
            del os.environ["XDG_STATE_HOME"]
        self.assertEqual(json.loads(out.getvalue())["kind"], "pipeline_failed")
        self.assertFalse(os.path.exists(os.path.join(d, "gitlab-notify", "state.json")))
        stdin = io.StringIO(json.dumps(pipeline_hook("failed")) + "\n")
        gn.main(["--dry-run", "--state-dir", d, "--cache-dir", d], stdin=stdin,
                notifier=gn.Printer(gn.Collector(), out=io.StringIO()), now_ms=T0)
        self.assertTrue(os.path.exists(os.path.join(d, "state.json")))


class AvatarTests(unittest.TestCase):
    def test_cached_avatar_used_and_failures_fall_back(self):
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "avatars"))
        open(os.path.join(d, "avatars", "11.png"), "wb").close()
        av = gn.Avatars(d, fetch=False)
        self.assertTrue(av.get(ALICE).endswith("avatars/11.png"))
        self.assertIsNone(av.get(BOB))
        self.assertIsNone(av.get({"id": "../x", "avatar_url": "https://a"}))

    def test_unreachable_avatar_marks_failure(self):
        d = tempfile.mkdtemp()
        av = gn.Avatars(d, timeout=0.5)
        self.assertIsNone(av.get({"id": 99, "avatar_url": "http://127.0.0.1:9/a.png"}))
        self.assertTrue(os.path.exists(os.path.join(d, "avatars", "99.fail")))

    def test_comment_uses_avatar_pipeline_uses_icon(self):
        d = tempfile.mkdtemp()
        os.makedirs(os.path.join(d, "avatars"))
        open(os.path.join(d, "avatars", "12.png"), "wb").close()
        av = gn.Avatars(d, fetch=False)
        sigs = gn.Processor(gn.empty_state()).handle(note_hook(user=BOB))
        n = gn.notification(sigs[-1], "g", av)
        self.assertTrue(n["image"].endswith("avatars/12.png"))
        sigs = gn.Processor(gn.empty_state()).handle(pipeline_hook("failed"))
        self.assertTrue(gn.notification(sigs[-1], "g", av)["image"].endswith("pipeline_failed.png"))


class ThreadTests(unittest.TestCase):
    def test_one_thread_per_mr_across_kinds(self):
        h = Harness()
        sent = h.run([pipeline_hook("failed"), note_hook(), mr_hook("approved"),
                      note_hook(iid=8)])
        threads = {n["kind"]: n["thread"] for n in sent if "!7" in n["group"]}
        self.assertEqual(set(threads), {"pipeline_failed", "comments", "approved"})
        self.assertEqual(set(threads.values()), {"gitlab-notify:acme/widgets!7"})
        self.assertEqual(len({n["group"] for n in sent if "!7" in n["group"]}), 3)
        self.assertIn("gitlab-notify:acme/widgets!8", [n["thread"] for n in sent])


def fake_helper(d, rc=0):
    """A stand-in for nudge that records its argv, one JSON line per run."""
    path = os.path.join(d, "Nudge")
    with open(path, "w") as f:
        f.write("#!/bin/sh\n"
                "python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' \"$@\" >> %s/argv\n"
                "echo boom >&2\nexit %d\n" % (d, rc))
    os.chmod(path, 0o755)
    return path


class AppNotifierTests(unittest.TestCase):
    def setUp(self):
        self.saved = (gn.nudge_bin, gn.sys.platform)

    def tearDown(self):
        gn.nudge_bin, gn.sys.platform = self.saved

    def test_passes_the_notification_as_flags(self):
        d = tempfile.mkdtemp()
        app = gn.Nudge(fake_helper(d))
        app.send({"kind": "pipeline_failed", "group": "gitlab-notify:g", "title": "T",
                  "subtitle": "S", "message": "M", "url": "https://git.example/u",
                  "image": "/i.png", "thread": "gitlab-notify:acme/widgets!7"}, gn.empty_state())
        app.send({"kind": "closed", "group": "gitlab-notify:h", "title": "T2",
                  "subtitle": None, "message": "M2", "url": None, "image": None},
                 gn.empty_state())
        with open(os.path.join(d, "argv")) as f:
            runs = [json.loads(line) for line in f]
        self.assertEqual(runs[0], ["--title", "T", "--message", "M", "--group", "gitlab-notify:g",
                                   "--subtitle", "S", "--url", "https://git.example/u",
                                   "--image", "/i.png", "--thread", "gitlab-notify:acme/widgets!7"])
        self.assertEqual(runs[1], ["--title", "T2", "--message", "M2", "--group", "gitlab-notify:h"])

    def test_helper_failure_never_fails_the_batch(self):
        d = tempfile.mkdtemp()
        logged = []
        gn.log, saved = logged.append, gn.log
        try:
            stdin = io.StringIO(json.dumps(pipeline_hook("failed")) + "\n")
            rc = gn.main(["--phase", "live", "--state-dir", d, "--cache-dir", d, "--no-avatars"],
                         stdin=stdin, notifier=gn.Nudge(fake_helper(d, rc=2)), now_ms=T0)
            gn.Nudge(os.path.join(d, "missing")).send(
                {"group": "g", "title": "T", "message": "M"}, gn.empty_state())
        finally:
            gn.log = saved
        self.assertEqual(rc, 0)
        self.assertTrue(any("exited 2: boom" in m for m in logged), logged)
        self.assertTrue(any("missing failed" in m for m in logged), logged)

    def test_auto_prefers_nudge_on_macos(self):
        d = tempfile.mkdtemp()
        gn.sys.platform = "darwin"
        helper = fake_helper(d)
        gn.nudge_bin = lambda: helper
        n = gn.pick_notifier("auto")
        self.assertIsInstance(n, gn.Nudge)
        self.assertEqual(n.path, helper)
        gn.nudge_bin = lambda: None
        self.assertNotIsInstance(gn.pick_notifier("auto"), gn.Nudge)
        gn.sys.platform = "linux"
        gn.nudge_bin = lambda: helper
        self.assertNotIsInstance(gn.pick_notifier("auto"), gn.Nudge)

    def test_finds_nudge_app_from_env_then_installed(self):
        d = tempfile.mkdtemp()
        app = os.path.join(d, "Nudge.app")
        os.makedirs(os.path.join(app, "Contents", "MacOS"))
        fake_helper(os.path.join(app, "Contents", "MacOS"))
        saved = (os.environ.get("NUDGE_APP"), gn.NUDGE_APPS)
        try:
            os.environ["NUDGE_APP"] = app
            gn.NUDGE_APPS = ()
            self.assertEqual(gn.nudge_bin(), os.path.join(app, "Contents", "MacOS", "Nudge"))
            os.environ["NUDGE_APP"] = os.path.join(d, "Missing.app")
            self.assertIsNone(gn.nudge_bin())
            gn.NUDGE_APPS = (app,)
            self.assertEqual(gn.nudge_bin(), os.path.join(app, "Contents", "MacOS", "Nudge"))
        finally:
            if saved[0] is None:
                os.environ.pop("NUDGE_APP", None)
            else:
                os.environ["NUDGE_APP"] = saved[0]
            gn.NUDGE_APPS = saved[1]


def fake_helper_without_actions(d):
    """A nudge built before action buttons: usage error on --action."""
    path = os.path.join(d, "OldNudge")
    with open(path, "w") as f:
        f.write("#!/bin/sh\n"
                "python3 -c 'import json,sys; print(json.dumps(sys.argv[1:]))' \"$@\" >> %s/argv\n"
                "for a in \"$@\"; do [ \"$a\" = --action ] && "
                "{ echo 'unknown argument --action' >&2; exit 64; }; done\nexit 0\n" % d)
    os.chmod(path, 0o755)
    return path


class ActionTests(unittest.TestCase):
    CONF = {
        "handle-review": {"label": "Handle Review", "kinds": ["comments", "changes_requested"],
                          "command": ["/usr/bin/true", "--flag"]},
        "retry": {"label": "Retry", "kinds": ["pipeline_failed"], "command": ["/usr/bin/true"]},
    }

    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.saved_env = os.environ.get("XDG_CONFIG_HOME")
        os.environ["XDG_CONFIG_HOME"] = self.dir
        self.logged = []
        self.saved_log, gn.log = gn.log, self.logged.append

    def tearDown(self):
        gn.log = self.saved_log
        if self.saved_env is None:
            os.environ.pop("XDG_CONFIG_HOME", None)
        else:
            os.environ["XDG_CONFIG_HOME"] = self.saved_env

    def write_conf(self, conf):
        os.makedirs(os.path.join(self.dir, "nudge"), exist_ok=True)
        with open(os.path.join(self.dir, "nudge", "actions.json"), "w") as f:
            f.write(conf if isinstance(conf, str) else json.dumps(conf))

    def test_matching_kinds_get_buttons_and_mr_context(self):
        self.write_conf(self.CONF)
        out = Harness().run([note_hook(), mr_hook("approved"), pipeline_hook("failed")])
        by_kind = {n["kind"]: n for n in out}
        self.assertEqual(by_kind["comments"]["actions"],
                         [{"id": "handle-review", "label": "Handle Review"}])
        self.assertEqual(by_kind["approved"]["actions"], [])
        self.assertEqual(by_kind["pipeline_failed"]["actions"], [{"id": "retry", "label": "Retry"}])
        self.assertEqual(by_kind["comments"]["context"], {
            "project": "acme/widgets", "iid": 7, "source_branch": "sprockets",
            "mr_url": "https://git.example/acme/widgets/-/merge_requests/7",
            "note_url": "https://git.example/n"})
        self.assertNotIn("note_url", by_kind["pipeline_failed"]["context"])

    def test_changes_requested_from_a_note_hook_has_no_note_url(self):
        self.write_conf(self.CONF)
        out = Harness().run([note_hook(action="update", detailed_merge_status="requested_changes")])
        self.assertEqual(kinds(out), ["changes_requested"])
        self.assertEqual(out[0]["actions"], [{"id": "handle-review", "label": "Handle Review"}])
        self.assertEqual(out[0]["context"]["iid"], 7)
        self.assertNotIn("note_url", out[0]["context"])

    def test_backfill_summary_gets_no_buttons(self):
        self.write_conf(self.CONF)
        out = Harness().run([note_hook()], phase="backfill")
        self.assertEqual(kinds(out), ["summary"])
        self.assertFalse(out[0].get("actions"))

    def test_missing_or_broken_config_still_notifies_without_buttons(self):
        out = Harness().run([note_hook()])
        self.assertEqual(out[0]["actions"], [])
        self.assertEqual([m for m in self.logged if "actions.json" in m], [])
        self.write_conf("{not json")
        out = Harness().run([note_hook()])
        self.assertEqual(kinds(out), ["comments"])
        self.assertEqual(out[0]["actions"], [])
        self.assertTrue(any("unreadable" in m for m in self.logged), self.logged)

    def test_bad_entries_are_skipped_and_logged(self):
        self.write_conf({
            "relative": {"label": "R", "kinds": ["comments"], "command": ["handle-review"]},
            "shell": {"label": "S", "kinds": ["comments"], "command": "/bin/sh -c 'x'"},
            "bad id!": {"label": "B", "kinds": ["comments"], "command": ["/usr/bin/true"]},
            "nolabel": {"kinds": ["comments"], "command": ["/usr/bin/true"]},
            "ok": {"label": "OK", "kinds": ["comments", "nonsense"], "command": ["/usr/bin/true"]},
        })
        actions = gn.load_actions(gn.actions_path())
        self.assertEqual([(a["id"], a["kinds"]) for a in actions], [("ok", ["comments"])])
        for aid in ("relative", "shell", "bad id!", "nolabel"):
            self.assertTrue(any(repr(aid) in m and "skipped" in m for m in self.logged), aid)
        self.assertTrue(any("unknown kinds nonsense" in m for m in self.logged), self.logged)

    def test_nudge_passes_buttons_and_context_as_flags(self):
        d = tempfile.mkdtemp()
        n = {"kind": "comments", "group": "gitlab-notify:g", "title": "T", "message": "M",
             "url": "https://git.example/n", "image": None,
             "actions": [{"id": "handle-review", "label": "Handle Review"}],
             "context": {"project": "acme/widgets", "iid": 7, "source_branch": "sprockets",
                         "mr_url": "https://git.example/mr/7", "note_url": None}}
        gn.Nudge(fake_helper(d)).send(n, gn.empty_state())
        gn.Nudge(fake_helper(d)).send(dict(n, actions=[]), gn.empty_state())
        with open(os.path.join(d, "argv")) as f:
            runs = [json.loads(line) for line in f]
        self.assertEqual(runs[0], [
            "--title", "T", "--message", "M", "--group", "gitlab-notify:g",
            "--url", "https://git.example/n", "--action", "handle-review=Handle Review",
            "--env", "GITLAB_NOTIFY_KIND=comments", "--env", "GITLAB_PROJECT=acme/widgets",
            "--env", "GITLAB_MR_IID=7", "--env", "GITLAB_SOURCE_BRANCH=sprockets",
            "--env", "GITLAB_MR_URL=https://git.example/mr/7"])
        # No buttons, no button flags: an older helper still takes it.
        self.assertEqual(runs[1], ["--title", "T", "--message", "M", "--group", "gitlab-notify:g",
                                   "--url", "https://git.example/n"])

    def test_helper_without_buttons_reposts_plain(self):
        d = tempfile.mkdtemp()
        n = {"kind": "comments", "group": "g", "title": "T", "message": "M",
             "actions": [{"id": "a", "label": "A"}], "context": {"project": "acme/widgets"}}
        gn.Nudge(fake_helper_without_actions(d)).send(n, gn.empty_state())
        with open(os.path.join(d, "argv")) as f:
            runs = [json.loads(line) for line in f]
        self.assertEqual(len(runs), 2)
        self.assertIn("--action", runs[0])
        self.assertEqual(runs[1], ["--title", "T", "--message", "M", "--group", "g"])
        self.assertTrue(any("update it" in m for m in self.logged), self.logged)
        self.assertFalse(any("exited" in m for m in self.logged), self.logged)


if __name__ == "__main__":
    unittest.main()
