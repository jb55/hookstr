// GitLabNotify: gitlab-notify's macOS notifier. Posting through
// UNUserNotificationCenter from a bundle of our own is what gets our own
// left-hand icon; terminal-notifier always shows its own.
//
//   GitLabNotify.app/Contents/MacOS/GitLabNotify --title T --message M
//       [--subtitle S] [--url U] [--image PNG] [--group G]
//   GitLabNotify.app/Contents/MacOS/GitLabNotify --authorize
//
// Posting exits once the notification is added. The request's identifier
// is the group, so a later post with the same group replaces it in place.
// When the user clicks one, macOS launches the app again with no
// arguments and hands the delegate the response: it opens the url and
// exits. Exit status: 0 posted, 2 not allowed, 3 failed, 64 usage.

import AppKit
import UserNotifications

struct Post {
    var title = "", subtitle = "", message = "", url = "", image = "", group = ""
}

enum Mode {
    case post(Post)
    case authorize
    case launched  // by macOS, for a click
}

func fail(_ code: Int32, _ msg: String) -> Never {
    FileHandle.standardError.write(("GitLabNotify: " + msg + "\n").data(using: .utf8)!)
    exit(code)
}

func parse(_ args: [String]) -> Mode {
    if args.isEmpty { return .launched }
    if args == ["--authorize"] { return .authorize }
    var p = Post()
    var i = 0
    while i < args.count {
        guard i + 1 < args.count else { fail(64, "missing value for \(args[i])") }
        let v = args[i + 1]
        switch args[i] {
        case "--title": p.title = v
        case "--subtitle": p.subtitle = v
        case "--message": p.message = v
        case "--url": p.url = v
        case "--image": p.image = v
        case "--group": p.group = v
        default: fail(64, "unknown argument \(args[i])")
        }
        i += 2
    }
    if p.title.isEmpty && p.message.isEmpty { fail(64, "need --title or --message") }
    return .post(p)
}

// Older LaunchServices passes a -psn_ argument on launch; it isn't ours.
let args = Array(CommandLine.arguments.dropFirst()).filter { !$0.hasPrefix("-psn_") }
let mode = parse(args)
let center = UNUserNotificationCenter.current()

final class Delegate: NSObject, NSApplicationDelegate, UNUserNotificationCenterDelegate {
    // The delegate must be set before launch finishes, or a click that
    // launched us is never delivered.
    func applicationWillFinishLaunching(_ note: Notification) {
        center.delegate = self
    }

    func applicationDidFinishLaunching(_ note: Notification) {
        switch mode {
        case .post(let p):
            // A first run waits on the permission prompt; the script's own
            // timeout is longer than this.
            exitAfter(8, code: 3, "timed out (waiting on the notification permission prompt?)")
            authorized { self.add(p) }
        case .authorize:
            authorized { exit(0) }
        case .launched:
            // Launched for a click: the response arrives right after launch.
            // Launched by hand: nothing arrives.
            exitAfter(5, code: 0, nil)
        }
    }

    func authorized(_ then: @escaping () -> Void) {
        center.getNotificationSettings { s in
            switch s.authorizationStatus {
            case .notDetermined:
                center.requestAuthorization(options: [.alert, .sound]) { ok, err in
                    if ok { then() } else { fail(2, "notifications not allowed: \(err?.localizedDescription ?? "denied")") }
                }
            case .denied:
                fail(2, "notifications are off for GitLabNotify (System Settings > Notifications)")
            default:
                then()
            }
        }
    }

    func add(_ p: Post) {
        let c = UNMutableNotificationContent()
        c.title = p.title
        c.subtitle = p.subtitle
        c.body = p.message
        if !p.url.isEmpty { c.userInfo = ["url": p.url] }
        if !p.group.isEmpty { c.threadIdentifier = p.group }
        if !p.image.isEmpty {
            // Adding an attachment moves its file into the notification
            // store, so hand it a copy, never the caller's file.
            do {
                let src = URL(fileURLWithPath: p.image)
                let tmp = FileManager.default.temporaryDirectory
                    .appendingPathComponent(UUID().uuidString)
                    .appendingPathExtension(src.pathExtension.isEmpty ? "png" : src.pathExtension)
                try FileManager.default.copyItem(at: src, to: tmp)
                c.attachments = [try UNNotificationAttachment(identifier: "image", url: tmp)]
            } catch {
                FileHandle.standardError.write("GitLabNotify: image skipped: \(error.localizedDescription)\n".data(using: .utf8)!)
            }
        }
        let id = p.group.isEmpty ? UUID().uuidString : p.group
        center.add(UNNotificationRequest(identifier: id, content: c, trigger: nil)) { err in
            if let err { fail(3, "post failed: \(err.localizedDescription)") }
            // Linger so a banner shown while we're frontmost-eligible
            // (willPresent below) isn't cut off by our exit.
            self.exitAfter(0.5, code: 0, nil)
        }
    }

    // We're running when our own notification is posted, which macOS
    // treats as the app being in the foreground: ask for the banner anyway.
    func userNotificationCenter(_ c: UNUserNotificationCenter, willPresent n: UNNotification,
                                withCompletionHandler done: @escaping (UNNotificationPresentationOptions) -> Void) {
        done([.banner, .list])
    }

    func userNotificationCenter(_ c: UNUserNotificationCenter, didReceive r: UNNotificationResponse,
                                withCompletionHandler done: @escaping () -> Void) {
        if r.actionIdentifier == UNNotificationDefaultActionIdentifier,
           let s = r.notification.request.content.userInfo["url"] as? String,
           let url = URL(string: s) {
            NSWorkspace.shared.open(url)
        }
        done()
        exitAfter(0.2, code: 0, nil)
    }

    func exitAfter(_ secs: Double, code: Int32, _ msg: String?) {
        DispatchQueue.main.asyncAfter(deadline: .now() + secs) {
            if let msg { fail(code, msg) }
            exit(code)
        }
    }
}

let app = NSApplication.shared
let delegate = Delegate()
app.delegate = delegate
app.setActivationPolicy(.accessory)
app.run()
