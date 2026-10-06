"""跨重启持久化验证（无需 Flask）：真实进程重启 + SIGKILL 崩溃。

用两种方式驱动 app.py：

1. 在本进程内用最小 Flask stub（与 backend_compensate.py 同款）导入 app，
   完成一组操作后，用全新的 Experiment 实例按原 ID 重新打开同一库文件，
   验证已提交值与完整时间线（含 compensates 关联、序号接续）被恢复，
   而两栏连接状态（事务 / 只读 / 最近读值）与脚本、日志、检查点缓存
   一律不恢复；
2. 派生子进程真正运行 app.py（Flask 由路由测试桩在子进程内置入，无需
   第三方依赖），用 urllib 走 HTTP，在「未提交事务进行中」与「已提交后」
   两个时点 SIGKILL 强杀，再启动新进程按原 ID 访问，验证值与时间线只能
   处于「都生效 / 都未生效」之一：未提交被杀整笔消失，已提交的一笔
   （kv UPDATE 与 timeline INSERT 同事务）完整保留并可继续补偿。
"""
import json
import os
import re
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import types
import urllib.request
import urllib.error

# ---- 最小 Flask stub（与其它后端测试同款，仅 Web 框架管道） ----
flask = types.ModuleType("flask")


class _Resp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def get_json(self):
        return self._payload


def _jsonify(*args, **kwargs):
    if args and kwargs:
        raise TypeError("jsonify: cannot mix positional and keyword")
    if len(args) == 1:
        payload = args[0]
    elif args:
        payload = list(args)
    else:
        payload = kwargs
    return _Resp(payload)


def _compile_path(path):
    return re.compile("^" + re.sub(r"<(\w+)>", r"(?P<\1>[^/]+)", path) + "$")


class _App:
    def __init__(self, name):
        self.routes = []

    def _reg(self, method, path):
        def deco(fn):
            self.routes.append((method, _compile_path(path), fn))
            return fn
        return deco

    def get(self, path, **k):
        return self._reg("GET", path)

    def post(self, path, **k):
        return self._reg("POST", path)

    def delete(self, path, **k):
        return self._reg("DELETE", path)

    def run(self, host="0.0.0.0", port=8000, threaded=True, **k):
        from wsgiref.simple_server import make_server, WSGIRequestHandler

        class Quiet(WSGIRequestHandler):
            def log_message(self, *a):
                pass

        def wsgi_app(environ, start_response):
            method = environ["REQUEST_METHOD"]
            path = environ["PATH_INFO"]
            length = int(environ.get("CONTENT_LENGTH") or 0)
            raw = environ["wsgi.input"].read(length) if length else b""
            body = json.loads(raw.decode()) if raw else None
            flask.request._json = body
            try:
                for registered_method, rx, fn in self.routes:
                    if registered_method != method:
                        continue
                    match = rx.match(path)
                    if match:
                        rv = fn(**match.groupdict())
                        break
                else:
                    rv = (_jsonify({"error": "not found"}), 404)
                if isinstance(rv, tuple):
                    resp, status = rv
                else:
                    resp, status = rv, 200
                payload = json.dumps(resp._payload).encode()
                start_response(
                    "%d %s" % (status, "OK" if 200 <= status < 300 else "ERR"),
                    [("Content-Type", "application/json")],
                )
                return [payload]
            finally:
                flask.request._json = None

        httpd = make_server(host, port, wsgi_app, handler_class=Quiet)
        httpd.serve_forever()


class _Req(threading.local):
    def __init__(self):
        self._json = None

    def get_json(self, silent=False):
        return self._json


flask.Flask = _App
flask.jsonify = _jsonify
flask.render_template = lambda *a, **k: "html"
flask.request = _Req()
sys.modules["flask"] = flask

ROOT = os.path.dirname(os.path.abspath(__file__)) + "/.."
sys.path.insert(0, ROOT)

fails = []


def check(name, cond, extra=""):
    print(("  ok  " if cond else "  FAIL ") + name + (f"  [{extra}]" if extra and not cond else ""))
    if not cond:
        fails.append(name)


# =====================================================================
# 第一部分：进程内「重新打开同一库」= 重启后恢复的语义
# =====================================================================
data_dir = tempfile.mkdtemp()
os.environ["LAB_DATA_DIR"] = data_dir
import app as A

# 启动时目录为空
check("启动·空目录扫描到0个实验", A.discover_experiments() == 0)

exp_id = "abcdef012345"
exp = A.Experiment(exp_id)
with A.experiments_lock:
    A.experiments[exp_id] = exp


def op(e, side, opname, value=None):
    body = {"op": opname}
    if value is not None:
        body["value"] = value
    res = A.run_op(e, side, opname, value)
    assert res["ok"], res
    return res


# #1 甲 +5 +7 提交 100→112
op(exp, "a", "begin")
op(exp, "a", "add", 5)
op(exp, "a", "add", 7)
op(exp, "a", "commit")
# 甲再开事务读值（重启后必须消失）
op(exp, "a", "begin")
op(exp, "a", "read")
# 乙开启只读并开事务（重启后只读与未提交事务都必须消失）
assert A.run_set_readonly(exp, "b", True)["ok"]
op(exp, "b", "begin")
# 补偿 #1（乙持只读事务不影响独立补偿连接）：112→100，#2 system
result, conflict = A.run_compensate(exp, 1)
check("准备·补偿#1成功", conflict is None and result["ok"], str(conflict or result))
# 甲的读快照已过时：加法如实报 SQLITE_BUSY_SNAPSHOT(517)，自行回滚重开
res = A.run_op(exp, "a", "add", 9)
check("准备·过时快照加法报517", not res["ok"] and res["error"]["code"] == 517,
      str(res.get("error")))
op(exp, "a", "rollback")
# #3 甲新事务 +9 提交：100→109
op(exp, "a", "begin")
op(exp, "a", "add", 9)
op(exp, "a", "commit")
# 时间线：#1 a 100→112 [5,7]，#2 system 112→100 [-7,-5] comp=1，#3 a 100→109 [9]
tl_before = exp.timeline_records()
check("准备·时间线3条", len(tl_before) == 3, str(tl_before))

# 模拟重启：丢弃内存对象，用全新 Experiment 打开同一库文件
exp2 = A.Experiment(exp_id)
check("恢复·已提交值=109", exp2.committed_value() == 109, str(exp2.committed_value()))
tl = exp2.timeline_records()
check("恢复·时间线完整3条", len(tl) == 3, str(tl))
check(
    "恢复·记录内容与补偿关联完整",
    tl == [
        {"seq": 1, "side": "a", "before": 100, "after": 112, "deltas": [5, 7]},
        {"seq": 2, "side": "system", "before": 112, "after": 100,
         "deltas": [-7, -5], "compensates": 1},
        {"seq": 3, "side": "a", "before": 100, "after": 109, "deltas": [9]},
    ],
    str(tl),
)
# 两栏重建：无事务、只读关闭、最近读值为空、无日志
for side in ("a", "b"):
    st = exp2.panel_state(side)
    check(f"恢复·{side}栏无事务", st["in_transaction"] is False)
    check(f"恢复·{side}栏只读关闭", st["readonly"] is False)
    check(f"恢复·{side}栏最近读值为空", st["last_read"] is None)
    check(f"恢复·{side}栏操作日志不恢复", st["log"] == [])
check("恢复·脚本不恢复", exp2.script_state() is None)
check("恢复·检查点缓存不恢复", exp2.checkpoint_state() == {"last": None, "log": []})

# 恢复后继续编号：#4；末条普通记录 #3 仍可补偿
op(exp2, "b", "begin")
op(exp2, "b", "add", 2)
op(exp2, "b", "commit")
check("恢复·新提交编号接续#4",
      exp2.timeline_records()[-1] == {"seq": 4, "side": "b", "before": 109,
                                     "after": 111, "deltas": [2]},
      str(exp2.timeline_records()[-1]))
result, conflict = A.run_compensate(exp2, 4)
check("恢复·末条普通记录#4可补偿", conflict is None and result["ok"])
check("恢复·补偿记录#5且compensates=4",
      exp2.timeline_records()[-1]["seq"] == 5
      and exp2.timeline_records()[-1]["compensates"] == 4)
# 对已补偿的 #4 再补偿：409
result, conflict = A.run_compensate(exp2, 4)
check("恢复·补偿记录不可再补偿409", conflict is not None)

# 重启两次后仍然一致
exp3 = A.Experiment(exp_id)
check("二次重启·值109", exp3.committed_value() == 109)
check("二次重启·时间线5条", len(exp3.timeline_records()) == 5)

# ---- 成功加法后开启只读，提交仍成功，值与记录同时落地 ----
exp_ro = A.Experiment("111122223333")
op(exp_ro, "a", "begin")
op(exp_ro, "a", "add", 4)
assert A.run_set_readonly(exp_ro, "a", True)["ok"]
# 只读下再加法：SQLITE_READONLY(8)，失败不记录
res = A.run_op(exp_ro, "a", "add", 6)
check("只读·只读下加法失败8", not res["ok"] and res["error"]["code"] == 8,
      str(res.get("error")))
# 提交仍成功（提交路径临时解除只读写 timeline，COMMIT 后恢复只读）
res = A.run_op(exp_ro, "a", "commit", None)
check("只读·只读下提交成功", res["ok"], str(res))
check("只读·提交后开关仍开启", res["state"]["readonly"] is True)
check("只读·已提交值104（只记成功的+4）", exp_ro.committed_value() == 104)
tl = exp_ro.timeline_records()
check("只读·一条记录100→104 [4]",
      len(tl) == 1 and tl[0] == {"seq": 1, "side": "a", "before": 100,
                                "after": 104, "deltas": [4]}, str(tl))
# 开关在连接上确实仍为只读：再开事务写仍被拒
op(exp_ro, "a", "begin")
res = A.run_op(exp_ro, "a", "add", 1)
check("只读·提交后连接仍只读", not res["ok"] and res["error"]["code"] == 8)
op(exp_ro, "a", "rollback")
exp_ro4 = A.Experiment("111122223333")
check("只读·重启后只读关闭且值104记录在",
      exp_ro4.committed_value() == 104
      and exp_ro4.panel_state("a")["readonly"] is False
      and len(exp_ro4.timeline_records()) == 1)

# ---- 失败加法 / 纯读提交 / 回滚不产生记录；失败提交重试不重复 ----
exp_nr = A.Experiment("444455556666")
op(exp_nr, "a", "begin")
op(exp_nr, "a", "read")  # 纯读
op(exp_nr, "a", "commit")
op(exp_nr, "a", "begin")
op(exp_nr, "a", "rollback")  # 空事务回滚
# 乙持写锁，甲撞 BUSY 的加法失败
op(exp_nr, "b", "begin")
op(exp_nr, "b", "add", 10)
op(exp_nr, "a", "begin")
res = A.run_op(exp_nr, "a", "add", 1)
check("无记录·撞锁加法失败5", not res["ok"] and res["error"]["code"] == 5)
op(exp_nr, "a", "rollback")
op(exp_nr, "b", "commit")
check("无记录·时间线只有乙的1条",
      exp_nr.timeline_records() == [{"seq": 1, "side": "b", "before": 100,
                                    "after": 110, "deltas": [10]}])
# COMMIT 失败重试不重复：白盒——用连接代理让第一次 COMMIT 抛 BUSY，
# 事务保持开启（sqlite3.Connection 不允许直接覆写 execute）
conn_a = exp_nr.conns["a"]
op(exp_nr, "a", "begin")
op(exp_nr, "a", "add", 2)
state = {"n": 0}


class FlakyConn:
    def __init__(self, real):
        self._real = real

    def execute(self, sql, *a, **k):
        if sql == "COMMIT":
            state["n"] += 1
            if state["n"] == 1:
                raise sqlite3.OperationalError("database is locked")
        return self._real.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._real, name)


exp_nr.conns["a"] = FlakyConn(conn_a)
try:
    res = A.run_op(exp_nr, "a", "commit", None)
    check("重试·首次COMMIT失败", not res["ok"])
    check("重试·失败后事务仍在", conn_a.in_transaction)
    res = A.run_op(exp_nr, "a", "commit", None)
    check("重试·重试COMMIT成功", res["ok"])
finally:
    exp_nr.conns["a"] = conn_a
tl = exp_nr.timeline_records()
check("重试·只追加一条#2 110→112 [2]",
      len(tl) == 2 and tl[-1] == {"seq": 2, "side": "a", "before": 110,
                                 "after": 112, "deltas": [2]}, str(tl))
exp_nr2 = A.Experiment("444455556666")
check("重试·重启后仍只有2条且值112",
      len(exp_nr2.timeline_records()) == 2 and exp_nr2.committed_value() == 112)

# ---- 旧版库（只有 kv 表、现值非 100）：保留现值、空时间线基线 ----
legacy_id = "777788889999"
legacy_path = os.path.join(data_dir, "exp_%s.db" % legacy_id)
lc = __import__("sqlite3").connect(legacy_path)
lc.execute("PRAGMA journal_mode=WAL")
lc.execute("CREATE TABLE kv (id INTEGER PRIMARY KEY CHECK (id = 1), val INTEGER NOT NULL)")
lc.execute("INSERT INTO kv (id, val) VALUES (1, 777)")
lc.commit()
lc.close()
legacy = A.Experiment(legacy_id)
check("旧版·保留现值777", legacy.committed_value() == 777)
check("旧版·空时间线基线", legacy.timeline_records() == [])
op(legacy, "a", "begin")
op(legacy, "a", "add", 1)
op(legacy, "a", "commit")
check("旧版·首条记录从#1、before=777",
      legacy.timeline_records() == [{"seq": 1, "side": "a", "before": 777,
                                    "after": 778, "deltas": [1]}])

# ---- 删除的实验不复活 ----
del_id = "aaaabbbbcccc"
dexp = A.Experiment(del_id)
with A.experiments_lock:
    A.experiments[del_id] = dexp
    # 与 DELETE 路由一致：先立墓碑（本次运行 410），再等待并拆除
    A.deleted_experiments.add(del_id)
assert dexp.claim_or_wait_delete()
dexp.begin_delete()
dexp.teardown()
check("删除·库文件已移除", not os.path.exists(os.path.join(data_dir, "exp_%s.db" % del_id)))
ghost = A.load_experiment_from_disk(del_id)
check("删除·按ID加载返回None（不复活）", ghost is None)

# =====================================================================
# 第二部分：真实子进程 + SIGKILL + 重启，按原 ID 走 HTTP
# =====================================================================
APP_PY = os.path.join(ROOT, "app.py")
SERVER_PRELUDE = """
import json, os, re, sys, threading, types
# 内置最小 Flask（无需第三方包），让 app.py 的路由在 wsgiref 上提供 HTTP
flask = types.ModuleType("flask")
class _Resp:
    def __init__(self, p, s=200): self._payload, self.status_code = p, s
    def get_json(self): return self._payload
def _jsonify(*a, **k):
    p = a[0] if len(a)==1 else (list(a) if a else k)
    return _Resp(p)
def _cp(path):
    return re.compile("^" + re.sub(r"<(\\w+)>", r"(?P<\\1>[^/]+)", path) + "$")
class _App:
    def __init__(self, n): self.routes=[]
    def _reg(self, m, p):
        def deco(fn): self.routes.append((m, _cp(p), fn)); return fn
        return deco
    def get(self, p, **k): return self._reg("GET", p)
    def post(self, p, **k): return self._reg("POST", p)
    def delete(self, p, **k): return self._reg("DELETE", p)
    def run(self, host="0.0.0.0", port=8000, threaded=True, **k):
        from wsgiref.simple_server import make_server, WSGIRequestHandler
        class Q(WSGIRequestHandler):
            def log_message(self, *a): pass
        def app(environ, start_response):
            raw = environ["wsgi.input"].read(int(environ.get("CONTENT_LENGTH") or 0))
            flask.request._json = json.loads(raw.decode()) if raw else None
            try:
                for m, rx, fn in self.routes:
                    if m == environ["REQUEST_METHOD"]:
                        mt = rx.match(environ["PATH_INFO"])
                        if mt:
                            rv = fn(**mt.groupdict()); break
                else:
                    rv = (_jsonify({"error":"nf"}), 404)
                if isinstance(rv, tuple): resp, st = rv
                else: resp, st = rv, 200
                payload = json.dumps(resp._payload).encode()
                start_response("%d X" % st, [("Content-Type","application/json")])
                return [payload]
            finally:
                flask.request._json = None
        make_server(host, port, app, handler_class=Q).serve_forever()
class _Req(threading.local):
    def __init__(self): self._json=None
    def get_json(self, silent=False): return self._json
flask.Flask=_App; flask.jsonify=_jsonify
flask.render_template=lambda *a, **k:"html"; flask.request=_Req()
sys.modules["flask"]=flask
"""


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start_server(proc_dir, port):
    script = (
        SERVER_PRELUDE
        + "import os; os.environ['LAB_DATA_DIR']=%r\n" % proc_dir
        + "import runpy, sys\n"
        + "sys.path.insert(0, %r)\n" % ROOT
        + "import app\napp.app.run(port=%d)\n" % port
    )
    return subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


def wait_http(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), 0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def http(method, url, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def op_http(base, side, opname, value=None):
    body = {"op": opname}
    if value is not None:
        body["value"] = value
    return http("POST", "%s/conn/%s/op" % (base, side), body)


proc_dir = tempfile.mkdtemp()
port = free_port()
proc = start_server(proc_dir, port)
try:
    check("子进程·服务启动", wait_http(port))
    time.sleep(0.3)
    st, body = http("POST", "http://127.0.0.1:%d/api/experiments" % port)
    pid = body["id"]
    base = "http://127.0.0.1:%d/api/experiments/%s" % (port, pid)
    check("子进程·新建实验", st == 200 and re.fullmatch(r"[0-9a-f]{12}", pid))

    # 乙先提交一笔 +20（必须完整保留：值与时间线同事务同时落地）
    assert op_http(base, "b", "begin")[0] == 200
    assert op_http(base, "b", "add", 20)[0] == 200
    assert op_http(base, "b", "commit")[0] == 200
    # 然后甲开未提交事务 +5：单一写者下未提交内容必须随 SIGKILL 整笔消失
    assert op_http(base, "a", "begin")[0] == 200
    assert op_http(base, "a", "add", 5)[0] == 200
    op_http(base, "a", "read")
    st, body = http("GET", "%s/committed" % base)
    check("强杀前·已提交值120", st == 200 and body["committed"] == 120, str(body))

    proc.send_signal(signal.SIGKILL)
    proc.wait()
    check("强杀·进程被SIGKILL终止", proc.returncode == -signal.SIGKILL)

    # 重启，按原 ID 访问
    port2 = free_port()
    proc = start_server(proc_dir, port2)
    try:
        check("强杀后·服务重启", wait_http(port2))
        time.sleep(0.3)
        base2 = "http://127.0.0.1:%d/api/experiments/%s" % (port2, pid)
        st, body = http("GET", "%s/committed" % base2)
        check("强杀后·原ID可访问且值=120（未提交的+5消失）",
              st == 200 and body["committed"] == 120, str(body))
        st, body = http("GET", "%s/timeline" % base2)
        check("强杀后·时间线只有乙的一条（原子：无值无记录都未半生效）",
              st == 200 and body["timeline"] == [
                  {"seq": 1, "side": "b", "before": 100, "after": 120,
                   "deltas": [20]}], str(body))
        st, body = http("GET", "%s/status" % base2)
        check("强杀后·两栏无事务/只读关/读值空",
              body["a"]["in_transaction"] is False
              and body["b"]["in_transaction"] is False
              and body["a"]["readonly"] is False and body["b"]["readonly"] is False
              and body["a"]["last_read"] is None and body["b"]["last_read"] is None)
        check("强杀后·脚本与检查点缓存不恢复",
              body["script"] is None and body["checkpoint"]["last"] is None)

        # 恢复后继续编号 #2，并补偿末条普通记录
        assert op_http(base2, "a", "begin")[0] == 200
        assert op_http(base2, "a", "add", 3)[0] == 200
        assert op_http(base2, "a", "commit")[0] == 200
        st, body = http("GET", "%s/timeline" % base2)
        check("强杀后·新提交编号#2",
              body["timeline"][-1] == {"seq": 2, "side": "a", "before": 120,
                                      "after": 123, "deltas": [3]}, str(body))
        st, body = http("POST", "%s/timeline/compensate" % base2, {"seq": 2})
        check("强杀后·末条普通记录可补偿",
              st == 200 and body["ok"] and body["record"]["compensates"] == 2,
              str(body)[:200])
        st, body = http("GET", "%s/timeline" % base2)
        check("强杀后·补偿记录#3且原#2保留",
              [r["seq"] for r in body["timeline"]] == [1, 2, 3]
              and body["timeline"][2]["side"] == "system"
              and body["timeline"][2]["compensates"] == 2)
        st, body = http("GET", "%s/committed" % base2)
        check("强杀后·补偿值恢复120", body["committed"] == 120)

        # docker compose restart 语义：正常停止（SIGTERM，无清理钩子）再起
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    # 第三次启动：数据仍在
    port3 = free_port()
    proc = start_server(proc_dir, port3)
    try:
        check("再重启·服务启动", wait_http(port3))
        time.sleep(0.3)
        base3 = "http://127.0.0.1:%d/api/experiments/%s" % (port3, pid)
        st, body = http("GET", "%s/committed" % base3)
        st2, tlbody = http("GET", "%s/timeline" % base3)
        check("再重启·原ID值120、时间线3条",
              body["committed"] == 120 and len(tlbody["timeline"]) == 3,
              str((body, tlbody)))

        # 删除后重启：实验不得复活
        st, _ = http("DELETE", base3)
        check("再重启·删除成功", st == 200)
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    port4 = free_port()
    proc = start_server(proc_dir, port4)
    try:
        check("删除后·服务重启", wait_http(port4))
        time.sleep(0.3)
        st, _ = http(
            "GET", "http://127.0.0.1:%d/api/experiments/%s/committed" % (port4, pid)
        )
        check("删除后·原ID返回404不复活", st == 404, str(st))
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
finally:
    if proc.poll() is None:
        proc.kill()
        proc.wait()

print()
print(f"{len(fails)} 失败" if fails else "后端持久化 / 重启 / 强杀测试全部通过")
sys.exit(1 if fails else 0)
