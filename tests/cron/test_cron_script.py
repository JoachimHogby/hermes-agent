"""Tests for cron job script injection feature.

Tests cover:
- Script field in job creation / storage / update
- Script execution and output injection into prompts
- Error handling (missing script, timeout, non-zero exit)
- Path resolution (absolute, relative to HERMES_HOME/scripts/)
"""

import json
import os
import signal
import subprocess
import sys
import textwrap
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, cast

import pytest

# Ensure project root is importable
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def _capturing_popen(captured):
    class FakePopen:
        pid = 4242
        returncode = 0

        def __init__(self, argv, **kwargs):
            captured["argv"] = argv
            captured["kwargs"] = kwargs

        def communicate(self, timeout=None):
            captured["timeout"] = timeout
            return "ok\n", ""

    return FakePopen


def test_windows_job_runner_releases_assignment_handle_before_spawning_child():
    from cron import _windows_job_runner as runner

    events = []

    class FakeJob:
        def assign_current_process(self):
            events.append("assign")

        def close(self):
            events.append("close")

    class FakeChild:
        def wait(self):
            events.append("wait")
            return 7

    def fake_popen(argv, **kwargs):
        events.append(("spawn", argv, kwargs))
        return FakeChild()

    returncode = runner.run_child_in_job(
        ["python.exe", "script.py"],
        job=FakeJob(),
        popen_factory=fake_popen,
    )

    assert returncode == 7
    assert events == [
        "assign",
        "close",
        (
            "spawn",
            ["python.exe", "script.py"],
            {"creationflags": runner._CREATE_NO_WINDOW},
        ),
        "wait",
    ]


def test_windows_job_runner_configures_kill_on_close(monkeypatch):
    from cron import _windows_job_runner as runner

    observed = {}

    class FakeFunction:
        def __init__(self, callback):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    def set_information(handle, info_class, info_pointer, info_size):
        info = cast(Any, info_pointer)._obj
        observed.setdefault("set_information", []).append(
            (
                handle,
                info_class,
                info.BasicLimitInformation.LimitFlags,
                info_size,
            )
        )
        return 1

    def assign(job_handle, process_handle):
        observed["assign"] = (job_handle, process_handle)
        return 1

    def close(handle):
        observed.setdefault("closed", []).append(handle)
        return 1

    def create_job(_attrs, name):
        observed["created_name"] = name
        return 101

    def terminate_job(handle, exit_code):
        observed["terminated"] = (handle, exit_code)
        return 1

    class FakeKernel32:
        CreateJobObjectW = FakeFunction(create_job)
        OpenJobObjectW = FakeFunction(lambda *_args: 303)
        GetCurrentProcess = FakeFunction(lambda: 202)
        SetInformationJobObject = FakeFunction(set_information)
        AssignProcessToJobObject = FakeFunction(assign)
        TerminateJobObject = FakeFunction(terminate_job)
        CloseHandle = FakeFunction(close)

    fake_kernel = FakeKernel32()
    monkeypatch.setattr(runner.sys, "platform", "win32")
    monkeypatch.setattr(
        runner.ctypes, "WinDLL", lambda *_args, **_kwargs: fake_kernel,
        raising=False,
    )

    job = runner._KillOnCloseJob("Local\\HermesCron-test-owner")
    job.assign_current_process()
    job.terminate(99)
    job.disarm_kill_on_close()
    job.close()

    handle, info_class, limit_flags, info_size = observed["set_information"][0]
    assert handle == 101
    assert info_class == runner._JOB_OBJECT_EXTENDED_LIMIT_INFORMATION
    assert limit_flags & runner._JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    assert info_size > 0
    assert observed["set_information"][1][2] == 0
    assert observed["created_name"] == "Local\\HermesCron-test-owner"
    assert observed["assign"] == (101, 202)
    assert observed["terminated"] == (101, 99)
    assert observed["closed"] == [101]


def test_windows_job_runner_opens_scheduler_job_for_assignment(monkeypatch):
    from cron import _windows_job_runner as runner

    observed = {}

    class FakeFunction:
        def __init__(self, callback):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    def open_job(access, inherit, name):
        observed["opened"] = (access, inherit, name)
        return 303

    def assign(job_handle, process_handle):
        observed["assigned"] = (job_handle, process_handle)
        return 1

    def close(handle):
        observed.setdefault("closed", []).append(handle)
        return 1

    class FakeKernel32:
        CreateJobObjectW = FakeFunction(lambda *_args: pytest.fail("must open"))
        OpenJobObjectW = FakeFunction(open_job)
        GetCurrentProcess = FakeFunction(lambda: 404)
        SetInformationJobObject = FakeFunction(lambda *_args: 1)
        AssignProcessToJobObject = FakeFunction(assign)
        TerminateJobObject = FakeFunction(lambda *_args: 1)
        CloseHandle = FakeFunction(close)

    monkeypatch.setattr(runner.sys, "platform", "win32")
    monkeypatch.setattr(
        runner.ctypes,
        "WinDLL",
        lambda *_args, **_kwargs: FakeKernel32(),
        raising=False,
    )

    job = runner._KillOnCloseJob.open_existing("Local\\HermesCron-test-owner")
    job.assign_current_process()
    job.close()

    assert observed["opened"] == (
        runner._JOB_OBJECT_ASSIGN_PROCESS,
        False,
        "Local\\HermesCron-test-owner",
    )
    assert observed["assigned"] == (303, 404)
    assert observed["closed"] == [303]


def test_windows_job_runner_main_requires_scheduler_job(monkeypatch):
    from cron import _windows_job_runner as runner

    captured = {}
    assignment_job = object()

    monkeypatch.setattr(
        runner._KillOnCloseJob,
        "open_existing",
        classmethod(
            lambda _cls, name: captured.setdefault("opened", (name, assignment_job))[1]
        ),
    )

    def fake_run(argv, *, job, popen_factory=runner.subprocess.Popen):
        captured["run"] = (argv, job, popen_factory)
        return 7

    monkeypatch.setattr(runner, "run_child_in_job", fake_run)

    returncode = runner.main(
        [
            "--job-name",
            "Local\\HermesCron-test-owner",
            "--",
            "python.exe",
            "script.py",
        ]
    )

    assert returncode == 7
    assert captured["opened"] == ("Local\\HermesCron-test-owner", assignment_job)
    assert captured["run"][0] == ["python.exe", "script.py"]
    assert captured["run"][1] is assignment_job


def test_windows_job_runner_assignment_failure_never_spawns_child():
    from cron import _windows_job_runner as runner

    events = []

    class RejectingJob:
        def assign_current_process(self):
            events.append("assign")
            raise OSError("nested job rejected")

        def close(self):
            events.append("close")

    def forbidden_popen(*_args, **_kwargs) -> Any:
        pytest.fail("script child must not spawn after assignment failure")

    with pytest.raises(OSError, match="nested job rejected"):
        runner.run_child_in_job(
            ["python.exe", "script.py"],
            job=RejectingJob(),
            popen_factory=forbidden_popen,
        )

    assert events == ["assign", "close"]


def test_windows_job_runner_main_reports_open_job_failure(monkeypatch, capsys):
    from cron import _windows_job_runner as runner

    monkeypatch.setattr(
        runner._KillOnCloseJob,
        "open_existing",
        classmethod(
            lambda _cls, _name: (_ for _ in ()).throw(
                OSError("OpenJobObjectW failed")
            )
        ),
    )

    returncode = runner.main(
        ["--job-name", "missing-job", "--", "python.exe", "script.py"]
    )

    assert returncode == 125
    assert "OpenJobObjectW failed" in capsys.readouterr().err


def test_windows_job_owner_closes_handle_when_limit_configuration_fails(monkeypatch):
    from cron import _windows_job_runner as runner

    closed = []

    class FakeFunction:
        def __init__(self, callback):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    class FakeKernel32:
        CreateJobObjectW = FakeFunction(lambda *_args: 101)
        OpenJobObjectW = FakeFunction(lambda *_args: 303)
        GetCurrentProcess = FakeFunction(lambda: 202)
        SetInformationJobObject = FakeFunction(lambda *_args: 0)
        AssignProcessToJobObject = FakeFunction(lambda *_args: 1)
        TerminateJobObject = FakeFunction(lambda *_args: 1)
        CloseHandle = FakeFunction(lambda handle: closed.append(handle) or 1)

    monkeypatch.setattr(runner.sys, "platform", "win32")
    monkeypatch.setattr(
        runner.ctypes,
        "WinDLL",
        lambda *_args, **_kwargs: FakeKernel32(),
        raising=False,
    )

    with pytest.raises(OSError, match="Windows API call failed"):
        runner._KillOnCloseJob("Local\\HermesCron-broken")

    assert closed == [101]


def test_windows_job_owner_rejects_existing_named_job(monkeypatch):
    from cron import _windows_job_runner as runner

    closed = []

    class FakeFunction:
        def __init__(self, callback):
            self.callback = callback

        def __call__(self, *args):
            return self.callback(*args)

    class FakeKernel32:
        CreateJobObjectW = FakeFunction(lambda *_args: 101)
        OpenJobObjectW = FakeFunction(lambda *_args: 303)
        GetCurrentProcess = FakeFunction(lambda: 202)
        SetInformationJobObject = FakeFunction(
            lambda *_args: pytest.fail("must not reconfigure an existing named Job")
        )
        AssignProcessToJobObject = FakeFunction(lambda *_args: 1)
        TerminateJobObject = FakeFunction(lambda *_args: 1)
        CloseHandle = FakeFunction(lambda handle: closed.append(handle) or 1)

    monkeypatch.setattr(runner.sys, "platform", "win32")
    monkeypatch.setattr(
        runner.ctypes,
        "WinDLL",
        lambda *_args, **_kwargs: FakeKernel32(),
        raising=False,
    )
    monkeypatch.setattr(runner.ctypes, "set_last_error", lambda _error: None, raising=False)
    monkeypatch.setattr(runner.ctypes, "get_last_error", lambda: 183, raising=False)

    with pytest.raises(FileExistsError, match="already exists"):
        runner._KillOnCloseJob("Local\\HermesCron-collision")

    assert closed == [101]


@pytest.fixture
def cron_env(tmp_path, monkeypatch):
    """Isolated cron environment with temp HERMES_HOME."""
    hermes_home = tmp_path / ".hermes"
    hermes_home.mkdir()
    (hermes_home / "cron").mkdir()
    (hermes_home / "cron" / "output").mkdir()
    (hermes_home / "scripts").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(hermes_home))

    # Clear cached module-level paths
    import cron.jobs as jobs_mod
    monkeypatch.setattr(jobs_mod, "HERMES_DIR", hermes_home)
    monkeypatch.setattr(jobs_mod, "CRON_DIR", hermes_home / "cron")
    monkeypatch.setattr(jobs_mod, "JOBS_FILE", hermes_home / "cron" / "jobs.json")
    monkeypatch.setattr(jobs_mod, "OUTPUT_DIR", hermes_home / "cron" / "output")

    return hermes_home


class TestJobScriptField:
    """Test that the script field is stored and retrieved correctly."""

    def test_create_job_with_script(self, cron_env):
        from cron.jobs import create_job, get_job

        job = create_job(
            prompt="Analyze the data",
            schedule="every 30m",
            script="/path/to/monitor.py",
        )
        assert job["script"] == "/path/to/monitor.py"

        loaded = get_job(job["id"])
        assert loaded["script"] == "/path/to/monitor.py"

    def test_create_job_without_script(self, cron_env):
        from cron.jobs import create_job

        job = create_job(prompt="Hello", schedule="every 1h")
        assert job.get("script") is None

    def test_create_job_empty_script_normalized_to_none(self, cron_env):
        from cron.jobs import create_job

        job = create_job(prompt="Hello", schedule="every 1h", script="  ")
        assert job.get("script") is None

    def test_update_job_add_script(self, cron_env):
        from cron.jobs import create_job, update_job

        job = create_job(prompt="Hello", schedule="every 1h")
        assert job.get("script") is None

        updated = update_job(job["id"], {"script": "/new/script.py"})
        assert updated["script"] == "/new/script.py"

    def test_update_job_clear_script(self, cron_env):
        from cron.jobs import create_job, update_job

        job = create_job(prompt="Hello", schedule="every 1h", script="/some/script.py")
        assert job["script"] == "/some/script.py"

        updated = update_job(job["id"], {"script": None})
        assert updated.get("script") is None


def test_cronjob_tool_rejects_stale_past_one_shot(cron_env, monkeypatch):
    from tools.cronjob_tools import cronjob

    now = datetime(2026, 3, 18, 4, 30, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("cron.jobs._hermes_now", lambda: now)
    stale = (now - timedelta(minutes=5)).isoformat()

    result = json.loads(cronjob(action="create", prompt="Too late", schedule=stale))

    assert result["success"] is False
    assert "past and cannot be scheduled" in result["error"]


class TestRunJobScript:
    """Test the _run_job_script() function."""

    def test_successful_script(self, cron_env):
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "test.py"
        script.write_text('print("hello from script")\n')

        success, output = _run_job_script(str(script))
        assert success is True
        assert output == "hello from script"

    def test_script_relative_path(self, cron_env):
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "relative.py"
        script.write_text('print("relative works")\n')

        success, output = _run_job_script("relative.py")
        assert success is True
        assert output == "relative works"

    def test_script_not_found(self, cron_env):
        from cron.scheduler import _run_job_script

        success, output = _run_job_script("nonexistent_script.py")
        assert success is False
        assert "not found" in output.lower()

    def test_script_nonzero_exit(self, cron_env):
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "fail.py"
        script.write_text(textwrap.dedent("""\
            import sys
            print("partial output")
            print("error info", file=sys.stderr)
            sys.exit(1)
        """))

        success, output = _run_job_script(str(script))
        assert success is False
        assert "exited with code 1" in output
        assert "error info" in output
        assert "partial output" in output

    def test_script_subprocess_env_sanitized(self, cron_env, monkeypatch):
        """Cron scripts must not inherit Hermes provider env (SECURITY.md §2.3)."""
        from tools.environments.local import _HERMES_PROVIDER_ENV_BLOCKLIST
        from cron.scheduler import _run_job_script

        # sorted() so the probed var is deterministic across runs
        # (frozenset iteration order varies with PYTHONHASHSEED).
        blocked_var = sorted(_HERMES_PROVIDER_ENV_BLOCKLIST)[0]
        monkeypatch.setenv(blocked_var, "must_not_leak")

        script = cron_env / "scripts" / "env_probe.py"
        script.write_text(
            textwrap.dedent(
                f"""\
                import os
                key = {blocked_var!r}
                print("PRESENT" if os.environ.get(key) else "ABSENT")
                """
            )
        )

        success, output = _run_job_script("env_probe.py")
        assert success is True
        assert output == "ABSENT"

    def test_windows_uv_venv_python_script_bypasses_launcher(self, cron_env, tmp_path, monkeypatch):
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "probe.py"
        script.write_text('print("ok")\n')

        venv = tmp_path / "venv"
        venv_scripts = venv / "Scripts"
        site_packages = venv / "Lib" / "site-packages"
        base = tmp_path / "base"
        venv_scripts.mkdir(parents=True)
        site_packages.mkdir(parents=True)
        base.mkdir()
        venv_python = venv_scripts / "python.exe"
        base_python = base / "python.exe"
        venv_python.write_text("", encoding="utf-8")
        base_python.write_text("", encoding="utf-8")
        (venv / "pyvenv.cfg").write_text(f"home = {base}\nuv = true\n", encoding="utf-8")

        captured = {}

        class FakeWindowsJob:
            name = "Local\\HermesCron-test-owner"

            def __init__(self):
                self.events = []

            def disarm_kill_on_close(self):
                self.events.append("disarm")

            def close(self):
                self.events.append("close")

        windows_job = FakeWindowsJob()

        monkeypatch.setattr(sched_mod.sys, "platform", "win32")
        monkeypatch.setattr(sched_mod.sys, "executable", str(venv_python))
        monkeypatch.setattr(sched_mod, "windows_hide_flags", lambda: 0x08000000)
        monkeypatch.setattr(
            sched_mod, "_create_windows_cron_job", lambda: windows_job
        )
        monkeypatch.setattr(sched_mod.subprocess, "Popen", _capturing_popen(captured))

        success, output = _run_job_script("probe.py")

        assert success is True
        assert output == "ok"
        assert captured["argv"] == [
            str(base_python),
            str(Path(sched_mod.__file__).with_name("_windows_job_runner.py")),
            "--job-name",
            windows_job.name,
            "--",
            str(base_python),
            str(script.resolve()),
        ]
        assert captured["kwargs"]["creationflags"] == 0x09000000
        env = captured["kwargs"]["env"]
        assert env["VIRTUAL_ENV"] == str(venv)
        assert str(site_packages) in env["PYTHONPATH"]
        assert windows_job.events == ["disarm", "close"]

    def test_windows_breakaway_denied_retries_inside_parent_job(
        self, cron_env, monkeypatch
    ):
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "probe.py"
        script.write_text('print("ok")\n')

        class FakeWindowsJob:
            name = "Local\\HermesCron-test-owner"

            def __init__(self):
                self.events = []

            def disarm_kill_on_close(self):
                self.events.append("disarm")

            def close(self):
                self.events.append("close")

        class FakeProcess:
            pid = 4242
            returncode = 0

            def communicate(self, timeout=None):
                return "ok\n", ""

        windows_job = FakeWindowsJob()
        creationflags = []

        def fake_popen(_argv, **kwargs):
            creationflags.append(kwargs["creationflags"])
            if len(creationflags) == 1:
                raise PermissionError("breakaway denied")
            return FakeProcess()

        monkeypatch.setattr(sched_mod.sys, "platform", "win32")
        monkeypatch.setattr(sched_mod, "windows_hide_flags", lambda: 0x08000000)
        monkeypatch.setattr(
            sched_mod, "_create_windows_cron_job", lambda: windows_job
        )
        monkeypatch.setattr(sched_mod.subprocess, "Popen", fake_popen)

        success, output = _run_job_script("probe.py")

        assert success is True
        assert output == "ok"
        assert creationflags == [0x09000000, 0x08000000]
        assert windows_job.events == ["disarm", "close"]

    def test_windows_pythonw_script_uses_sibling_python_for_captured_output(self, cron_env, tmp_path, monkeypatch):
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "probe.py"
        script.write_text('print("ok")\n')

        venv = tmp_path / "venv"
        venv_scripts = venv / "Scripts"
        venv_scripts.mkdir(parents=True)
        pythonw = venv_scripts / "pythonw.exe"
        python = venv_scripts / "python.exe"
        pythonw.write_text("", encoding="utf-8")
        python.write_text("", encoding="utf-8")

        captured = {}

        class FakeWindowsJob:
            name = "Local\\HermesCron-test-owner"

            def __init__(self):
                self.events = []

            def disarm_kill_on_close(self):
                self.events.append("disarm")

            def close(self):
                self.events.append("close")

        windows_job = FakeWindowsJob()

        monkeypatch.setattr(sched_mod.sys, "platform", "win32")
        monkeypatch.setattr(sched_mod.sys, "executable", str(pythonw))
        monkeypatch.setattr(sched_mod, "windows_hide_flags", lambda: 0x08000000)
        monkeypatch.setattr(
            sched_mod, "_create_windows_cron_job", lambda: windows_job
        )
        monkeypatch.setattr(sched_mod.subprocess, "Popen", _capturing_popen(captured))

        success, output = _run_job_script("probe.py")

        assert success is True
        assert output == "ok"
        assert captured["argv"] == [
            str(python),
            str(Path(sched_mod.__file__).with_name("_windows_job_runner.py")),
            "--job-name",
            windows_job.name,
            "--",
            str(python),
            str(script.resolve()),
        ]
        assert captured["kwargs"]["creationflags"] == 0x09000000
        assert captured["kwargs"]["encoding"] == "utf-8"
        assert captured["kwargs"]["errors"] == "replace"
        assert windows_job.events == ["disarm", "close"]

    def test_non_windows_script_preserves_default_text_decoding(self, cron_env, monkeypatch):
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "probe.py"
        script.write_text('print("ok")\n')

        captured = {}

        monkeypatch.setattr(sched_mod.sys, "platform", "linux")
        monkeypatch.setattr(sched_mod.subprocess, "Popen", _capturing_popen(captured))

        success, output = _run_job_script("probe.py")

        assert success is True
        assert output == "ok"
        assert captured["argv"] == [sys.executable, str(script.resolve())]
        assert captured["kwargs"]["text"] is True
        assert "creationflags" not in captured["kwargs"]
        assert "encoding" not in captured["kwargs"]
        assert "errors" not in captured["kwargs"]
        assert captured["kwargs"]["start_new_session"] is True

    def test_script_empty_output(self, cron_env):
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "empty.py"
        script.write_text("# no output\n")

        success, output = _run_job_script(str(script))
        assert success is True
        assert output == ""

    def test_script_timeout(self, cron_env, monkeypatch):
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        # Use a very short timeout
        monkeypatch.setattr(sched_mod, "_SCRIPT_TIMEOUT", 1)

        script = cron_env / "scripts" / "slow.py"
        script.write_text("import time; time.sleep(30)\n")

        success, output = _run_job_script(str(script))
        assert success is False
        assert "timed out" in output.lower()

    def test_windows_timeout_terminates_owned_job_after_helper_exits(self, monkeypatch):
        """A won timeout must terminate descendants without reusing a PID."""
        from cron import scheduler as sched_mod

        class RootExitedDuringTerminate:
            pid = 4242
            returncode = 0
            terminate_attempts = 0

            def terminate(self):
                self.terminate_attempts += 1

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                return "", ""

        class OwnedJobWithLiveDescendant:
            descendant_alive = True
            terminate_attempts = 0

            def terminate(self):
                self.terminate_attempts += 1
                self.descendant_alive = False

        proc = RootExitedDuringTerminate()
        job = OwnedJobWithLiveDescendant()

        monkeypatch.setattr(sched_mod.sys, "platform", "win32")
        monkeypatch.setattr(
            sched_mod.subprocess,
            "run",
            lambda *_args, **_kwargs: pytest.fail(
                "timeout cleanup must not target a reapable/reusable PID"
            ),
        )

        sched_mod._terminate_script_process_tree(
            cast(Any, proc), pgid=None, windows_job=cast(Any, job)
        )

        assert proc.terminate_attempts == 1
        assert job.terminate_attempts == 1
        assert job.descendant_alive is False

    def test_windows_hidden_process_does_not_rely_on_ctrl_break_or_pid(self, monkeypatch):
        """CREATE_NO_WINDOW cleanup uses process and Job handles only."""
        from cron import scheduler as sched_mod

        class HiddenProcessWithLiveChild:
            pid = 4242
            returncode = None
            ctrl_break_attempts = 0
            terminate_attempts = 0

            def send_signal(self, _sig):
                self.ctrl_break_attempts += 1
                raise OSError("no console")

            def terminate(self):
                self.terminate_attempts += 1

            def wait(self, timeout=None):
                raise sched_mod.subprocess.TimeoutExpired(
                    "hidden", float(timeout or 0)
                )

            def poll(self):
                return self.returncode

            def communicate(self, timeout=None):
                return "", ""

            def kill(self):
                self.returncode = 1

        proc = HiddenProcessWithLiveChild()
        class OwnedJob:
            terminate_attempts = 0

            def terminate(self):
                self.terminate_attempts += 1
                proc.returncode = 1

        windows_job = OwnedJob()

        monkeypatch.setattr(sched_mod.sys, "platform", "win32")
        monkeypatch.setattr(sched_mod.signal, "CTRL_BREAK_EVENT", 1, raising=False)
        monkeypatch.setattr(
            sched_mod.subprocess,
            "run",
            lambda *_args, **_kwargs: pytest.fail("must not target a numeric PID"),
        )

        sched_mod._terminate_script_process_tree(
            cast(Any, proc), pgid=None, windows_job=cast(Any, windows_job)
        )

        assert proc.ctrl_break_attempts == 0
        assert proc.terminate_attempts == 1
        assert windows_job.terminate_attempts == 1

    def test_windows_terminate_job_failure_still_closes_armed_owner(
        self, cron_env, monkeypatch, caplog
    ):
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "slow.py"
        script.write_text("import time; time.sleep(30)\n")

        class FailingTerminateJob:
            name = "Local\\HermesCron-test-owner"

            def __init__(self):
                self.events = []

            def terminate(self):
                self.events.append("terminate")
                raise OSError("TerminateJobObject failed")

            def disarm_kill_on_close(self):
                self.events.append("disarm")

            def close(self):
                self.events.append("close")

        class TimedOutProcess:
            pid = 4242
            returncode = None
            terminate_attempts = 0
            communicate_attempts = 0

            def communicate(self, timeout=None):
                self.communicate_attempts += 1
                if self.communicate_attempts == 1:
                    raise sched_mod.subprocess.TimeoutExpired(
                        "helper", float(timeout or 0)
                    )
                self.returncode = 1
                return "", ""

            def terminate(self):
                self.terminate_attempts += 1

        windows_job = FailingTerminateJob()
        proc = TimedOutProcess()
        monkeypatch.setattr(sched_mod.sys, "platform", "win32")
        monkeypatch.setattr(sched_mod, "_SCRIPT_TIMEOUT", 1)
        monkeypatch.setattr(sched_mod, "windows_hide_flags", lambda: 0x08000000)
        monkeypatch.setattr(
            sched_mod, "_create_windows_cron_job", lambda: windows_job
        )
        monkeypatch.setattr(sched_mod.subprocess, "Popen", lambda *_a, **_kw: proc)
        monkeypatch.setattr(
            sched_mod.subprocess,
            "run",
            lambda *_args, **_kwargs: pytest.fail("must not target a numeric PID"),
        )

        success, output = _run_job_script("slow.py")

        assert success is False
        assert "timed out" in output.lower()
        assert proc.terminate_attempts == 1
        assert windows_job.events == ["terminate", "close"]
        assert "Failed to terminate timed-out Windows cron Job Object" in caplog.text

    @pytest.mark.live_system_guard_bypass
    @pytest.mark.skipif(
        sys.platform != "win32", reason="Windows Job Object regression"
    )
    def test_windows_timeout_terminates_descendant_with_job_object(
        self, cron_env, monkeypatch
    ):
        """Native Windows: root termination must reap a live descendant."""
        import psutil

        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        monkeypatch.setattr(sched_mod, "_SCRIPT_TIMEOUT", 1)
        monkeypatch.setattr(
            sched_mod, "_SCRIPT_TERMINATE_GRACE_SECONDS", 0.2, raising=False
        )

        child_pid_file = cron_env / "windows-child.pid"
        script = cron_env / "scripts" / "spawns_windows_child.py"
        script.write_text(
            textwrap.dedent(
                f"""\
                import subprocess
                import sys
                import time
                from pathlib import Path

                child = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(30)"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                Path({str(child_pid_file)!r}).write_text(str(child.pid))
                time.sleep(30)
                """
            )
        )

        success, output = _run_job_script(str(script))

        assert success is False
        assert "timed out" in output.lower()
        assert child_pid_file.exists()
        child_pid = int(child_pid_file.read_text())
        try:
            deadline = time.monotonic() + 3
            while psutil.pid_exists(child_pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not psutil.pid_exists(child_pid), (
                f"orphaned Windows child process still alive: {child_pid}"
            )
        finally:
            if psutil.pid_exists(child_pid):
                psutil.Process(child_pid).kill()

    @pytest.mark.live_system_guard_bypass
    @pytest.mark.skipif(
        sys.platform != "win32", reason="Windows Job Object race regression"
    )
    def test_windows_timeout_wins_before_script_exits_with_live_descendant(
        self, cron_env, monkeypatch
    ):
        """The scheduler-owned Job remains armed after the helper exits."""
        import psutil

        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        monkeypatch.setattr(sched_mod, "_SCRIPT_TIMEOUT", 1)
        monkeypatch.setattr(
            sched_mod, "_SCRIPT_TERMINATE_GRACE_SECONDS", 0.2, raising=False
        )

        release_file = cron_env / "release-script"
        script_pid_file = cron_env / "windows-script.pid"
        child_pid_file = cron_env / "windows-race-child.pid"
        script = cron_env / "scripts" / "windows_timeout_race.py"
        script.write_text(
            textwrap.dedent(
                f"""\
                import os
                import subprocess
                import sys
                import time
                from pathlib import Path

                release = Path({str(release_file)!r})
                child = subprocess.Popen(
                    [sys.executable, "-c", "import time; time.sleep(30)"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                Path({str(script_pid_file)!r}).write_text(str(os.getpid()))
                Path({str(child_pid_file)!r}).write_text(str(child.pid))
                while not release.exists():
                    time.sleep(0.01)
                """
            )
        )

        original_cleanup = sched_mod._terminate_script_process_tree

        def exit_script_before_cleanup(proc, *, pgid, windows_job=None):
            deadline = time.monotonic() + 3
            while (
                not script_pid_file.exists() or not child_pid_file.exists()
            ) and time.monotonic() < deadline:
                time.sleep(0.01)
            assert script_pid_file.exists()
            assert child_pid_file.exists()

            script_pid = int(script_pid_file.read_text())
            child_pid = int(child_pid_file.read_text())
            release_file.write_text("exit")
            while psutil.pid_exists(script_pid) and time.monotonic() < deadline:
                time.sleep(0.01)

            assert not psutil.pid_exists(script_pid), "script did not exit in cleanup window"
            assert psutil.pid_exists(child_pid), "descendant exited before cleanup assertion"
            return original_cleanup(
                proc,
                pgid=pgid,
                windows_job=windows_job,
            )

        monkeypatch.setattr(
            sched_mod,
            "_terminate_script_process_tree",
            exit_script_before_cleanup,
        )

        success, output = _run_job_script(str(script))

        assert success is False
        assert "timed out" in output.lower()
        child_pid = int(child_pid_file.read_text())
        try:
            deadline = time.monotonic() + 3
            while psutil.pid_exists(child_pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not psutil.pid_exists(child_pid), (
                f"orphaned Windows race child still alive: {child_pid}"
            )
        finally:
            if psutil.pid_exists(child_pid):
                psutil.Process(child_pid).kill()

    @pytest.mark.live_system_guard_bypass
    @pytest.mark.skipif(
        sys.platform != "win32", reason="Windows nested Job Object regression"
    )
    def test_windows_restrictive_parent_job_uses_nested_job_fallback(
        self, cron_env, tmp_path
    ):
        """BREAKAWAY denial retries inside the restrictive parent Job."""
        script = cron_env / "scripts" / "nested_probe.py"
        script.write_text('print("nested-ok")\n')
        harness = tmp_path / "nested_job_harness.py"
        harness.write_text(
            textwrap.dedent(
                """\
                import json
                import uuid

                from cron._windows_job_runner import _KillOnCloseJob
                from cron.scheduler import _run_job_script

                outer = _KillOnCloseJob(
                    f"HermesCron-test-outer-{uuid.uuid4().hex}"
                )
                try:
                    outer.assign_current_process()
                    result = _run_job_script("nested_probe.py")
                    print(json.dumps(result))
                finally:
                    outer.disarm_kill_on_close()
                    outer.close()
                """
            )
        )

        result = subprocess.run(
            [sys.executable, str(harness)],
            capture_output=True,
            text=True,
            timeout=15,
            cwd=str(Path(__file__).resolve().parents[2]),
            env=os.environ.copy(),
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000),
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout.strip().splitlines()[-1]) == [
            True,
            "nested-ok",
        ]

    @pytest.mark.live_system_guard_bypass
    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group regression")
    def test_script_timeout_terminates_descendant_process_group(self, cron_env, monkeypatch):
        """A timed-out wrapper must not leave its runner child orphaned."""
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        monkeypatch.setattr(sched_mod, "_SCRIPT_TIMEOUT", 1)
        monkeypatch.setattr(
            sched_mod, "_SCRIPT_TERMINATE_GRACE_SECONDS", 0.2, raising=False
        )

        child_pid_file = cron_env / "child.pid"
        script = cron_env / "scripts" / "spawns_child.py"
        child_code = (
            "import signal, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "time.sleep(30)"
        )
        script.write_text(
            textwrap.dedent(
                f"""\
                import subprocess
                import sys
                import time
                from pathlib import Path

                child = subprocess.Popen(
                    [sys.executable, "-c", {child_code!r}],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                Path({str(child_pid_file)!r}).write_text(str(child.pid))
                time.sleep(30)
                """
            )
        )

        success, output = _run_job_script(str(script))
        assert success is False
        assert "timed out" in output.lower()
        assert child_pid_file.exists()
        child_pid = int(child_pid_file.read_text())

        def child_is_alive() -> bool:
            try:
                os.kill(child_pid, 0)  # windows-footgun: ok — POSIX-only test
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            return True

        try:
            deadline = time.monotonic() + 3
            while child_is_alive() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not child_is_alive(), f"orphaned child process still alive: {child_pid}"
        finally:
            if child_is_alive():
                os.kill(child_pid, signal.SIGKILL)  # windows-footgun: ok — POSIX-only test

    @pytest.mark.live_system_guard_bypass
    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX process-group regression")
    def test_script_timeout_terminates_descendant_in_new_session(self, cron_env, monkeypatch):
        """A timed-out wrapper must also kill descendants that called setsid()."""
        from cron import scheduler as sched_mod
        from cron.scheduler import _run_job_script

        monkeypatch.setattr(sched_mod, "_SCRIPT_TIMEOUT", 1)
        monkeypatch.setattr(
            sched_mod, "_SCRIPT_TERMINATE_GRACE_SECONDS", 0.2, raising=False
        )

        child_pid_file = cron_env / "detached-child.pid"
        script = cron_env / "scripts" / "spawns_detached_child.py"
        child_code = (
            "import signal, time; "
            "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "time.sleep(30)"
        )
        script.write_text(
            textwrap.dedent(
                f"""\
                import subprocess
                import sys
                import time
                from pathlib import Path

                child = subprocess.Popen(
                    [sys.executable, "-c", {child_code!r}],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
                Path({str(child_pid_file)!r}).write_text(str(child.pid))
                time.sleep(30)
                """
            )
        )

        success, output = _run_job_script(str(script))
        assert success is False
        assert "timed out" in output.lower()
        assert child_pid_file.exists()
        child_pid = int(child_pid_file.read_text())

        def child_is_alive() -> bool:
            try:
                os.kill(child_pid, 0)  # windows-footgun: ok — POSIX-only test
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            return True

        try:
            deadline = time.monotonic() + 3
            while child_is_alive() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert not child_is_alive(), f"detached child process still alive: {child_pid}"
        finally:
            if child_is_alive():
                os.kill(child_pid, signal.SIGKILL)  # windows-footgun: ok — POSIX-only test

    def test_script_json_output(self, cron_env):
        """Scripts can output structured JSON for the LLM to parse."""
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "json_out.py"
        script.write_text(textwrap.dedent("""\
            import json
            data = {"new_prs": [{"number": 42, "title": "Fix bug"}]}
            print(json.dumps(data, indent=2))
        """))

        success, output = _run_job_script(str(script))
        assert success is True
        parsed = json.loads(output)
        assert parsed["new_prs"][0]["number"] == 42


class TestBuildJobPromptWithScript:
    """Test that script output is injected into the prompt."""

    def test_script_output_injected(self, cron_env):
        from cron.scheduler import _build_job_prompt

        script = cron_env / "scripts" / "data.py"
        script.write_text('print("new PR: #123 fix typo")\n')

        job = {
            "prompt": "Report any notable changes.",
            "script": str(script),
        }
        prompt = _build_job_prompt(job)
        assert "## Script Output" in prompt
        assert "new PR: #123 fix typo" in prompt
        assert "Report any notable changes." in prompt

    def test_script_error_injected(self, cron_env):
        from cron.scheduler import _build_job_prompt

        job = {
            "prompt": "Report status.",
            "script": "nonexistent_monitor.py",
        }
        prompt = _build_job_prompt(job)
        assert "## Script Error" in prompt
        assert "not found" in prompt.lower()
        assert "Report status." in prompt

    def test_no_script_unchanged(self, cron_env):
        from cron.scheduler import _build_job_prompt

        job = {"prompt": "Simple job."}
        prompt = _build_job_prompt(job)
        assert "## Script Output" not in prompt
        assert "Simple job." in prompt



class TestCronjobToolScript:
    """Test the cronjob tool's script parameter."""

    def test_create_with_script(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="monitor.py",
        ))
        assert result["success"] is True
        assert result["job"]["script"] == "monitor.py"

    def test_update_script(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        create_result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
        ))
        job_id = create_result["job_id"]

        update_result = json.loads(cronjob(
            action="update",
            job_id=job_id,
            script="new_script.py",
        ))
        assert update_result["success"] is True
        assert update_result["job"]["script"] == "new_script.py"

    def test_clear_script(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        create_result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="some_script.py",
        ))
        job_id = create_result["job_id"]

        update_result = json.loads(cronjob(
            action="update",
            job_id=job_id,
            script="",
        ))
        assert update_result["success"] is True
        assert "script" not in update_result["job"]

    def test_list_shows_script(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="data_collector.py",
        )

        list_result = json.loads(cronjob(action="list"))
        assert list_result["success"] is True
        assert len(list_result["jobs"]) == 1
        assert list_result["jobs"][0]["script"] == "data_collector.py"


class TestScriptPathContainment:
    """Regression tests for path containment bypass in _run_job_script().

    Prior to the fix, absolute paths and ~-prefixed paths bypassed the
    scripts_dir containment check entirely, allowing arbitrary script
    execution through the cron system.
    """

    def test_absolute_path_outside_scripts_dir_blocked(self, cron_env):
        """Absolute paths outside ~/.hermes/scripts/ must be rejected."""
        from cron.scheduler import _run_job_script

        # Create a script outside the scripts dir
        outside_script = cron_env / "outside.py"
        outside_script.write_text('print("should not run")\n')

        success, output = _run_job_script(str(outside_script))
        assert success is False
        assert "blocked" in output.lower() or "outside" in output.lower()

    def test_absolute_path_tmp_blocked(self, cron_env):
        """Absolute paths to /tmp must be rejected."""
        from cron.scheduler import _run_job_script

        success, output = _run_job_script("/tmp/evil.py")
        assert success is False
        assert "blocked" in output.lower() or "outside" in output.lower()

    def test_tilde_path_blocked(self, cron_env):
        """~ prefixed paths must be rejected (expanduser bypasses check)."""
        from cron.scheduler import _run_job_script

        success, output = _run_job_script("~/evil.py")
        assert success is False
        assert "blocked" in output.lower() or "outside" in output.lower()

    def test_tilde_traversal_blocked(self, cron_env):
        """~/../../../tmp/evil.py must be rejected."""
        from cron.scheduler import _run_job_script

        success, output = _run_job_script("~/../../../tmp/evil.py")
        assert success is False
        assert "blocked" in output.lower() or "outside" in output.lower()

    def test_relative_traversal_still_blocked(self, cron_env):
        """../../etc/passwd style traversal must still be blocked."""
        from cron.scheduler import _run_job_script

        success, output = _run_job_script("../../etc/passwd")
        assert success is False
        assert "blocked" in output.lower() or "outside" in output.lower()

    def test_relative_path_inside_scripts_dir_allowed(self, cron_env):
        """Relative paths within the scripts dir should still work."""
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "good.py"
        script.write_text('print("ok")\n')

        success, output = _run_job_script("good.py")
        assert success is True
        assert output == "ok"

    def test_subdirectory_inside_scripts_dir_allowed(self, cron_env):
        """Relative paths to subdirectories within scripts/ should work."""
        from cron.scheduler import _run_job_script

        subdir = cron_env / "scripts" / "monitors"
        subdir.mkdir()
        script = subdir / "check.py"
        script.write_text('print("sub ok")\n')

        success, output = _run_job_script("monitors/check.py")
        assert success is True
        assert output == "sub ok"

    def test_absolute_path_inside_scripts_dir_allowed(self, cron_env):
        """Absolute paths that resolve WITHIN scripts/ should work."""
        from cron.scheduler import _run_job_script

        script = cron_env / "scripts" / "abs_ok.py"
        script.write_text('print("abs ok")\n')

        success, output = _run_job_script(str(script))
        assert success is True
        assert output == "abs ok"

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="Symlinks require elevated privileges on Windows",
    )
    def test_symlink_escape_blocked(self, cron_env, tmp_path):
        """Symlinks pointing outside scripts/ must be rejected."""
        from cron.scheduler import _run_job_script

        # Create a script outside the scripts dir
        outside = tmp_path / "outside_evil.py"
        outside.write_text('print("escaped")\n')

        # Create a symlink inside scripts/ pointing outside
        link = cron_env / "scripts" / "sneaky.py"
        link.symlink_to(outside)

        success, output = _run_job_script("sneaky.py")
        assert success is False
        assert "blocked" in output.lower() or "outside" in output.lower()


class TestCronjobToolScriptValidation:
    """Test API-boundary validation of cron script paths in cronjob_tools."""

    def test_create_with_absolute_script_rejected(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="/home/user/evil.py",
        ))
        assert result["success"] is False
        assert "relative" in result["error"].lower() or "absolute" in result["error"].lower()

    def test_create_with_tilde_script_rejected(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="~/monitor.py",
        ))
        assert result["success"] is False
        assert "relative" in result["error"].lower() or "absolute" in result["error"].lower()

    def test_create_with_traversal_script_rejected(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="../../etc/passwd",
        ))
        assert result["success"] is False
        assert "escapes" in result["error"].lower() or "traversal" in result["error"].lower()

    def test_create_with_relative_script_allowed(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="monitor.py",
        ))
        assert result["success"] is True
        assert result["job"]["script"] == "monitor.py"

    def test_update_with_absolute_script_rejected(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        create_result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
        ))
        job_id = create_result["job_id"]

        update_result = json.loads(cronjob(
            action="update",
            job_id=job_id,
            script="/tmp/evil.py",
        ))
        assert update_result["success"] is False
        assert "relative" in update_result["error"].lower() or "absolute" in update_result["error"].lower()

    def test_update_clear_script_allowed(self, cron_env, monkeypatch):
        """Clearing a script (empty string) should always be permitted."""
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        create_result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="monitor.py",
        ))
        job_id = create_result["job_id"]

        update_result = json.loads(cronjob(
            action="update",
            job_id=job_id,
            script="",
        ))
        assert update_result["success"] is True
        assert "script" not in update_result["job"]

    def test_windows_absolute_path_rejected(self, cron_env, monkeypatch):
        monkeypatch.setenv("HERMES_INTERACTIVE", "1")
        from tools.cronjob_tools import cronjob

        result = json.loads(cronjob(
            action="create",
            schedule="every 1h",
            prompt="Monitor things",
            script="C:\\Users\\evil\\script.py",
        ))
        assert result["success"] is False


class TestRunJobEnvVarCleanup:
    """Test that run_job() env vars are cleaned up even on early failure."""

    def test_env_vars_cleaned_on_early_error(self, cron_env, monkeypatch):
        """Origin env vars must be cleaned up even if run_job fails early."""
        # Ensure env vars are clean before test
        for key in (
            "HERMES_SESSION_PLATFORM",
            "HERMES_SESSION_CHAT_ID",
            "HERMES_SESSION_CHAT_NAME",
        ):
            monkeypatch.delenv(key, raising=False)

        # Build a job with origin info that will fail during execution
        # (no valid model, no API key — will raise inside try block)
        job = {
            "id": "test-envleak",
            "name": "env-leak-test",
            "prompt": "test",
            "schedule_display": "every 1h",
            "origin": {
                "platform": "telegram",
                "chat_id": "12345",
                "chat_name": "Test Chat",
            },
        }

        from cron.scheduler import run_job

        # Expect it to fail (no model/API key), but env vars must be cleaned
        try:
            run_job(job)
        except Exception:
            pass

        # Verify env vars were cleaned up by the finally block
        assert os.environ.get("HERMES_SESSION_PLATFORM") is None
        assert os.environ.get("HERMES_SESSION_CHAT_ID") is None
        assert os.environ.get("HERMES_SESSION_CHAT_NAME") is None
