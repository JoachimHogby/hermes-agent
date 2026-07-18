"""Run a Windows cron script inside a scheduler-owned Job Object.

The scheduler creates and retains a uniquely named kill-on-close Job before it
launches this hidden helper. The helper opens that exact Job, assigns itself,
then closes its assignment-only handle before spawning the requested command.
The scheduler therefore remains the sole owner while the helper, script, and
all descendants inherit Job membership. It can terminate the tree by handle
after any root exit, without targeting a reusable numeric PID.
"""

from __future__ import annotations

import ctypes
import subprocess
import sys
from ctypes import wintypes
from typing import Any, Callable, Protocol, Sequence

_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
_JOB_OBJECT_ASSIGN_PROCESS = 0x0001
_ERROR_ALREADY_EXISTS = 183
_CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)


def _win_error(error: int | None = None) -> OSError:
    """Build a WinError without exposing Windows-only ctypes attrs to POSIX."""
    if error is None:
        get_last_error = getattr(ctypes, "get_last_error", lambda: 0)
        error = int(get_last_error())
    win_error = getattr(ctypes, "WinError", None)
    if win_error is None:
        return OSError(error, f"Windows API call failed with error {error}")
    return win_error(error)


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_ulonglong),
        ("WriteOperationCount", ctypes.c_ulonglong),
        ("OtherOperationCount", ctypes.c_ulonglong),
        ("ReadTransferCount", ctypes.c_ulonglong),
        ("WriteTransferCount", ctypes.c_ulonglong),
        ("OtherTransferCount", ctypes.c_ulonglong),
    ]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class _Job(Protocol):
    def assign_current_process(self) -> None: ...

    def close(self) -> None: ...


class _WaitableProcess(Protocol):
    def wait(self) -> int: ...


class _KillOnCloseJob:
    """Small stdlib-only wrapper around the documented Win32 Job APIs."""

    _kernel32: Any
    _handle: Any
    name: str | None

    @staticmethod
    def _load_kernel32() -> Any:
        if sys.platform != "win32":
            raise OSError("Windows Job Objects are only available on Windows")

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.OpenJobObjectW.argtypes = [
            wintypes.DWORD,
            wintypes.BOOL,
            wintypes.LPCWSTR,
        ]
        kernel32.OpenJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE,
            ctypes.c_int,
            ctypes.c_void_p,
            wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE,
            wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel32.TerminateJobObject.restype = wintypes.BOOL
        kernel32.GetCurrentProcess.argtypes = []
        kernel32.GetCurrentProcess.restype = wintypes.HANDLE
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        return kernel32

    def __init__(self, name: str | None = None) -> None:
        kernel32 = self._load_kernel32()

        set_last_error = getattr(ctypes, "set_last_error", None)
        if set_last_error is not None:
            set_last_error(0)
        handle = kernel32.CreateJobObjectW(None, name)
        if not handle:
            raise _win_error()
        get_last_error = getattr(ctypes, "get_last_error", lambda: 0)
        if name is not None and int(get_last_error()) == _ERROR_ALREADY_EXISTS:
            kernel32.CloseHandle(handle)
            raise FileExistsError(f"Windows Job Object already exists: {name}")

        self._kernel32: Any = kernel32
        self._handle: Any = handle
        self.name = name
        try:
            self._set_limit_flags(_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE)
        except OSError:
            self.close()
            raise

    @classmethod
    def open_existing(cls, name: str) -> "_KillOnCloseJob":
        """Open the scheduler-owned job only for self-assignment."""
        kernel32 = cls._load_kernel32()
        handle = kernel32.OpenJobObjectW(
            _JOB_OBJECT_ASSIGN_PROCESS,
            False,
            name,
        )
        if not handle:
            raise _win_error()

        job = cls.__new__(cls)
        job._kernel32 = kernel32
        job._handle = handle
        job.name = name
        return job

    def _set_limit_flags(self, flags: int) -> None:
        info = _ExtendedLimitInformation()
        info.BasicLimitInformation.LimitFlags = flags
        if not self._kernel32.SetInformationJobObject(
            self._handle,
            _JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(info),
            ctypes.sizeof(info),
        ):
            raise _win_error()

    def assign_current_process(self) -> None:
        if not self._handle:
            raise OSError("Windows Job Object is closed")
        if not self._kernel32.AssignProcessToJobObject(
            self._handle,
            self._kernel32.GetCurrentProcess(),
        ):
            raise _win_error()

    def disarm_kill_on_close(self) -> None:
        """Allow the scheduler to release a normally completed Job."""
        if not self._handle:
            raise OSError("Windows Job Object is closed")
        self._set_limit_flags(0)

    def terminate(self, exit_code: int = 1) -> None:
        """Terminate every process currently assigned to this exact job."""
        if not self._handle:
            raise OSError("Windows Job Object is closed")
        if not self._kernel32.TerminateJobObject(self._handle, exit_code):
            raise _win_error()

    def close(self) -> None:
        if self._handle:
            self._kernel32.CloseHandle(self._handle)
            self._handle = None


def run_child_in_job(
    argv: Sequence[str],
    *,
    job: _Job,
    popen_factory: Callable[..., _WaitableProcess] = subprocess.Popen,
) -> int:
    """Join the scheduler-owned job before spawning the requested command."""
    try:
        job.assign_current_process()
    finally:
        job.close()

    # CREATE_NO_WINDOW helpers do not have console-backed standard handles.
    # Pass the scheduler-provided pipe handles explicitly so the script's
    # stdout/stderr survive the second process boundary.
    child = popen_factory(
        argv,
        stdout=sys.stdout,
        stderr=sys.stderr,
        creationflags=_CREATE_NO_WINDOW,
    )
    return child.wait()


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if (
        len(arguments) < 4
        or arguments[0] != "--job-name"
        or not arguments[1]
        or arguments[2] != "--"
    ):
        print(
            "Usage: _windows_job_runner.py --job-name NAME -- COMMAND [ARG ...]",
            file=sys.stderr,
        )
        return 2

    job_name = arguments[1]
    command = arguments[3:]
    try:
        assignment_job = _KillOnCloseJob.open_existing(job_name)
        return run_child_in_job(command, job=assignment_job)
    except Exception as exc:
        print(f"Cron Windows job runner failed: {exc}", file=sys.stderr)
        return 125


if __name__ == "__main__":
    raise SystemExit(main())
