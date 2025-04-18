"""
Provides methods for executing a program or workflow locally. This class
is intended to be called from Exec, not by general users.
"""

import time
import subprocess
import os
import sys
import io
import threading
from jarvis_util.jutil_manager import JutilManager
from .exec_info import ExecInfo, ExecType, Executable


class LocalExec(Executable):
    """
    Provides methods for executing a program or workflow locally.
    """

    def __init__(self, cmd, exec_info):
        """
        Execute a program or workflow

        :param cmd: list of commands or a single command string
        :param exec_info: Info needed to execute processes locally
        """

        super().__init__()

        # Managing console output and collection
        self.collect_output = exec_info.collect_output
        self.pipe_stdout = exec_info.pipe_stdout
        self.pipe_stderr = exec_info.pipe_stderr
        self.pipe_stdout_fp = None
        self.pipe_stderr_fp = None
        self.hide_output = exec_info.hide_output
        # pylint: disable=R1732
        if self.collect_output is None:
            self.collect_output = self.jutil.collect_output
        if self.pipe_stdout is not None:
            self.pipe_stdout_fp = open(self.pipe_stdout, 'wb')
        if self.pipe_stderr is not None:
            self.pipe_stderr_fp = open(self.pipe_stderr, 'wb')
        if self.hide_output is None:
            self.hide_output = self.jutil.hide_output
        # pylint: enable=R1732
        self.stdout = io.StringIO()
        self.stderr = io.StringIO()
        self.last_stdout_size = 0
        self.last_stderr_size = 0
        self.print_stdout_thread = None
        self.print_stderr_thread = None
        self.exit_code = 0

        # Managing command execution
        self.start_time = time.time()
        self.timeout = exec_info.timeout
        self.sudo = exec_info.sudo
        self.stdin = exec_info.stdin
        self.exec_async = exec_info.exec_async
        self.sleep_ms = exec_info.sleep_ms
        if exec_info.cwd is None:
            self.cwd = os.getcwd()
        else:
            self.cwd = exec_info.cwd
        self.basic_env = exec_info.basic_env.copy()

        # Create the command
        cmd = self.smash_cmd(cmd, self.sudo, self.basic_env, exec_info.sudoenv)
        if exec_info.do_dbg:
            cmd = self.get_dbg_cmd(cmd, exec_info)
        self.cmd = cmd

        # Copy ENV
        self.env = exec_info.env.copy()
        for key, val in os.environ.items():
            if key not in self.env:
                self.env[key] = val

        # Execute the command
        if self.jutil.debug_local_exec:
            print(cmd)
        self._start_bash_processes()

    def _start_bash_processes(self):
        time.sleep(self.sleep_ms)
        # pylint: disable=R1732
        self.proc = subprocess.Popen(self.cmd,
                                     stdin=self.stdin,
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE,
                                     cwd=self.cwd,
                                     env=self.env,
                                     shell=True)
        # pylint: enable=R1732
        self.print_stdout_thread = threading.Thread(
            target=self.print_stdout_worker)
        self.print_stderr_thread = threading.Thread(
            target=self.print_stderr_worker)
        self.print_stdout_thread.start()
        self.print_stderr_thread.start()
        if not self.exec_async:
            self.wait()

    def wait(self):
        # self.proc.wait()
        if self.timeout:
            try:
                self.proc.wait(timeout=self.timeout)
            except:
                self.proc.kill()
                pass
        self.join_print_worker()
        self.set_exit_code()
        return self.exit_code

    def set_exit_code(self):
        self.exit_code = self.proc.returncode

    def get_pid(self):
        if self.proc is not None:
            return self.proc.pid
        else:
            return None

    def print_stdout_worker(self):
        while self.proc.poll() is None:
            self.print_to_outputs(self.proc.stdout, self.stdout,
                                  self.pipe_stdout_fp, sys.stdout)
            time.sleep(25 / 1000)
        self.print_to_outputs(self.proc.stdout, self.stdout,
                              self.pipe_stdout_fp, sys.stdout)

    def print_stderr_worker(self):
        while self.proc.poll() is None:
            self.print_to_outputs(self.proc.stderr, self.stderr,
                                  self.pipe_stderr_fp, sys.stderr)
            
            time.sleep(25 / 1000)
        self.print_to_outputs(self.proc.stderr, self.stderr,
                              self.pipe_stderr_fp, sys.stderr)

    def print_to_outputs(self, proc_sysout, self_sysout, file_sysout, sysout):
        # pylint: disable=W0702
        for line in proc_sysout:
            try:
                text = line.decode('utf-8')
                if not self.hide_output:
                    sysout.write(text)
                if self.collect_output:
                    self_sysout.write(text)
                    self_sysout.flush()
                if file_sysout is not None:
                    file_sysout.write(line)
            except:
                return
        # pylint: enable=W0702

    def join_print_worker(self):
        if isinstance(self.stdout, str):
            return
        self.print_stdout_thread.join()
        self.print_stderr_thread.join()
        self.stdout = self.stdout.getvalue()
        self.stderr = self.stderr.getvalue()
        if self.pipe_stdout_fp is not None:
            self.pipe_stdout_fp.close()
        if self.pipe_stderr_fp is not None:
            self.pipe_stderr_fp.close()


class LocalExecInfo(ExecInfo):
    def __init__(self, **kwargs):
        super().__init__(exec_type=ExecType.LOCAL, **kwargs)
