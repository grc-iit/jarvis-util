"""
This module contains data structures for determining how to execute
a subcommand. This includes information such as storing SSH keys,
passwords, working directory, etc.
"""

from enum import Enum
from jarvis_util.util.hostfile import Hostfile
from jarvis_util.jutil_manager import JutilManager
import os
from abc import ABC, abstractmethod


class ExecType(Enum):
    """
    Different program execution methods.
    """

    LOCAL = 'LOCAL'
    SSH = 'SSH'
    PSSH = 'PSSH'
    MPI = 'MPI'
    MPICH = 'MPICH'
    OPENMPI = 'OPENMPI'
    INTEL_MPI = 'INTEL_MPI'
    SLURM = 'SLURM'
    PBS = 'PBS'
    CRAY_MPICH = 'CRAY_MPICH'


class ExecInfo:
    """
    Contains all information needed to execute a program. This includes
    parameters such as the path to key-pairs, the hosts to run the program
    on, number of processes, etc.
    """
    def __init__(self,  exec_type=ExecType.LOCAL, nprocs=None, ppn=None,
                 user=None, pkey=None, port=None,
                 hostfile=None, hosts=None, env=None,
                 sleep_ms=0, sudo=False, sudoenv=True, cwd=None,
                 collect_output=None, pipe_stdout=None, pipe_stderr=None,
                 hide_output=None, exec_async=False, stdin=None,
                 do_dbg=False, dbg_port=None, strict_ssh=False, **kwargs):
        """

        :param exec_type: How to execute a program. SSH, MPI, Local, etc.
        :param nprocs: Number of processes to spawn. E.g., MPI uses this
        :param ppn: Number of processes per node. E.g., MPI uses this
        :param user: The user to execute command under. E.g., SSH, PSSH
        :param pkey: The path to the private key. E.g., SSH, PSSH
        :param port: The port to use for connection. E.g., SSH, PSSH
        :param hostfile: The hosts to launch command on. E.g., PSSH, MPI
        :param hosts: A list (or single string) of host names to run command on.
        :param env: The environment variables to use for command.
        :param sleep_ms: Sleep for a period of time AFTER executing
        :param sudo: Execute command with root privilege. E.g., SSH, PSSH
        :param sudoenv: Support environment preservation in sudo
        :param cwd: Set current working directory. E.g., SSH, PSSH
        :param collect_output: Collect program output in python buffer
        :param pipe_stdout: Pipe STDOUT into a file. (path string)
        :param pipe_stderr: Pipe STDERR into a file. (path string)
        :param hide_output: Whether to print output to console
        :param exec_async: Whether to execute program asynchronously
        :param stdin: Any input needed by the program. Only local
        :param do_dbg: Enable debugging
        :param dbg_port: The port number
        :param strict_ssh: Strict ssh host key verification
        """

        self.exec_type = exec_type
        self.nprocs = nprocs
        self.user = user
        self.pkey = pkey
        self.port = port
        self.ppn = ppn
        self.hostfile = hostfile
        self._set_hostfile(hostfile=hostfile, hosts=hosts)
        self.env = env
        self.basic_env = {}
        self._set_env(env)
        self.cwd = cwd
        self.sudo = sudo
        self.sudoenv = sudoenv
        self.sleep_ms = sleep_ms
        self.collect_output = collect_output
        self.pipe_stdout = pipe_stdout
        self.pipe_stderr = pipe_stderr
        self.hide_output = hide_output
        self.exec_async = exec_async
        self.stdin = stdin
        self.do_dbg = do_dbg
        self.dbg_port = dbg_port
        self.strict_ssh = strict_ssh
        self.keys = ['exec_type', 'nprocs', 'ppn', 'user', 'pkey', 'port',
                     'hostfile', 'env', 'sleep_ms', 'sudo', 'sudoenv',
                     'cwd', 'hosts', 'collect_output',
                     'pipe_stdout', 'pipe_stderr', 'hide_output',
                     'exec_async', 'stdin', 'do_dbg', 'dbg_port', 'strict_ssh']

    def _set_env(self, env):
        if env is None:
            self.env = {}
        else:
            self.env = env
        basic_env = [
            'PATH', 'LD_LIBRARY_PATH', 'LIBRARY_PATH', 'CMAKE_PREFIX_PATH',
            'PYTHONPATH', 'CPATH', 'INCLUDE', 'JAVA_HOME'
        ]
        self.basic_env = {}
        for key in basic_env:
            if key not in os.environ:
                continue
            self.basic_env[key] = os.getenv(key)
        for key, val in self.basic_env.items():
            if key not in self.env:
                self.env[key] = val
        self.basic_env.update(self.env)
        if 'LD_PRELOAD' in self.basic_env:
            del self.basic_env['LD_PRELOAD']

    def _set_hostfile(self, hostfile=None, hosts=None):
        if hostfile is not None:
            if isinstance(hostfile, str):
                self.hostfile = Hostfile(hostfile=hostfile)
            elif isinstance(hostfile, Hostfile):
                self.hostfile = hostfile
            else:
                raise Exception('Hostfile is neither string nor Hostfile')
        if hosts is not None:
            if isinstance(hosts, list):
                self.hostfile = Hostfile(all_hosts=hosts)
            elif isinstance(hosts, str):
                self.hostfile = Hostfile(all_hosts=[hosts])
            elif isinstance(hosts, Hostfile):
                self.hostfile = hosts
            else:
                raise Exception('Host set is neither str, list or Hostfile')

        if hosts is not None and hostfile is not None:
            raise Exception('Must choose either hosts or hostfile, not both')

        if self.hostfile is None:
            self.hostfile = Hostfile()

    def mod(self, **kwargs):
        self._mod_kwargs(kwargs)
        return ExecInfo(**kwargs)

    def _mod_kwargs(self, kwargs):
        for key in self.keys:
            if key not in kwargs and hasattr(self, key):
                kwargs[key] = getattr(self, key)

    def copy(self):
        return self.mod()


class Executable(ABC):
    """
    An abstract class representing a class which is intended to run
    shell commands. This includes SSH, MPI, etc.
    """

    def __init__(self):
        self.exit_code = None
        self.stdout = ''
        self.stderr = ''
        self.jutil = JutilManager.get_instance()

    def failed(self):
        return self.exit_code != 0

    @abstractmethod
    def set_exit_code(self):
        pass

    @abstractmethod
    def wait(self):
        pass

    def smash_cmd(self, cmds, sudo, basic_env, sudoenv):
        """
        Convert a list of commands into a single command for the shell
        to execute.

        :param cmds: A list of commands or a single command string
        :param prefix: A prefix for each command
        :param sudo: Whether or not root is required
        :param basic_env: The environment to forward to the command
        :param sudoenv: Whether sudo supports environment forwarding
        :return:
        """
        env = None
        if sudo:
            env = ''
            if sudoenv:
                env = [f'-E {key}=\"{val}\"' for key, val in
                       basic_env.items()]
                env = ' '.join(env)
            env = f'sudo {env}'
        if not isinstance(cmds, (list, tuple)):
            cmds = [cmds]
        if env is not None:
            cmds = [f'{env} {cmd}' for cmd in cmds]
        return ';'.join(cmds)

    def wait_list(self, nodes):
        for node in nodes:
            node.wait()

    def smash_list_outputs(self, nodes):
        """
        Combine the outputs of a set of nodes into a single output.
        For example, used if executing multiple commands in sequence.

        :param nodes:
        :return:
        """
        self.stdout = '\n'.join([node.stdout for node in nodes])
        self.stderr = '\n'.join([node.stderr for node in nodes])

    def per_host_outputs(self, nodes):
        """
        Convert the outputs of a set of nodes to a per-host dictionary.
        Used if sending commands to multiple hosts

        :param nodes:
        :return:
        """
        self.stdout = {}
        self.stderr = {}
        self.stdout = {node.addr: node.stdout for node in nodes}
        self.stderr = {node.addr: node.stderr for node in nodes}

    def set_exit_code_list(self, nodes):
        """
        Set the exit code from a set of nodes.

        :param nodes: The set of execution nodes that have been executed
        :return:
        """
        for node in nodes:
            if node.exit_code:
                self.exit_code = node.exit_code

    def get_dbg_cmd(self, cmd, exec_info):
        """
        Get the command to debug a program

        :param cmd: the command to debug
        :param exec_info: exec information
        :return: the debug command
        """
        dbg_port = exec_info.dbg_port
        preload = ""
        if 'LD_PRELOAD' in exec_info.env:
            exec_info.env = exec_info.env.copy()
            preload = exec_info.env['LD_PRELOAD']
            del exec_info.env['LD_PRELOAD']
        if len(preload):
            return f'gdbserver localhost:{dbg_port} env LD_PRELOAD={preload} {cmd}'
        else:
            return f'gdbserver localhost:{dbg_port} {cmd}'

