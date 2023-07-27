"""
This module contains various wrappers over typical filesystem commands seen
in shell scripts. This includes operations such as creating directories,
changing file permissions, etc.
"""
import shutil
import re
from jarvis_util.shell.exec import Exec
from jarvis_util.shell.exec_info import ExecInfo
from jarvis_util.util.hostfile import Hostfile

"""
Contains all information needed to start a slurm allocation
"""


class SlurmInfo:

    def __init__(self, nnodes: int, node_list="", partition="", exclusive=True):
        self.partition = partition
        self.exclusive = exclusive

        self.nnodes = nnodes
        self.node_list = node_list

        if self.node_list:
            nodes_on_list = self.count_expansion(node_list)
            if nodes_on_list > nnodes:
                raise Exception("Node list requested more nodes than defined on nnodes")

    @staticmethod
    def count_expansion(s: str):
        # Check for range in brackets.
        match = re.search(r'\[(\d+)-(\d+)\]', s)
        if match:
            start, end = map(int, match.groups())
            return end - start + 1

        # Check for single value without brackets.
        match = re.search(r'(\d+)(?=[^-]*$)', s)
        if match:
            return 1

        # Return 0 if there are no matches.
        return 0


class Slurm:
    """
    Create directories + subdirectories.
    """

    def __init__(self, slurm_info):
        """
        Create directories + subdirectories. Does not fail if the dirs
        already exist.

        :param paths: A list of paths or a single path string.
        :param exec_info: Info needed to execute the mkdir command
        """
        self.command = ""
        self.nodes = ""
        self.slurm_info = slurm_info
        self.hostfile = None
        self.job_id = 0
        if shutil.which('salloc') is None:
            raise EnvironmentError('salloc not found on the system.')

    def __del__(self):
        self.exit()

    def allocate(self, exec_info=None):
        if exec_info is None:
            exec_info = ExecInfo(collect_output=True)
        else:
            exec_info.collect_output = True

        base_cmd = "salloc --no-shell "
        if self.slurm_info.nnodes:
            base_cmd += f'-n {self.slurm_info.nnodes} '
        if self.slurm_info.node_list:
            base_cmd += f'--nodelist={self.slurm_info.node_list} '
        if self.slurm_info.partition:
            base_cmd += f'-p {self.slurm_info.partition} '
        if self.slurm_info.exclusive:
            base_cmd += f'--exclusive '
        self.command = base_cmd
        node = Exec(base_cmd, exec_info)
        self.job_id = re.findall('\d+', node.stderr['localhost'])[0]

    def get_nodes(self, exec_info=None) -> list:
        if exec_info is None:
            exec_info = ExecInfo(collect_output=True)
        else:
            exec_info.collect_output = True
        node = Exec(f'scontrol show hostname', exec_info)
        print(node.stdout.items)
        print(node.stderr.items)
        self.nodes = node.stdout['localhost']
        return self.nodes

    def get_node_list(self):
        if not self.nodes:
            self.get_nodes()
        return self.nodes.splitlines()

    def get_node_counts(self):
        if not self.nodes:
            self.get_nodes()
        return len(self.nodes.splitlines())

    def get_command(self):
        return self.command

    def get_hostfile(self):
        if self.hostfile is None:
            if not self.nodes:
                self.get_nodes()
            self.hostfile = Hostfile(all_hosts=self.get_node_list())
        return self.hostfile

    def exit(self, exec_info=None):
        Exec(f'scancel {self.job_id}', exec_info)
