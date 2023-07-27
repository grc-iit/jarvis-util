from jarvis_util.util.hostfile import Hostfile
from jarvis_util.shell.slurm import Slurm, SlurmInfo
import pathlib
from unittest import TestCase


class TestSlurm(TestCase):
    def test_alloc(self):
        num_nodes = 2
        slurm_info = SlurmInfo(nnodes=num_nodes)
        slurm = Slurm(slurm_info=slurm_info)
        slurm.allocate()
        self.assertTrue(num_nodes == slurm.get_node_counts())

    def test_slurm_hostfile(self):
        num_nodes = 2
        slurm_info = SlurmInfo(nnodes=num_nodes)
        slurm = Slurm(slurm_info=slurm_info)
        slurm.allocate()
        hostfile = slurm.get_hostfile()
        self.assertTrue(len(hostfile.hosts) == slurm.get_node_counts())