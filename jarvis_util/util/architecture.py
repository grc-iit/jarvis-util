from jarvis_util import Hostfile
from jarvis_util.shell.slurm import Slurm, SlurmInfo


class Service:
    def __init__(self, name, num_nodes, color_distribution):
        self.name = name
        self.num_nodes = num_nodes
        self.color_distribution = self.parse_distribution(color_distribution)
        self.allocated_nodes = []
        self.hostfile = None
        self.slurm = None
        self.validate()

    @staticmethod
    def parse_distribution(distribution):
        """Parse a color distribution string into a dictionary."""
        colors = distribution.split("+")
        color_dict = {}
        for color in colors:
            color_split = color.split(":")
            if len(color_split) == 1:  # only color is specified, no number
                color_dict[color_split[0]] = None
            else:
                color_dict[color_split[0]] = int(color_split[1])
        return color_dict

    def add_nodes(self, nodes: list):
        self.allocated_nodes.extend(nodes)

    def generate_hostfile(self):
        self.hostfile = Hostfile(all_hosts=self.allocated_nodes)

    def validate(self):
        """Validate that the total nodes requested through the color distribution match num_nodes."""
        total_nodes_requested = sum(value for value in self.color_distribution.values() if value is not None)
        if len(self.color_distribution) == 1:
            # If only one color is used, it's assumed that the entire color will be attributed to num_nodes
            color = next(iter(self.color_distribution))
            self.color_distribution[color] = self.num_nodes
        elif total_nodes_requested != self.num_nodes:
            raise ValueError(f"Total nodes requested for service {self.name} ({total_nodes_requested}) "
                             f"does not match num_nodes ({self.num_nodes})")


class Architecture:
    services: dict[str, Service]
    color_groups: dict[str, int]

    def __init__(self):
        self.services = dict()
        self.color_groups = dict()

    def __init__(self, services: list):
        self.__init__()
        self.add_service(services)

    def __enter__(self):
        self.schedule_services()

    def __exit__(self):
        self.slurm.exit()

    def add_service(self, service: Service):
        if service.name in self.services:
            raise ValueError("Service already defined {0}".format(service.name))
        self.services[service.name] = service

    def add_service(self, service_name: str, num_nodes: int, color: str):
        self.add_service(Service(service_name, num_nodes, color))

    def add_service(self, services: list):
        for service in services:
            self.add_service(service)

    def hostfile(self, name: str):
        return self.services[name].hostfile

    def get_num_nodes_needed(self):
        for service in self.services.values():
            for color, num_nodes in service.color_distribution.items():
                if color in self.color_groups:
                    if self.color_groups[color] < num_nodes:
                        self.color_groups[color] = num_nodes
                else:
                    self.color_groups[color] = num_nodes
        return sum(self.color_groups.values())

    def schedule_services(self):
        total_nodes = self.get_num_nodes_needed()
        self.slurm = Slurm(SlurmInfo(nnodes=total_nodes))
        self.slurm.allocate()
        node_list = self.slurm.get_nodes()

        if len(node_list) != total_nodes:
            raise EnvironmentError("Slurm could not reserve enough nodes")

        # Create pools
        pools = dict()
        start = 0
        for color, num_nodes in self.color_groups.items():
            pools[color] = node_list[start:start + num_nodes]
            start += num_nodes

        # Assign nodes from pools to services
        for service in self.services.values():
            for color, num_nodes in service.color_distribution.items():
                # Get nodes from color pool
                pool_nodes = pools[color]
                service_nodes = pool_nodes[:num_nodes]
                # Update service with nodes
                service.add_nodes(service_nodes)
            service.generate_hostfile()

# services = [
#     Service("OrangeFS_client", 16, "RED:16"),
#     Service("OrangeFS_server", 16, "BLUE:16"),
#     Service("Application", 16, "RED:4+BLUE:4+GREEN:8"),
# ]
# arch = Architecture(services)
# arch.schedule_services()
# hostfile = arch.hostfile("Application")
# exec_info = MpiExecInfo(nprocs=num_processes, ppn=2, hostfile=hostfile, cwd=self.INSTALL_PATH)
# simulation = MpiExec(f"./adios2-gray-scott settings-files.json", exec_info)
