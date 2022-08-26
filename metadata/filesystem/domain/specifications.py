from . import model
from dfs_shared.domain import specification


class DataNodeByHostAndPortSpec(specification.Specification):
    type = model.DataNode
    
    def __init__(self, host, port):
        def expression(datanode):
            dhost = datanode.address.host
            dport = datanode.address.port
            return dhost == host and dport == port
        super().__init__(expression)