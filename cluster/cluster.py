from src.common.address import Address

class ClusterConfiguration():
    def __init__(self):
        self.joinAddr = Address('localhost', 8888)
        self.workAddr = Address('localhost', 8889)
        self.clientAddr = Address('localhost', 8890)