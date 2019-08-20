class Address():
    def __init__(self, host:str, port:int):
        self.host = host
        self.port = port

    def __str__(self):
        return f"{self.host}:{self.port}"

    def __repr__(self):
        return f"Address({self.host}, {self.port})"

    def __eq__(self, other):
        return (self.host == other.host) and (self.port == other.port)

    def packProtoAddress(self, protoAddr):
        protoAddr.host = self.host
        protoAddr.port = self.port

    @classmethod
    def fromProtoAddress(self, protoAddr):
        return Address(protoAddr.host, protoAddr.port)