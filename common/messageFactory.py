from proto.common_messages_pb2 import MessageWrapper

class UnsupportedMessageType(Exception):
    pass


class MessageFactory():
    @classmethod
    def fromString(cls, data):
        messageWrapper = MessageWrapper()
        messageWrapper.ParseFromString(data)
        messageOut = None
        for messageClass in cls.messageClasses:
            if ( messageWrapper.message.Is(messageClass.DESCRIPTOR) ):
                messageOut = messageClass()
                break
        else:
            raise UnsupportedMessageType()
        messageWrapper.message.Unpack(messageOut)
        return messageOut


    @classmethod
    def toString(cls, msg):
        messageWrapper = MessageWrapper()
        messageWrapper.message.Pack(msg)
        return messageWrapper.SerializeToString()