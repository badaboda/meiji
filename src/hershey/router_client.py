#!/usr/bin/env python2.6
# vim: et sts=4 sw=4 ts=4 ai :
import sys
sys.path.append('../relay-thrift/gen-py')

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from meiji import RouterProxy


def send_as_raw(channel, msg):
    try:
        # Make socket
        transport = TSocket.TSocket('localhost', 9999)

        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)

        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client to use the protocol encoder
        router = RouterProxy.Client(protocol)

        # Connect!
        print 'transport.open()'
        transport.open()

        router.send_as_raw(channel, msg)
        print 'send_as_raw()'

        # Close!
        transport.close()

    except Thrift.TException, tx:
        print '%s' % (tx.message)
           
 
def create(channel):
    try:
        # Make socket
        transport = TSocket.TSocket('localhost', 9999)

        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)

        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client to use the protocol encoder
        router = RouterProxy.Client(protocol)

        # Connect!
        print 'transport.open()'
        transport.open()

        router.create(channel)
        print 'create'

        # Close!
        transport.close()

    except Thrift.TException, tx:
        print '%s' % (tx.message)



if __name__=="__main__":
    create("20090701HHSK0")
