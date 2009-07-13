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
        transport = TSocket.TSocket('localhost', 9999)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        router = RouterProxy.Client(protocol)
        transport.open()

        router.send_as_raw(channel, msg)

        transport.close()
    except Thrift.TException, tx:
        print '%s' % (tx.message)
           
def create(channel):
    try:
        transport = TSocket.TSocket('localhost', 9999)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        router = RouterProxy.Client(protocol)
        transport.open()

        router.create(channel)

        transport.close()
    except Thrift.TException, tx:
        print '%s' % (tx.message)


if __name__=="__main__":
    create("20090701HHSK0")
