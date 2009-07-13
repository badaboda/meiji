import sys
sys.path.append('./gen-py')

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from meiji import RouterProxy

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

  #router.create("c1")

  #router.destroy(1)

  router.send_as_raw("c1","hello\n")
  print 'send()'

  # Close!
  transport.close()

except Thrift.TException, tx:
  print tx

