import unittest
import feed
import mock

class DeltaTest(unittest.TestCase):
    def setUp(self):
        self.game_code ='20090701HHSK0'
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)

    def testDeltaLoop(self):
        import delta
        delta.loop(self.delta, self.game_code)
        delta.loop(self.delta, self.game_code)

if __name__=='__main__':
    unittest.main()
