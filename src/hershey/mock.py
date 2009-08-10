import types
import feed

class MockJavascriptConsumer(feed.JavascriptSysoutConsumer):
    def __init__(self):
        feed.JavascriptSysoutConsumer.__init__(self)
        self.lst = []
    def emit(self, s):
        self.lst.append(s)

class MockDatum(feed.RelayDatum):
    def __init__(self, json_path, delta_feed):
        assert type(delta_feed)==types.ListType
        self._json_path=json_path
        self.rows=self.delta_feed=delta_feed

    def json_path(self):
        return self._json_path

    def ensure_rows(self):
        pass

class MockDatumAsAtom(MockDatum, feed.RelayDatumAsAtom):
    pass

class MockDatumAsList(MockDatum, feed.RelayDatumAsList):
    pass
