# vim: fileencoding=utf-8 :
import unittest, MySQLdb

import merge, feed, mock
from feed import kbo


class DeltaGeneratorWithKeyedDatumTest(unittest.TestCase):
    def setUp(self):
        self.consumer = mock.MockJavascriptConsumer()
        self.delta=feed.DeltaGenerator(self.consumer)
        self.KEYED_JSON_PATH="registry:players:**pcode:profile"

    def tearDown(self):
        pass

    def test_insert_keyed_datum(self):
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" }]))
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                              { "pcode": "98260", "name": "정삼흠" }]))
        self.assertEquals(
            ["db.registry.players['98260'].profile={'pcode': '98260', 'name': %s};"%repr('정삼흠')],
            self.consumer.lst)

    def test_insert_list_datum(self):
        self.delta.feed(mock.MockDatumAsList("livetext", [{ "a": 1, "b": 2 }]))
        self.delta.feed(mock.MockDatumAsList("livetext", [{ "a": 1, "b": 2 },
                                                          { "a": 2, "b" : 2}]))
        self.assertEquals(
            ["db.livetext.push({'a': 2, 'b': 2});"], self.consumer.lst)

    def test_mockdatum_as_atom(self):
        self.delta.feed(mock.MockDatumAsAtom("scoreboard", [{"a": 1, "b": 2}]))
        self.delta.feed(mock.MockDatumAsAtom("scoreboard", [{"a": 2, "b": 3}]))
        self.assertEquals(
            ['db.scoreboard.a=2;', 'db.scoreboard.b=3;'], self.consumer.lst)

    def test_waiting_batters(self):
        self.delta.feed(mock.MockDatumAsList("waiting_batters", [{ "a": 1, "b": 1 },
                                                          { "a": 2, "b": 2 },
                                                          { "a": 3, "b": 3 }]))
        self.delta.feed(mock.MockDatumAsList("waiting_batters", [{ "a": 2, "b": 2 },
                                                          { "a": 3, "b": 3 },
                                                          { "a": 4, "b": 4 }]))
        self.assertEquals(
            ["db.waiting_batters.splice(0, 1);", "db.waiting_batters.push({'a': 4, 'b': 4});"], self.consumer.lst)

        #self.assertEquals(
        #    ["db.waiting_batters=db.waiting_batters.slice(1)+{'a': 4, 'b': 4});", "db.waiting_batters.push({'a': 4, 'b': 4});"], self.consumer.lst)

        #self.assertEquals(
        #    ["db.waiting_batters=[{'a': 2, 'b': 2}, {'a': 3, 'b': 3}, {'a': 4, 'b': 4}]);"], self.consumer.lst)

    def test_delete(self):
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" }]))
        self.assertEquals(
            ["delete db.registry.players['98260'].profile;"],
            self.consumer.lst)

    def test_replace(self):
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.delta.feed(mock.MockDatum(self.KEYED_JSON_PATH, [{ "pcode": "79260", "name": "장원삼2" },
                                                        { "pcode": "98260", "name": "정삼흠" }]))
        self.assertEquals(
            ["db.registry.players['79260'].profile={'pcode': '79260', 'name': %s};"%repr('장원삼2')],
            self.consumer.lst)

class DictMergeTest(unittest.TestCase):
    def test_merge_insert(self):
        initial_data = \
            { "registry":
                { "players":
                    { "79260":
                        { "profile":
                            { "name": "장원삼" }}}}}
        delta_spec = \
             ('insert',
               { "registry":
                 { "players":
                   { "98260":
                     { "profile":
                        { "name": "정삼흠" }}}}})

        n=merge.merge(initial_data, delta_spec)
        self.assertEquals(
            { "registry":
                { "players":
                    { "79260": { "profile":
                                   { "name": "장원삼" }},
                      "98260": { "profile":
                                   { "name": "정삼흠" }}}}}, n)

    def test_merge_delete(self):
        initial_data = \
            { "registry":
                { "players":
                    { "79260":
                        { "profile":
                            { "name": "장원삼" }},
                      "98260":
                        { "profile":
                            { "name": "정삼흠" }}}}}
        delta_spec = \
             ('delete',
               { "registry":
                 { "players":
                   { "79260":
                     { "profile":
                        { "name": "정삼흠" }}}}})

        n=merge.merge(initial_data, delta_spec)
        self.assertEquals(
            { "registry":
                { "players":
                    { "79260": {},
                      "98260": { "profile":
                                   { "name": "정삼흠" }}}}}, n)


if __name__=='__main__':
    unittest.main()
