import types 
import feed

class MockJavascriptConsumer:
    def __init__(self):
        self.lst = []
    def feed(self, tag, json_path, dict):
        if tag in ['insert', 'replace']:
            self.lst.append(self.insert_javascript(json_path, dict))
        elif tag=='delete':
            self.lst.append(self.delete_javascript(json_path, dict))
        else:
            raise NotImplementedError

    def insert_javascript(self, json_path, dict):
        return "%s=%s;" % (self._js_variable("db", json_path, dict), str(dict))
        
    def delete_javascript(self, json_path, dict):
        return "delete %s;" % (self._js_variable("db", json_path, dict))

    def _js_variable(self, root, json_path, dict):
        keys=json_path.split(":")
        js_key_parts=[root]
        for i, k in enumerate(keys):
            is_placeholder=lambda s: s.startswith("**")
            if is_placeholder(k):
                js_key_parts.append("['%s']" % dict[k[2:]])    
            else:
                js_key_parts.append(".%s" % k)
        return ''.join(js_key_parts) 

class MockDatum:
    def __init__(self, json_path, delta_feed):
        assert type(delta_feed)==types.ListType
        self._json_path=json_path
        self.delta_feed=delta_feed
    def json_path(self):
        return self._json_path
    def as_delta_generator_input(self):
        return feed.list_of_dict_to_list_of_pairs(self.delta_feed)

