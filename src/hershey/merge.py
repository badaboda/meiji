import types

def merge(org, delta):
    (tag, data)=delta
    if tag=='insert':
        return _merge_insert(org, data)
    elif tag=='delete':
        return _merge_delete(org, data)
    else:
        raise NotImplementedError

def _merge_insert(org, data):
    if type(org)==types.DictType:
        for k in data.keys():
            if org.has_key(k):
                org[k]=_merge_insert(org[k], data[k])
            else:
                org[k]=data[k]
        return org
    elif type(org)==types.ListType:
        return org
    else:
        return org

def _merge_delete(org, data):
    _org = org
    path_to_leaf = []
    # find leaf
    while True:
        keys=data.keys()
        assert len(keys) == 1
        path_to_leaf.append(keys[0])
        data=data[keys[0]]
        if type(data)!=types.DictType:
            break
    for k in path_to_leaf[:-2]:
        org=org[k]

    del org[path_to_leaf[-2]]
    return _org

