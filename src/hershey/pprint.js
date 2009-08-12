function StringIO() {
    var lst=[];
    return {
        write: function(s) {
            lst.push(s)
        },
        getvalue: function() {
            return lst.join('') 
        }
    }
}

function pprint(o) {
    var io=new StringIO();
    _pprint(o, io)
    print(io.getvalue()) 
}

function _pprint(o, io){
    t=typeof(o)
    if (t=="object") {
        io.write('{')
        for (var k in o) {
            io.write('"'+k+'":')
            _pprint(o[k], io) 
            io.write(',')
        }
        io.write('}')
    } else {
       io.write(o) 
    }
}

