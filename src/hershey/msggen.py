#!/usr/bin/env python
# vim: et sts=4 sw=4 ts=4 ai :
import time
import router_client

interval = 3.0

if __name__=='__main__':
    game_id = "c1"
    router_client.create(game_id)
    while True:
        start=time.time()
        router_client.send_as_raw(game_id, "x"*512+"\n")
        elapsed=time.time()-start
        print 'finished router_client.send_as_raw'
        remain=interval-elapsed
        if remain >= 0:
            time.sleep(remain)
        else:
            print 'warning: router_client.send elasped (%s)' % str(elapsed)
