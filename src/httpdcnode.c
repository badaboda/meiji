#include <sys/types.h>
#include <sys/time.h>
#include <sys/queue.h>
#include <stdlib.h>
#include <err.h>
#include <event.h>
#include <evhttp.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
 
#include "erl_interface.h"
#include "ei.h"
 
#include <pthread.h>
 
#define BUFSIZE 1024
#define MAXUSERS (17*65536) // C1024K
 
// List of current http requests by uid:
struct evhttp_request * clients[MAXUSERS+1];
// Memory to store uids passed to the cleanup callback:
int slots[MAXUSERS+1];
 
// called when user disconnects
void cleanup(struct evhttp_connection *evcon, void *arg)
{
    int *uidp = (int *) arg;
    fprintf(stderr, "disconnected uid %d\n", *uidp);
    clients[*uidp] = NULL;
}
 
// handles http connections, sets them up for chunked transfer,
// extracts the user id and registers in the global connection table,
// also sends a welcome chunk.
void request_handler(struct evhttp_request *req, void *arg)
{
        static int uid = 0;

        struct evbuffer *buf;
        buf = evbuffer_new();
        if (buf == NULL){
            err(1, "failed to create response buffer");
        }
 
        evhttp_add_header(req->output_headers, "Content-Type", "text/html; charset=utf-8");
 
        if(uid > MAXUSERS){
            evbuffer_add_printf(buf, "Max uid allowed is %d", MAXUSERS);
            evhttp_send_reply(req, HTTP_SERVUNAVAIL, "We ran out of numbers", buf);
            evbuffer_free(buf);
            return;
        }
 
        evhttp_send_reply_start(req, HTTP_OK, "OK");
        // Send welcome chunk:
        evbuffer_add_printf(buf, "Welcome, Url: ‘%s’ Id: %d\n", evhttp_request_uri(req), uid);
        evhttp_send_reply_chunk(req, buf);
        evbuffer_free(buf);
 
        // put reference into global uid->connection table:
        clients[uid++] = req;
        // set close callback
        evhttp_connection_set_closecb( req->evcon, cleanup, &slots[uid] );
}
 
 
// runs in a thread - the erlang c-node stuff
// expects msgs like {uid, msg} and sends a a ‘msg’ chunk to uid if connected
void cnode_run()
{
    int fd;                                  /* fd to Erlang node */
    int got;                                 /* Result of receive */
    unsigned char buf[BUFSIZE];              /* Buffer for incoming message */
    ErlMessage emsg;                         /* Incoming message */
 
    ETERM *uid, *msg;
 
    erl_init(NULL, 0);
 
    if (erl_connect_init(1, "secretcookie", 0) == -1)
        erl_err_quit("erl_connect_init");
 
    if ((fd = erl_connect("httpdmaster@localhost")) < 0)
        erl_err_quit("erl_connect");
 
    fprintf(stderr, "Connected to httpdmaster@localhost\n\r");
 
    struct evbuffer *evbuf;
 
    while (1) {
        got = erl_receive_msg(fd, buf, BUFSIZE, &emsg);
        if (got == ERL_TICK) {
            continue;
        } else if (got == ERL_ERROR) {
            fprintf(stderr, "ERL_ERROR from erl_receive_msg.\n");
            break;
        } else {
            if (emsg.type == ERL_REG_SEND) {
                // get uid and body data from eg: {123, <<"Hello">>}
                msg = erl_element(2, emsg.msg);
                char *body = (char *) ERL_BIN_PTR(msg);
                int body_len = ERL_BIN_SIZE(msg);
                // Is this userid connected?
                int l_uid = uid;
                while (--l_uid >= 0) {
                    if(clients[l_uid]){
                        fprintf(stderr, "Sending %d bytes to uid %d\n", body_len, userid);
                        evbuf = evbuffer_new();
                        evbuffer_add(evbuf, (const void*)body, (size_t) body_len);
                        evhttp_send_reply_chunk(clients[userid], evbuf);
                        evbuffer_free(evbuf);
                    }else{
                        fprintf(stderr, "Discarding %d bytes to uid %d - user not connected\n",
                                body_len, userid);
                        // noop
                    }
                }
                erl_free_term(emsg.msg);
                erl_free_term(uid);
                erl_free_term(msg);
            }
        }
    }
    // if we got here, erlang connection died.
    // this thread is supposed to run forever
    // TODO - gracefully handle failure / reconnect / etc
    pthread_exit(0);
}
 
int main(int argc, char **argv)
{
    // Launch the thread that runs the cnode:
    pthread_attr_t tattr;
    pthread_t helper;
    int status;
    pthread_create(&helper, NULL, cnode_run, NULL);
 
    int i;
    for(i=0;i<=MAXUSERS;i++) slots[i]=i;
    // Launch libevent httpd:
    struct evhttp *httpd;
    event_init();
    httpd = evhttp_start("0.0.0.0", 8000);
    evhttp_set_gencb(httpd, request_handler, NULL);
    event_dispatch();
    // Not reached, event_dispatch() shouldn’t return
    evhttp_free(httpd);
    return 0;
}

