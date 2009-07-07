#include <sys/types.h>
#include <sys/time.h>
#include <sys/queue.h>
#include <stdlib.h>
#include <err.h>
#include <event.h>
#include <evhttp.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <time.h>
#include <pthread.h>
 
#define BUFSIZE 4096
#define NUMCONNS 62000
#define SERVERADDR "10.99.99.121"
#define SERVERPORT 8000
#define SLEEP_MS 10
 
char buf[BUFSIZE];
 
int bytes_recvd = 0;
int chunks_recvd = 0;
int closed = 0;
int connected = 0;

const char *addresses[] = {
    "10.99.99.3",
    "10.99.99.4",
    "10.99.99.5",
    "10.99.99.6",
    "10.99.99.7",
    "10.99.99.8",
    "10.99.99.9",
    "10.99.99.11",
    "10.99.99.12",
    "10.99.99.13",
    "10.99.99.14",
    "10.99.99.15",
    "10.99.99.16",
    "10.99.99.17",
    "10.99.99.18",
    "10.99.99.40",
    "10.99.99.41"
};
 
// called per chunk received
void chunkcb(struct evhttp_request * req, void * arg)
{
    int s = evbuffer_remove( req->input_buffer, &buf, BUFSIZE );
    //printf("Read %d bytes: %s\n", s, &buf);
    bytes_recvd += s;
    chunks_recvd++;
    if(connected >= NUMCONNS && chunks_recvd%10000==0)
        printf(">Chunks: %d\tBytes: %d\tClosed: %d\n", chunks_recvd, bytes_recvd, closed);
}
 
// gets called when request completes
void reqcb(struct evhttp_request * req, void * arg)
{
    closed++;
}
 
int main(int argc, char **argv)
{
    event_init();
    struct evhttp *evhttp_connection;
    struct evhttp_request *evhttp_request;
    char path[32]; // eg: "/test/123"
    int i,j,octet;
    for(j=0; j<17; j++){
        for(i=1;i<=NUMCONNS;i++) {
            evhttp_connection = evhttp_connection_new(SERVERADDR, SERVERPORT);
            evhttp_connection_set_local_address(evhttp_connection, (const char*)addresses[j]);
	    evhttp_connection_set_local_port(evhttp_connection, (unsigned short)(1024+i));
            evhttp_set_timeout(evhttp_connection, 864000); // 10 day timeout
            evhttp_request = evhttp_request_new(reqcb, NULL);
            evhttp_request->chunk_cb = chunkcb;
	    connected+=1;
            sprintf(&path, "/test/%d", 1);
            if(i%100==0)  
		printf("Req: %s\t->\t%s\n", addresses[j], &path);
            evhttp_make_request( evhttp_connection, evhttp_request, EVHTTP_REQ_GET, path );
            evhttp_connection_set_timeout(evhttp_request->evcon, 864000);
            event_loop( EVLOOP_NONBLOCK );
            if( connected % 200 == 0 )
                printf("\nChunks: %d\tBytes: %d\tConnected: %d\tClosed: %d\n", chunks_recvd, bytes_recvd, connected, closed);
            usleep(SLEEP_MS*1000);
        }
    }
    event_dispatch();
    return 0;
}

