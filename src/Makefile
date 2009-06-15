include ../support/include.mk

# OSX에서 macport로 erlang을 설치했을 때는 이렇게 컴파일 make
#   make httpdcnode PREFIX=/opt
#
# Linux에서는 보통 
#   make httpdcnode 하면 컴파일 된다 

PREFIX=/usr
ERL_HOME=$(PREFIX)/local/lib/erlang/lib/erl_interface-3.6.1

all: $(EBIN_FILES)

debug:
	$(MAKE) DEBUG=-DDEBUG

clean:
	rm -rf $(EBIN_FILES)

httpdcnode: 
	LD_LIBRARY_PATH=$(PREFIX)/local/lib gcc -o httpdcnode \
        -I$(PREFIX)/local/include \
        -I$(ERL_HOME)/include httpdcnode.c \
        -L$(ERL_HOME)/lib -lei -lerl_interface \
        -dynamic -L$(PREFIX)/local/lib -levent
