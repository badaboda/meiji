include ../support/include.mk
ERL_HOME=/usr/local/lib/erlang/lib/erl_interface-3.6.1

all: $(EBIN_FILES)

debug:
	$(MAKE) DEBUG=-DDEBUG

clean:
	rm -rf $(EBIN_FILES)

httpdcnode: 
	LD_LIBRARY_PATH=/usr/local/lib gcc -o httpdcnode \
		-I $(ERL_HOME)/include httpdcnode.c -L $(ERL_HOME)/lib -lerl_interface -lei -levent
