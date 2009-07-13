# vim: set noet :
.PHONY: all compile run 

ERL_THRIFT?=${HOME}/thrift/lib/erl
EFILES=$(wildcard *.erl)

all: run

gen-erl: meiji.thrift
	thrift --gen py --gen erl $<

compile: gen-erl ${EFILES}
	erlc -I ${ERL_THRIFT}/include -I ./gen-erl -o ./gen-erl ./gen-erl/*.erl && \
	  erlc -I ${ERL_THRIFT}/include -I ./gen-erl ${EFILES}

run: compile
	erl +K true -pa ${ERL_THRIFT}/ebin -pa ./gen-erl -pa ../../ebin -setcookie abc -sname router_stub -s router_stub start

clean:
	rm *.beam
	rm -rf gen-erl gen-py
