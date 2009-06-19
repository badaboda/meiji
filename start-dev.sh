#!/bin/sh
cd `dirname $0`
exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl -s reloader -s mochiconntest -sname 'mochiweb' +K true +P 5000000 -setcookie abc
