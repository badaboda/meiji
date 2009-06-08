#!/bin/sh
cd `dirname $0`
exec erl -pa $PWD/ebin $PWD/deps/*/ebin -boot start_sasl -s reloader -s mochiconntest -name 'n2@sports-testweb2.internal.media.daum.net' +K true +P 134217727 -setcookie abc
