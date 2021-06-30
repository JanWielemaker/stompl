# A STOMP client for SWI-Prolog

A [STOMP](http://stomp.github.io) client.

> This is a fork from https://github.com/honnix/stompl by Hongxin Liang.
> (hxliang1982@gmail.com)  The  current  implementation   is  an  almost
> complete  rewrite  of  the  frame   (de)serialization  and  connection
> management with significant changes to the API.


## Installation

Using SWI-Prolog 7 or later.

    ?- pack_install('https://github.com/JanWielemaker/stompl.git').

Source     code     available     and     pull     requests     accepted
[here](https://github.com/JanWielemaker/stompl).

## Supported STOMP versions

STOMP versions 1.0, 1.1 and 1.2 are supported.

## Example

```
:- module(ex, []).

:- use_module(library(stompl)).

connect(Connection) :-
    stomp_connection('127.0.0.1':32772,
                     '/',
                     _{'heart-beat': '5000,5000',
                       login: guest,
                       passcode: guest
                      },
                     on_frame, Connection),
    stomp_connect(Connection).

on_frame(connected, Connection, _Header, _Body) :-
    ...
on_frame(message, Connection, Header, Body) :-
    ...
on_frame(disconnected, Connection, _Header, _Body) :-
    ...
on_frame(error, Connection, Header, Body) :-
    ...
on_frame(heartbeat_timeout, Connection, _Header, _Body) :-
    ...

% Sending messages

    ...
    stomp_send_json(Connection, '/queue/test', _{hello: "World"}).
```

For more examples, please check source code under `examples` directory.

## License

Licensed under the BSD-2 license
