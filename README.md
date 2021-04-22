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

Version 1.0 and 1.1 are supported, while 1.2 is not intentionally
because I don't like '\r\n', plus it doesn't bring many new features.

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

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
