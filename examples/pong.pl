:- module(pong,
          []).
:- use_module(library(stompl)).
:- use_module(library(main)).

:- initialization(main, main).

% :- debug(stompl(_)).
% :- debug(pong).

main(_) :-
    connect(_),
    thread_get_message(_).

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
    stomp_subscribe(Connection, '/queue/pong', 0, _{}).
on_frame(message, Connection, _Header, _{pong:Count}) :-
    debug(pong, 'Got ~D', [Count]),
    stomp_send_json(Connection, '/queue/ping', _{}, _{ping:Count}).


