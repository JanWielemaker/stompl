:- module(ex1,
          [ connect/1
          ]).

:- use_module(user:library(stompl)).    % make available on toplevel
:- use_module(library(stompl)).

% :- debug(stompl(_)).
% :- debug(ex1).

connect(Connection) :-
    stomp_connection('127.0.0.1':32772,
                     '/',
                     _{'heart-beat': '5000,5000',
                       login: guest,
                       passcode: guest
                      },
                     on_frame, Connection),
    stomp_connect(Connection).

:- det((on_frame/4)).

on_frame(connected, Connection, _Header, _Body) =>
    Destination = '/queue/test',
    stomp_subscribe(Connection, Destination, 0, _{}).
on_frame(message, Connection, Header, Body) =>
    debug(ex1, 'message from connection ~p~n~p~n~p',
          [Connection, Header, Body]),
    format('Received ~p~n', [Body]).
on_frame(disconnected, Connection, _Header, _Body) =>
    debug(ex1, 'on_disconnected from connection ~p', [Connection]),
    stomp_teardown(Connection).
on_frame(error, Connection, Header, Body) =>
    debug(ex1, 'on_error from connection ~p~n~p~n~p',
          [Connection, Header, Body]).
on_frame(heartbeat_timeout, Connection, _Header, _Body) =>
    debug(ex1, 'on_heartbeat_timeout from connection ~p', [Connection]),
    stomp_teardown(Connection).
on_frame(Event, Connection, _Header, _Body) =>
    debug(ex1, 'Unknown event ~p on connection ~p',
          [Event, Connection]).

