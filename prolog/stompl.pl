:- module(stompl, [
                   connection/2,    % +Address, -Connection
                   connection/3,    % +Address, +CallbackDict, -Connection
                   setup/1,         % +Connection
                   teardown/1,      % +Connection
                   connect/3,       % +Connection, +Host, +Headers
                   send/3,          % +Connection, +Destination, +Headers
                   send/4,          % +Connection, +Destination, +Headers, +Body
                   send_json/4,     % +Connection, +Destination, +Headers, +JSON
                   subscribe/4,     % +Connection, +Destination, +Id, +Headers
                   unsubscribe/2,   % +Connection, +Id
                   ack/3,           % +Connection, +MessageId, +Headers
                   nack/3,          % +Connection, +MessageId, +Headers
                   begin/2,         % +Connection, +Transaction
                   commit/2,        % +Connection, +Transaction
                   abort/2,         % +Connection, +Transaction
                   disconnect/2     % +Connection, +Headers
                  ]).

/** <module> STOMP client.
A STOMP 1.0 and 1.1 compatible client.

[stomp.py](https://github.com/jasonrbriggs/stomp.py)
is used as a reference for the implementation.

@author Hongxin Liang
@license Apache License Version 2.0
@see http://stomp.github.io/index.html
@see https://github.com/jasonrbriggs/stomp.py
*/

:- use_module(library(uuid)).
:- use_module(library(socket)).
:- use_module(library(apply)).
:- use_module(library(http/json)).

:- dynamic
    connection_mapping/2.

%% connected(+Address, -Connected) is det.
%% connected(+Address, +CallbackDict, -Connected) is det.
%
% Create a connection reference. The connection
% is not set up yet by this predicate.
% If =CallbackDict= is provided, it will be associated
% with the connection reference. Valid keys of the dict
% are:
% ==
% on_connected
% on_disconnected
% on_message
% on_heartbeat_timeout
% on_error
% ==
% When registering callbacks, both module name and predicate
% name shall be provided in the format of module:predicate.
% Valid predicate signatures for example could be:
% ==
% example:on_connected_handler(Connection, Headers, Body)
% example:on_disconnected_handler(Connection)
% example:on_message_handler(Connection, Headers, Body)
% example:on_heartbeat_timeout_handler(Connection)
% example:on_error_handler(Connection, Headers, Body)
% ==

connection(Address, Connection) :-
    connection(Address, Connection, _{}).

connection(Address, CallbackDict, Connection) :-
    uuid(Connection),
    asserta(connection_mapping(Connection,
                               _{
                                 address: Address,
                                 callbacks: CallbackDict
                                })).

%% setup(+Connection) is semidet.
%
% Set up the actual socket connection and start
% receiving thread.

setup(Connection) :-
    get_mapping_data(Connection, address, Address),
    tcp_connect(Address, Stream, []),
    set_stream(Stream, buffer_size(4096)),
    thread_create(receive(Connection, Stream), ReceiverThreadId, []),
    update_connection_mapping(Connection, _{receiver_thread_id: ReceiverThreadId, stream:Stream}).

%% teardown(+Connection) is semidet.
%
% Tear down the socket connection, stop receiving
% thread and heartbeat thread (if applicable).

teardown(Connection) :-
    get_mapping_data(Connection, receiver_thread_id, ReceiverThreadId),
    (   \+ thread_self(ReceiverThreadId),
        thread_property(ReceiverThreadId, status(running))
    ->  debug(stompl(connection), 'attempting to kill receive thread ~w', [ReceiverThreadId]),
        thread_signal(ReceiverThreadId, throw(kill))
    ;   true
    ),
    (   get_mapping_data(Connection, heartbeat_thread_id, HeartbeatThreadId)
    ->  (   thread_property(HeartbeatThreadId, status(running))
        ->  debug(stompl(connection), 'attempting to kill heartbeat thread ~w', [HeartbeatThreadId]),
            thread_signal(HeartbeatThreadId, throw(kill))
        )
    ),
    get_mapping_data(Connection, stream, Stream),
    catch(close(Stream), _, true), %% close it anyway
    debug(stompl(connection), 'retract connection mapping', []),
    retract(connection_mapping(Connection, _)).

%% connect(+Connectio, +Host, +Headers) is semidet.
%
% Send a =CONNECT= frame. Protocol version and heartbeat
% negotiation will be handled. =STOMP= frame is not used
% for backward compatibility.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#CONNECT_or_STOMP_Frame).

connect(Connection, Host, Headers) :-
    create_frame('CONNECT',
                 Headers.put(_{
                               'accept-version':'1.0,1.1', %% 1.2 doesn't bring much benefit but trouble
                               host:Host
                              }),
                 '', Frame),
    (   Heartbeat = Headers.get('heart-beat')
    ->  update_connection_mapping(Connection, _{'heart-beat':Heartbeat})
    ;   true
    ),
    send0(Connection, Frame).

%% send(+Connection, +Destination, +Headers) is semidet.
%% send(+Connection, +Destination, +Headers, +Body) is semidet.
%
% Send a =SEND= frame. If =content-type= is not provided, =text/plain=
% will be used. =content-length= will be filled in automatically.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#SEND).

send(Connection, Destination, Headers) :-
    send(Connection, Destination, Headers, '').

send(Connection, Destination, Headers, Body) :-
    create_frame('SEND', Headers.put(destination, Destination), Body, Frame),
    send0(Connection, Frame).

%% send_json(+Connection, +Destination, +Headers, +JSON) is semidet.
%
% Send a =SEND= frame. =JSON= can be either a JSON term or a dict.
% =content-type= is filled in automatically as =application/json=
% and =content-length= will be filled in automatically as well.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#SEND).

send_json(Connection, Destination, Headers, JSON) :-
    atom_json_term(Body, JSON, [as(atom)]),
    create_frame('SEND',
                 Headers.put(_{
                               destination:Destination,
                               'content-type':'application/json'
                              }),
                 Body, Frame),
    send0(Connection, Frame).

%% subscribe(+Connection, +Destination, +Id, +Headers) is semidet.
%
% Send a =SUBSCRIBE= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#SUBSCRIBE).

subscribe(Connection, Destination, Id, Headers) :-
    create_frame('SUBSCRIBE', Headers.put(_{destination:Destination, id:Id}), '', Frame),
    send0(Connection, Frame).

%% unsubscribe(+Connection, +Id) is semidet.
%
% Send an =UNSUBSCRIBE= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#UNSUBSCRIBE).

unsubscribe(Connection, Id) :-
    create_frame('UNSUBSCRIBE', _{id:Id}, '', Frame),
    send0(Connection, Frame).

%% ack(+Connection, +MessageId, +Headers) is semidet.
%
% Send an =ACK= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#ACK).

ack(Connection, MessageId, Headers) :-
    create_frame('ACK', Headers.put('message-id', MessageId), '', Frame),
    send0(Connection, Frame).

%% nack(+Connection, +MessageId, +Headers) is semidet.
%
% Send a =NACK= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#NACK).

nack(Connection, MessageId, Headers) :-
    create_frame('NACK', Headers.put('message-id', MessageId), '', Frame),
    send0(Connection, Frame).

%% begin(+Connection, +Transaction) is semidet.
%
% Send a =BEGIN= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#BEGIN).

begin(Connection, Transaction) :-
    create_frame('BEGIN', _{transaction:Transaction}, '', Frame),
    send0(Connection, Frame).

%% commit(+Connection, +Transaction) is semidet.
%
% Send a =COMMIT= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#COMMIT).

commit(Connection, Transaction) :-
    create_frame('COMMIT', _{transaction:Transaction}, '', Frame),
    send0(Connection, Frame).

%% abort(+Connection, +Transaction) is semidet.
%
% Send a =ABORT= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#ABORT).

abort(Connection, Transaction) :-
    create_frame('ABORT', _{transaction:Transaction}, '', Frame),
    send0(Connection, Frame).

%% disconnect(+Connection, +Headers) is semidet.
%
% Send a =DISCONNECT= frame.
% See [here](http://stomp.github.io/stomp-specification-1.1.html#DISCONNECT).

disconnect(Connection, Headers) :-
    create_frame('DISCONNECT', Headers, '', Frame),
    send0(Connection, Frame).

send0(Connection, Frame) :-
    send0(Connection, Frame, true).

send0(Connection, Frame, EndWithNull) :-
    (   EndWithNull
    ->  atom_concat(Frame, '\x00', Frame1)
    ;   Frame1 = Frame
    ),
    debug(stompl(send), 'frame to send~n~w', [Frame1]),
    get_mapping_data(Connection, stream, Stream),
    format(Stream, '~w', [Frame1]),
    flush_output(Stream).

create_frame(Command, Headers, Body, Frame) :-
    (   Body \= ''
    ->  atom_length(Body, Length),
        atom_number(Length1, Length),
        Headers1 = Headers.put('content-length', Length1),
        (   \+ _ = Headers1.get('content-type')
        ->  Headers2 = Headers1.put('content-type', 'text/plain')
        ;   Headers2 = Headers1
        )
    ;   Headers2 = Headers
    ),
    create_header_lines(Headers2, HeaderLines),
    (   HeaderLines \= ''
    ->  atomic_list_concat([Command, HeaderLines], '\n', WithoutBody)
    ;   WithoutBody = Command
    ),
    atomic_list_concat([WithoutBody, Body], '\n\n', Frame).

create_header_lines(Headers, HeaderLines) :-
    dict_pairs(Headers, _, Pairs),
    maplist(create_header_line, Pairs, HeaderLines0),
    atomic_list_concat(HeaderLines0, '\n', HeaderLines).

create_header_line(K-V, HeaderLine) :-
    atomic_list_concat([K, V], ':', HeaderLine).

receive(Connection, Stream) :-
    receive0(Connection, Stream, '').

receive0(Connection, Stream, Buffered) :-
    (   catch(receive_frames(Stream, Frames, Buffered, Buffered1), E, true)
    ->  (   nonvar(E)
        ->  E = exception(disconnected),
            debug(stompl(connection), 'disconnected', []),
            notify(Connection, disconnected)
        ;   debug(stompl(message), 'frames received~n~w', [Frames]),
            handle_frames(Connection, Frames),
            debug(stompl(message), 'frames handled', []),
            receive0(Connection, Stream, Buffered1)
        )
    ).

receive_frames(Stream, Frames, Buffered0, Buffered) :-
    (   at_end_of_stream(Stream)
    ->  throw(exception(disconnected))
    ;   read_pending_input(Stream, Codes, [])
    ),
    atom_codes(Chars, Codes),
    debug(stompl(frame), 'received~n~w', [Chars]),
    (   Chars = '\x0a'
    ->  Buffered = Buffered0,
        Frames = [Chars]
    ;   atom_concat(Buffered0, Chars, Buffered1),
        debug(stompl(frame), 'current buffer~n~w', [Buffered1]),
        extract_frames(Frames, Buffered1, Buffered)
    ).

extract_frames(Frames, Buffered0, Buffered) :-
    extract_frames0([], Frames0, Buffered0, Buffered),
    reverse(Frames0, Frames).

extract_frames0(Frames, Frames, '', '') :- !.
extract_frames0(Frames, Frames, Buffered, Buffered) :-
    \+ sub_atom(Buffered, _, 1, _, '\x00'), !.
extract_frames0(Frames0, Frames, Buffered0, Buffered) :-
    sub_atom(Buffered0, FrameLength, 1, _, '\x00'), !,
    sub_atom(Buffered0, 0, FrameLength, _, Frame),
    (  sub_atom(Frame, PreambleLength, 2, _, '\n\n'), !
    -> (   check_frame(Frame, Buffered0, FrameLength, PreambleLength, Frame1, Buffered1)
       ->  Frames1 = [Frame1|Frames0],
           extract_frames0(Frames1, Frames, Buffered1, Buffered)
       ;   Frames = Frames0,
           Buffered = Buffered0
       )
    ;  Frames1 = [Frame|Frames0],
       Length is FrameLength + 1,
       sub_atom(Buffered0, Length, _, 0, Buffered1),
       extract_frames0(Frames1, Frames, Buffered1, Buffered)
    ).

check_frame(Frame0, Buffered0, FrameLength, PreambleLength, Frame, Buffered) :-
    (   read_content_length(Frame0, ContentLength)
    ->  ContentOffset is PreambleLength + 2,
        FrameSize is ContentOffset + ContentLength,
        (   FrameSize > FrameLength
        ->  atom_length(Buffered0, Length),
            (   FrameSize < Length
            ->  sub_atom(Buffered0, 0, FrameSize, _, Frame),
                Length is FrameSize + 1,
                sub_atom(Buffered0, Length, _, 0, Buffered)
            )
        ;   Frame = Frame0,
            Length is FrameLength + 1,
            sub_atom(Buffered0, Length, _, 0, Buffered)
        )
    ;   Frame = Frame0,
        Length is FrameLength + 1,
        sub_atom(Buffered0, Length, _, 0, Buffered)
    ).

read_content_length(Frame, Length) :-
    atomic_list_concat([_, Frame1], 'content-length:', Frame),
    atomic_list_concat([Length0|_], '\n', Frame1),
    atomic_list_concat(L, ' ', Length0),
    last(L, Length1),
    atom_number(Length1, Length).

handle_frames(_, []) :- !.
handle_frames(Connection, [H|T]) :-
    parse_frame(H, ParsedFrame),
    debug(stompl(frame), 'parsed frame~n~w', [ParsedFrame]),
    process_frame(Connection, ParsedFrame),
    handle_frames(Connection, T).

parse_frame('\x0a', _{cmd:heartbeat}) :- !.
parse_frame(Frame, ParsedFrame) :-
    sub_atom(Frame, PreambleLength, 2, _, '\n\n'), !,
    sub_atom(Frame, 0, PreambleLength, _, Preamble),
    Begin is PreambleLength + 2,
    sub_atom(Frame, Begin, _, 0, Body),
    parse_headers(Preamble, Command, Headers),
    ParsedFrame = _{cmd:Command, headers:Headers, body:Body}.

parse_headers(Preamble, Command, Headers) :-
    atomic_list_concat([Command|PreambleLines], '\n', Preamble),
    parse_headers0(PreambleLines, _{}, Headers).

parse_headers0([], Headers, Headers) :- !.
parse_headers0([H|T], Headers0, Headers) :-
    atomic_list_concat([Key0, Value0], ':', H),
    replace(Key0, Key),
    (   \+ Headers0.get(Key) %% repeated hearder entries, only the first one should be used
    ->  sub_atom(Value0, _, _, 0, Value1),
        \+ sub_atom(Value1, 0, 1, _, ' '), !,
        replace(Value1, Value),
        Headers1 = Headers0.put(Key, Value)
    ;   Headers1 = Headers0
    ),
    parse_headers0(T, Headers1, Headers).

replace(A0, A) :-
    atomic_list_concat(L0, '\\n', A0),
    atomic_list_concat(L0, '\n', A1),
    atomic_list_concat(L1, '\\r', A1),
    atomic_list_concat(L1, '\r', A2),
    atomic_list_concat(L2, '\\\\', A2),
    atomic_list_concat(L2, '\\', A3),
    atomic_list_concat(L3, '\\c', A3),
    atomic_list_concat(L3, ':', A).

process_frame(Connection, Frame) :-
    Frame.cmd = heartbeat, !,
    get_time(Now),
    debug(stompl(heartbeat), 'received heartbeat at ~w', [Now]),
    update_connection_mapping(Connection, _{received_heartbeat:Now}).
process_frame(Connection, Frame) :-
    downcase_atom(Frame.cmd, FrameType),
    (   FrameType = connected
    ->  start_heartbeat_if_required(Connection, Frame.headers)
    ;   true
    ),
    notify(Connection, FrameType, Frame).

start_heartbeat_if_required(Connection, Headers) :-
    (   get_mapping_data(Connection, 'heart-beat', CHB),
        SHB = Headers.get('heart-beat')
    ->  start_heartbeat(Connection, CHB, SHB)
    ;   true
    ).

start_heartbeat(Connection, CHB, SHB) :-
    extract_heartbeats(CHB, CX, CY),
    extract_heartbeats(SHB, SX, SY),
    calculate_heartbeats(CX-CY, SX-SY, X-Y),
    X-Y \= 0-0, !,
    debug(stompl(heartbeat), 'calculated heartbeats are ~w,~w', [X, Y]),
    SendSleep is X / 1000,
    ReceiveSleep is Y / 1000 + 2,
    (   SendSleep = 0
    ->  SleepTime = ReceiveSleep
    ;   (   ReceiveSleep = 0
        ->  SleepTime = SendSleep
        ;   SleepTime is gcd(SendSleep, ReceiveSleep) / 2
        )
    ),
    get_time(Now),
    thread_create(heartbeat_loop(Connection, SendSleep, ReceiveSleep, SleepTime, Now),
                  HeartbeatThreadId, []),
    update_connection_mapping(Connection,
                              _{
                                heartbeat_thread_id:HeartbeatThreadId,
                                received_heartbeat:Now
                               }).
start_heartbeat(_, _, _).

extract_heartbeats(Heartbeat, X, Y) :-
    atomic_list_concat(L, ' ', Heartbeat),
    atomic_list_concat(L, '', Heartbeat1),
    atomic_list_concat([X0, Y0], ',', Heartbeat1),
    atom_number(X0, X),
    atom_number(Y0, Y).

calculate_heartbeats(CX-CY, SX-SY, X-Y) :-
    (   CX \= 0, SY \= 0
    ->  X is max(CX, floor(SY))
    ;   X = 0
    ),
    (   CY \= 0, SX \= 0
    ->  Y is max(CY, floor(SX))
    ;   Y = 0
    ).

heartbeat_loop(Connection, SendSleep, ReceiveSleep, SleepTime, SendTime) :-
    sleep(SleepTime),
    get_time(Now),
    (   Now - SendTime > SendSleep
    ->  SendTime1 = Now,
        debug(stompl(heartbeat), 'sending a heartbeat message at ~w', [Now]),
        send0(Connection, '\x0a', false)
    ;   SendTime1 = SendTime
    ),
    get_mapping_data(Connection, received_heartbeat, ReceivedHeartbeat),
    DiffReceive is Now - ReceivedHeartbeat,
    (   DiffReceive > ReceiveSleep
    ->  debug(stompl(heartbeat),
              'heartbeat timeout: diff_receive=~w, time=~w, lastrec=~w',
              [DiffReceive, Now, ReceivedHeartbeat]),
        notify(Connection, heartbeat_timeout)
    ;   true
    ),
    heartbeat_loop(Connection, SendSleep, ReceiveSleep, SleepTime, SendTime1).

notify(Connection, FrameType) :-
    get_mapping_data(Connection, callbacks, CallbackDict),
    atom_concat(on_, FrameType, Key),
    (   Predicate = CallbackDict.get(Key)
    ->  debug(stompl(callback), 'callback predicate ~w', [Predicate]),
        ignore(call(Predicate, Connection))
    ;   true
    ).

notify(Connection, FrameType, Frame) :-
    get_mapping_data(Connection, callbacks, CallbackDict),
    atom_concat(on_, FrameType, Key),
    (   Predicate = CallbackDict.get(Key)
    ->  debug(stompl(callback), 'callback predicate ~w', [Predicate]),
        ignore(call(Predicate, Connection, Frame.headers, Frame.body))
    ;   true
    ).

get_mapping_data(Connection, Key, Value) :-
    connection_mapping(Connection, Dict),
    Value = Dict.get(Key).

update_connection_mapping(Connection, Dict) :-
    connection_mapping(Connection, OldDict),
    retract(connection_mapping(Connection, OldDict)),
    asserta(connection_mapping(Connection, OldDict.put(Dict))).
