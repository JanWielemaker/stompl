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
:- use_module(library(debug)).
:- use_module(library(readutil)).
:- use_module(library(http/http_stream)).

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


		 /*******************************
		 *        INCOMING DATA		*
		 *******************************/

%!  read_frame(+Stream, -Frame) is det.
%
%   Read a frame from a STOMP stream.   On end-of-file, Frame is unified
%   with the atom `end_of_file`. Otherwise  it   is  a  dict holding the
%   `cmd`, `headers` (another dict) and `body` (a string)

read_frame(Stream, Frame) :-
    read_command(Stream, Command),
    (   Command == end_of_file
    ->  Frame = end_of_file
    ;   Command == heartbeat
    ->  Frame = stomp_frame{cmd:heartbeat}
    ;   read_header(Stream, Header),
        (   has_body(Command)
        ->  read_content(Stream, Header, Content),
            Frame = stomp_frame{cmd:Command, headers:Header, body:Content}
        ;   Frame = stomp_frame{cmd:Command, headers:Header}
        )
    ).

has_body(send).
has_body(message).
has_body(error).

read_command(Stream, Command) :-
    read_line_to_string(Stream, String),
    debug(stompl(command), 'Got line ~p', [String]),
    (   String == end_of_file
    ->  Command = end_of_file
    ;   String == ""
    ->  Command = heartbeat
    ;   string_lower(String, Lwr),
        atom_string(Command, Lwr)
    ).

read_header(Stream, Header) :-
    read_header_lines(Stream, Pairs, []),
    dict_pairs(Header, stomp_header, Pairs).

read_header_lines(Stream, Header, Seen) :-
    read_line_to_string(Stream, Line),
    (   Line == ""
    ->  Header = []
    ;   sub_string(Line, Before, _, After, ":")
    ->  sub_atom(Line, 0, Before, _, Name),
        (   memberchk(Name, Seen)
        ->  read_header_lines(Stream, Header, Seen)
        ;   sub_string(Line, _, After, 0, Value0),
            convert_value(Name, Value0, Value),
            Header = [Name-Value|More],
            read_header_lines(Stream, More, [Name|Seen])
        )
    ).

convert_value('content-length', String, Bytes) :-
    !,
    number_string(Bytes, String).
convert_value(_, String, Value) :-
    unescape_header_value(String, Value).

unescape_header_value(String, Value) :-
    sub_atom(String, _, _, _, "\\"),
    !,
    string_codes(String, Codes),
    phrase(unescape(Plain), Codes),
    string_codes(Value, Plain).
unescape_header_value(String, String).

unescape([H|T]) --> "\\", !, unesc(H), unescape(T).
unescape([H|T]) --> [H], !, unescape(T).
unescape([]) --> [].

unesc(0'\n) --> "n", !.
unesc(0':)  --> "c", !.
unesc(0'\\) --> "\\", !.
unesc(_) --> [C], { syntax_error(invalid_stomp_escape(C)) }.

%!  read_content(+Stream, +Header, -Content) is det.
%
%   Read the body. Note that the body   may  be followed by an arbitrary
%   number of newlines. We leave them in place to avoid blocking.

read_content(Stream, Header, Content) :-
    Bytes = Header.'content-length',
    !,
    setup_call_cleanup(
        stream_range_open(Stream, DataStream, [size(Bytes)]),
        read_string(DataStream, _, Content),
        close(DataStream)).

%!  receive(+Connection, +Stream) is det.
%
%   Read and process incoming messages from Stream.

receive(Connection, Stream) :-
    E = error(Formal, _),
    catch(read_frame(Stream, Frame), E, true),
    (   var(Formal)
    ->  debug(stompl(heartbeat), 'received frame ~p', [Frame]),
        !,
        (   Frame == end_of_file
        ->  notify(Connection, disconnected)
        ;   process_frame(Connection, Frame),
            receive(Connection, Stream)
        )
    ;   print_message(warning, E),
        notify(Connection, disconnected),
        debug(stompl(connection), 'Receiver thread died', []),
        thread_self(Me),
        thread_detach(Me)
    ).
receive(Connection, _Stream) :-
    notify(Connection, disconnected),
    debug(stompl(connection), 'Receiver thread finished', []),
    thread_self(Me),
    thread_detach(Me).

process_frame(Connection, Frame) :-
    Frame.cmd = heartbeat, !,
    get_time(Now),
    debug(stompl(heartbeat), 'received heartbeat at ~w', [Now]),
    update_connection_mapping(Connection, _{received_heartbeat:Now}).
process_frame(Connection, Frame) :-
    FrameType = Frame.cmd,
    (   FrameType == connected
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
    thread_create(heartbeat_loop(Connection, SendSleep, ReceiveSleep,
                                 SleepTime, Now),
                  HeartbeatThreadId, []),
    update_connection_mapping(Connection,
                              _{
                                heartbeat_thread_id:HeartbeatThreadId,
                                received_heartbeat:Now
                               }).
start_heartbeat(_, _, _).

extract_heartbeats(Heartbeat, X, Y) :-
    split_string(Heartbeat, ",", " ", [XS, YS]),
    number_string(X, XS),
    number_string(Y, YS).

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

%!  notify(+Connection, +FrameType) is det.
%!  notify(+Connection, +FrameType, +Frame) is det.

notify(Connection, FrameType) :-
    get_mapping_data(Connection, callbacks, CallbackDict),
    atom_concat(on_, FrameType, Key),
    (   Predicate = CallbackDict.get(Key)
    ->  debug(stompl(callback), 'callback predicate ~w', [Predicate]),
        callback(Predicate, Connection)
    ;   true
    ).

notify(Connection, FrameType, Frame) :-
    get_mapping_data(Connection, callbacks, CallbackDict),
    atom_concat(on_, FrameType, Key),
    (   Predicate = CallbackDict.get(Key)
    ->  debug(stompl(callback), 'callback predicate ~w', [Predicate]),
        (   has_body(FrameType)
        ->  callback(Predicate, Connection, Frame.headers, Frame.body)
        ;   callback(Predicate, Connection, Frame.headers)
        )
    ;   true
    ).

callback(Pred, Connection) :-
    Error = error(_,_),
    (   catch_with_backtrace(
            call(Pred, Connection),
            Error,
            print_message(warning, Error))
    ->  true
    ;   true
    ).
callback(Pred, Connection, Headers) :-
    Error = error(_,_),
    (   catch_with_backtrace(
            call(Pred, Connection, Headers),
            Error,
            print_message(warning, Error))
    ->  true
    ;   true
    ).
callback(Pred, Connection, Headers, Body) :-
    Error = error(_,_),
    (   catch_with_backtrace(
            call(Pred, Connection, Headers, Body),
            Error,
            print_message(warning, Error))
    ->  true
    ;   true
    ).

get_mapping_data(Connection, Key, Value) :-
    connection_mapping(Connection, Dict),
    Value = Dict.get(Key).

update_connection_mapping(Connection, Dict) :-
    connection_mapping(Connection, OldDict),
    retract(connection_mapping(Connection, OldDict)),
    asserta(connection_mapping(Connection, OldDict.put(Dict))).
