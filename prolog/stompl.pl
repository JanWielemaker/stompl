:- module(stompl,
          [ connection/2,    % +Address, -Connection
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

:- meta_predicate
    connection(+,:,-).

:- use_module(library(uuid)).
:- use_module(library(socket)).
:- use_module(library(apply)).
:- use_module(library(http/json)).
:- use_module(library(debug)).
:- use_module(library(readutil)).
:- use_module(library(http/http_stream)).

:- dynamic
    connection_mapping/2.

%!  connected(+Address, -Connected) is det.
%!  connected(+Address, +CallbackDict, -Connected) is det.
%
%   Create a connection reference. The connection is   not set up yet by
%   this predicate. If CallbackDict is provided,   it will be associated
%   with the connection reference. Valid  keys   of  the dict are below,
%   together with the additional arguments passed.   `Header`  is a dict
%   holding the STOMP frame header, where  all values are strings except
%   for the `'content-length'` key value which is passed as an integer.
%
%   Body  is  a   string   or,   if   the    data   is   of   the   type
%   ``application/json``, a dict.
%
%     - on_connected:Closure
%       Called as call(Closure, Connection, Header)
%     - on_disconnected:Closure
%       Called as call(Closure, Connection, Header)
%     - on_message:Closure
%       Called as call(Closure, Connection, Header, Body)
%     - on_heartbeat_timeout:Closure
%       Called as call(Closure, Connection, Header)
%     - on_error:Closure
%       Called as call(Closure, Connection, Header, Body)

connection(Address, Connection) :-
    connection(Address, Connection, _{}).

connection(Address, Module:CallbackDict, Connection) :-
    uuid(Connection),
    qualify_callbacks(Module, CallbackDict, CallbackDictQ),
    asserta(connection_mapping(Connection,
                               _{ address: Address,
                                  callbacks: CallbackDictQ
                                })).

qualify_callbacks(Module, Dict, DictQ) :-
    dict_pairs(Dict, Tag, Pairs),
    maplist(qualify_callback(Module), Pairs, PairsQ),
    dict_pairs(DictQ, Tag, PairsQ).

qualify_callback(Module, Name-Value, Name-(M:Plain)) :-
    strip_module(Module:Value, M, Plain).

%!  setup(+Connection) is det.
%
%   Set up the actual socket connection and start receiving thread.

setup(Connection) :-
    get_mapping_data(Connection, address, Address),
    tcp_connect(Address, Stream, []),
    gensym(stompl_receive, Alias),
    thread_create(receive(Connection, Stream), ReceiverThreadId, [alias(Alias)]),
    debug(stompl(connection), 'Handling input on thread ~p', [ReceiverThreadId]),
    update_connection_mapping(Connection,
                              _{ receiver_thread_id: ReceiverThreadId,
                                 stream:Stream
                               }).

%!  teardown(+Connection) is semidet.
%
%   Tear down the socket connection, stop receiving thread and heartbeat
%   thread (if applicable).

teardown(Connection) :-
    terminate_helper(Connection, receiver_thread_id),
    terminate_helper(Connection, heartbeat_thread_id),
    get_mapping_data(Connection, stream, Stream),
    close(Stream, [force(true)]),
    debug(stompl(connection), 'retract connection mapping', []),
    retract(connection_mapping(Connection, _)).

terminate_helper(Connection, Helper) :-
    get_mapping_data(Connection, Helper, Thread),
    \+ thread_self(Thread),
    catch(thread_signal(Thread, throw(kill)), error(_,_), fail),
    !,
    thread_join(Thread, _Status).
terminate_helper(_, _).


%!  connect(+Connectio, +Host, +Headers) is det.
%
%   Send a ``CONNECT`` frame. Protocol version and heartbeat negotiation
%   will  be  handled.  ``STOMP``  frame  is    not  used  for  backward
%   compatibility.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#CONNECT_or_STOMP_Frame).
%   @tbd 1.2 doesn't bring much benefit but trouble

connect(Connection, Host, Headers) :-
    send_frame(Connection,
               connect,
               Headers.put(_{ 'accept-version':'1.0,1.1',
                               host:Host
                            })),
    (   Heartbeat = Headers.get('heart-beat')
    ->  update_connection_mapping(Connection, _{'heart-beat':Heartbeat})
    ;   true
    ).


%!  send(+Connection, +Destination, +Headers) is det.
%!  send(+Connection, +Destination, +Headers, +Body) is det.
%
%   Send  a  ``SEND``  frame.  If   ``content-type``  is  not  provided,
%   ``text/plain`` will be used. ``content-length``   will  be filled in
%   automatically.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#SEND

send(Connection, Destination, Headers) :-
    send(Connection, Destination, Headers).

send(Connection, Destination, Headers, Body) :-
    send_frame(Connection, send, Headers.put(destination, Destination), Body).

%!  send_json(+Connection, +Destination, +Headers, +JSON) is det.
%
%   Send a ``SEND`` frame. ``JSON`` can be either a JSON term or a dict.
%   ``content-type`` is filled in  automatically as ``application/json``
%   and ``content-length`` will be filled in automatically as well.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#SEND

send_json(Connection, Destination, Headers, JSON) :-
    atom_json_term(Body, JSON, [as(string)]),
    send_frame(Connection, send,
               Headers.put(_{ destination:Destination,
                              'content-type':'application/json'
                            }),
               Body).

%!  subscribe(+Connection, +Destination, +Id, +Headers) is det.
%
%   Send a ``SUBSCRIBE`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#SUBSCRIBE

subscribe(Connection, Destination, Id, Headers) :-
    send_frame(Connection, subscribe,
               Headers.put(_{destination:Destination, id:Id})).

%!  unsubscribe(+Connection, +Id) is det.
%
%   Send an ``UNSUBSCRIBE`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#UNSUBSCRIBE

unsubscribe(Connection, Id) :-
    send_frame(Connection, unsubscribe, _{id:Id}).

%!  ack(+Connection, +MessageId, +Headers) is det.
%
%   Send an ``ACK`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#ACK

ack(Connection, MessageId, Headers) :-
    send_frame(Connection, ack, Headers.put('message-id', MessageId)).

%!  nack(+Connection, +MessageId, +Headers) is det.
%
%   Send a ``NACK`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#NACK

nack(Connection, MessageId, Headers) :-
    send_frame(Connection, nack, Headers.put('message-id', MessageId)).

%!  begin(+Connection, +Transaction) is det.
%
%   Send a ``BEGIN`` frame.
%   @see http://stomp.github.io/stomp-specification-1.1.html#BEGIN

begin(Connection, Transaction) :-
    send_frame(Connection, begin, _{transaction:Transaction}).

%!  commit(+Connection, +Transaction) is det.
%
%   Send a ``COMMIT`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#COMMIT

commit(Connection, Transaction) :-
    send_frame(Connection, commit, _{transaction:Transaction}).

%!  abort(+Connection, +Transaction) is det.
%
%   Send a ``ABORT`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#ABORT

abort(Connection, Transaction) :-
    send_frame(Connection, abort, _{transaction:Transaction}).

%!  disconnect(+Connection, +Headers) is det.
%
%   Send a ``DISCONNECT`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#DISCONNECT

disconnect(Connection, Headers) :-
    send_frame(Connection, disconnect, Headers).

%!  send_frame(+Connection, +Command, +Headers) is det.
%!  send_frame(+Connection, +Command, +Headers, +Body) is det.

send_frame(Connection, heartbeat, _Headers) :-
    !,
    get_mapping_data(Connection, stream, Stream),
    put_code(Stream, 0'\n),
    flush_output(Stream).
send_frame(Connection, Command, Headers) :-
    assertion(\+has_body(Command)),
    send_frame(Connection, Command, Headers, '').

send_frame(Connection, Command, Headers, Body) :-
    has_body(Command),
    !,
    get_mapping_data(Connection, stream, Stream),
    default_content_type('text/plain', Headers, Headers1),
    body_bytes(Body, ContentLength),
    Headers2 = Headers1.put('content-length', ContentLength),
    send_command(Stream, Command),
    send_header(Stream, Headers2),
    format(Stream, '~w\u0000\n', [Body]),
    flush_output(Stream).
send_frame(Connection, Command, Headers, _Body) :-
    get_mapping_data(Connection, stream, Stream),
    send_command(Stream, Command),
    send_header(Stream, Headers),
    format(Stream, '\u0000\n', []),
    flush_output(Stream).

send_command(Stream, Command) :-
    string_upper(Command, Upper),
    format(Stream, '~w\n', [Upper]).

send_header(Stream, Headers) :-
    dict_pairs(Headers, _, Pairs),
    maplist(send_header_line(Stream), Pairs),
    nl(Stream).

send_header_line(Stream, Name-Value) :-
    (   integer(Value)
    ->  format(Stream, '~w:~w\n', [Name,Value])
    ;   escape_value(Value, String),
        format(Stream, '~w:~w\n', [Name,String])
    ).

escape_value(Value, String) :-
    split_string(Value, "\n:\\", "", [_]),
    !,
    String = Value.
escape_value(Value, String) :-
    string_codes(Value, Codes),
    phrase(escape(Codes), Encoded),
    string_codes(String, Encoded).

escape([]) --> [].
escape([H|T]) --> esc(H), escape(T).

esc(0'\n) --> !, "\\n".
esc(0':)  --> !, "\\:".
esc(0'\\) --> !, "\\\\".
esc(C)    --> [C].

default_content_type(ContentType, Header0, Header) :-
    (   get_dict('content-type', Header0, _)
    ->  Header = Header0
    ;   put_dict('content-type', Header0, ContentType, Header)
    ).

body_bytes(String, Bytes) :-
    setup_call_cleanup(
        open_null_stream(Out),
        ( write(Out, String),
          byte_count(Out, Bytes)
        ),
        close(Out)).


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
    _{ 'content-length':Bytes,
       'content-type':Type
     } :< Header,
    setup_call_cleanup(
        stream_range_open(Stream, DataStream, [size(Bytes)]),
        read_content(Type, DataStream, Header, Content),
        close(DataStream)).

read_content("application/json", Stream, _Header, Content) :-
    !,
    json_read_dict(Stream, Content).
read_content(_Type, Stream, _Header, Content) :-
    read_string(Stream, _, Content).


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
    gensym(stompl_heartbeat, Alias),
    thread_create(heartbeat_loop(Connection, SendSleep, ReceiveSleep,
                                 SleepTime, Now),
                  HeartbeatThreadId, [alias(Alias)]),
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
        debug(stompl(heartbeat), 'sending a heartbeat message at ~p', [Now]),
        send_frame(Connection, heartbeat, _{})
    ;   SendTime1 = SendTime
    ),
    get_mapping_data(Connection, received_heartbeat, ReceivedHeartbeat),
    DiffReceive is Now - ReceivedHeartbeat,
    (   DiffReceive > ReceiveSleep
    ->  debug(stompl(heartbeat),
              'heartbeat timeout: diff_receive=~p, time=~p, lastrec=~p',
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
