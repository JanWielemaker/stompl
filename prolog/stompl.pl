:- module(stompl,
          [ stomp_connection/5,    % +Address, +Host, +Headers,
                                   % :Callback, -Connection
            stomp_setup/1,         % +Connection
            stomp_teardown/1,      % +Connection
            stomp_connect/1,       % +Connection
            stomp_send/4,          % +Connection, +Destination, +Headers, +Body
            stomp_send_json/4,     % +Connection, +Destination, +Headers, +JSON
            stomp_subscribe/4,     % +Connection, +Destination, +Id, +Headers
            stomp_unsubscribe/2,   % +Connection, +Id
            stomp_ack/3,           % +Connection, +MessageId, +Headers
            stomp_nack/3,          % +Connection, +MessageId, +Headers
            stomp_begin/2,         % +Connection, +Transaction
            stomp_commit/2,        % +Connection, +Transaction
            stomp_abort/2,         % +Connection, +Transaction
            stomp_transaction/2,   % +Connection, :Goal
            stomp_disconnect/2     % +Connection, +Headers
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
    stomp_connection(+, +, +, 4, -),
    stomp_transaction(+, 0).

:- use_module(library(apply)).
:- use_module(library(debug)).
:- use_module(library(error)).
:- use_module(library(gensym)).
:- use_module(library(http/http_stream)).
:- use_module(library(http/json)).
:- use_module(library(readutil)).
:- use_module(library(socket)).
:- use_module(library(uuid)).

:- dynamic
    connection_property/3.

%!  stomp_connection(+Address, +Host, +Headers, :Callback, -Connection)
%!	is det.
%
%   Create a connection reference. The connection is   not set up yet by
%   this predicate. Callback is called on  any received frame except for
%   _heart beat_ frames as below.
%
%   ```
%   call(Callback, Command, Connection, Header, Body)
%   ```
%
%   Where command is one of the commands below.
%   with the connection reference. Valid  keys   of  the dict are below,
%   together with the additional arguments passed.   `Header`  is a dict
%   holding the STOMP frame header, where  all values are strings except
%   for the `'content-length'` key value which is passed as an integer.
%
%   Body  is  a   string   or,   if   the    data   is   of   the   type
%   ``application/json``, a dict.
%
%     - connected
%       A connection was established.  Connection and Header are valid.
%     - disconnected
%       The connection was lost.  Only Connection is valid.
%     - message
%       A message arrived.  All three arguments are valid.  Body is
%       a dict if the ``content-type`` of the message is
%       ``application/json`` and a string otherwise.
%     - heartbeat_timeout
%       No heartbeat was received.  Only Connection is valid.
%     - error
%       An error happened.  All three arguments are valid and handled
%       as `message`.

stomp_connection(Address, Host, Headers, Callback, Connection) :-
    valid_address(Address),
    must_be(atom, Host),
    must_be(dict, Headers),
    must_be(callable, Callback),
    uuid(Connection),
    retractall(connection_property(Connection, _, _)),
    update_connection_mapping(
        Connection,
        _{ address: Address,
           callback: Callback,
           host: Host,
           headers: Headers
         }).

valid_address(Host:Port) :-
    !,
    must_be(atom, Host),
    must_be(integer, Port).
valid_address(Address) :-
    type_error(stom_address, Address).

connection_property(address).
connection_property(callback).
connection_property(host).
connection_property(headers).


%!  stomp_setup(+Connection) is det.
%
%   Set up the actual socket connection and start receiving thread. This
%   is a no-op if the connection has already been created.

stomp_setup(Connection) :-
    connection_property(Connection, stream, _Stream),
    !.
stomp_setup(Connection) :-
    connection_property(Connection, address, Address),
    tcp_connect(Address, Stream, []),
    gensym(stompl_receive, Alias),
    thread_create(receive(Connection, Stream), ReceiverThreadId, [alias(Alias)]),
    debug(stompl(connection), 'Handling input on thread ~p', [ReceiverThreadId]),
    update_connection_mapping(Connection,
                              _{ receiver_thread_id: ReceiverThreadId,
                                 stream:Stream
                               }).

%!  stomp_teardown(+Connection) is semidet.
%
%   Tear down the socket connection, stop receiving thread and heartbeat
%   thread (if applicable).  The  registration   of  the  connection  as
%   created by stomp_connection/5 is preserved and the connection may be
%   reconnected using stomp_connect/1.

stomp_teardown(Connection) :-
    terminate_helper(Connection, receiver_thread_id),
    terminate_helper(Connection, heartbeat_thread_id),
    forall(connection_property(Connection, stream, Stream),
           close(Stream, [force(true)])),
    debug(stompl(connection), 'retract connection mapping', []),
    reset_connection_properties(Connection).

terminate_helper(Connection, Helper) :-
    retract(connection_property(Connection, Helper, Thread)),
    \+ thread_self(Thread),
    catch(thread_signal(Thread, throw(kill)), error(_,_), fail),
    !,
    thread_join(Thread, _Status).
terminate_helper(_, _).

reset_connection_properties(Connection) :-
    findall(P,
            (   connection_property(Connection, P, _),
                \+ connection_property(P)
            ), Ps),
    forall(member(P, Ps),
           retractall(connection_property(Connection, P, _))).

%!  stomp_connect(+Connectio) is det.
%
%   Send a ``CONNECT`` frame. Protocol version and heartbeat negotiation
%   will  be  handled.  ``STOMP``  frame  is    not  used  for  backward
%   compatibility.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#CONNECT_or_STOMP_Frame).
%   @tbd 1.2 doesn't bring much benefit but trouble

stomp_connect(Connection) :-
    stomp_setup(Connection),
    connection_property(Connection, headers, Headers),
    connection_property(Connection, host, Host),
    send_frame(Connection,
               connect,
               Headers.put(_{ 'accept-version':'1.0,1.1',
                               host:Host
                            })),
    (   Heartbeat = Headers.get('heart-beat')
    ->  update_connection_property(Connection, 'heart-beat', Heartbeat)
    ;   true
    ).


%!  stomp_send(+Connection, +Destination, +Headers, +Body) is det.
%
%   Send  a  ``SEND``  frame.  If   ``content-type``  is  not  provided,
%   ``text/plain`` will be used. ``content-length``   will  be filled in
%   automatically.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#SEND

stomp_send(Connection, Destination, Headers, Body) :-
    add_transaction(Headers, Headers1),
    send_frame(Connection, send, Headers1.put(destination, Destination), Body).

%!  stomp_send_json(+Connection, +Destination, +Headers, +JSON) is det.
%
%   Send a ``SEND`` frame. ``JSON`` can be either a JSON term or a dict.
%   ``content-type`` is filled in  automatically as ``application/json``
%   and ``content-length`` will be filled in automatically as well.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#SEND

stomp_send_json(Connection, Destination, Headers, JSON) :-
    add_transaction(Headers, Headers1),
    atom_json_term(Body, JSON, [as(string)]),
    send_frame(Connection, send,
               Headers1.put(_{ destination:Destination,
                               'content-type':'application/json'
                             }),
               Body).

%!  stomp_subscribe(+Connection, +Destination, +Id, +Headers) is det.
%
%   Send a ``SUBSCRIBE`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#SUBSCRIBE

stomp_subscribe(Connection, Destination, Id, Headers) :-
    send_frame(Connection, subscribe,
               Headers.put(_{destination:Destination, id:Id})).

%!  stomp_unsubscribe(+Connection, +Id) is det.
%
%   Send an ``UNSUBSCRIBE`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#UNSUBSCRIBE

stomp_unsubscribe(Connection, Id) :-
    send_frame(Connection, unsubscribe, _{id:Id}).

%!  stomp_ack(+Connection, +MessageId, +Headers) is det.
%
%   Send an ``ACK`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#ACK

stomp_ack(Connection, MessageId, Headers) :-
    send_frame(Connection, ack, Headers.put('message-id', MessageId)).

%!  stomp_nack(+Connection, +MessageId, +Headers) is det.
%
%   Send a ``NACK`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#NACK

stomp_nack(Connection, MessageId, Headers) :-
    send_frame(Connection, nack, Headers.put('message-id', MessageId)).

%!  stomp_begin(+Connection, +Transaction) is det.
%
%   Send a ``BEGIN`` frame.
%   @see http://stomp.github.io/stomp-specification-1.1.html#BEGIN

stomp_begin(Connection, Transaction) :-
    send_frame(Connection, begin, _{transaction:Transaction}).

%!  stomp_commit(+Connection, +Transaction) is det.
%
%   Send a ``COMMIT`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#COMMIT

stomp_commit(Connection, Transaction) :-
    send_frame(Connection, commit, _{transaction:Transaction}).

%!  stomp_abort(+Connection, +Transaction) is det.
%
%   Send a ``ABORT`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#ABORT

stomp_abort(Connection, Transaction) :-
    send_frame(Connection, abort, _{transaction:Transaction}).

%!  stomp_transaction(+Connection, :Goal) is semidet.
%
%   Run Goal as  once/1,  tagging  all   ``SEND``  messages  inside  the
%   transaction with the transaction id.  If   Goal  fails  or raises an
%   exception the transaction is aborted.   Failure  or exceptions cause
%   the transaction to be aborted using   stomp_abort/2, after which the
%   result is forwarded.

stomp_transaction(Connection, Goal) :-
    uuid(TransactionID),
    stomp_transaction(Connection, Goal, TransactionID).

stomp_transaction(Connection, Goal, TransactionID) :-
    stomp_begin(Connection, TransactionID),
    (   catch(once(Goal), Error, true)
    ->  (   var(Error)
        ->  stomp_commit(Connection, TransactionID)
        ;   stomp_abort(Connection, TransactionID),
            throw(Error)
        )
    ;   stomp_abort(Connection, TransactionID),
        fail
    ).

in_transaction(TransactionID) :-
    prolog_current_frame(Frame),
    prolog_frame_attribute(
        Frame, parent_goal,
        stomp_transaction(_Connection, _Goal, TransactionID)).

add_transaction(Headers, Headers1) :-
    in_transaction(TransactionID),
    !,
    Headers1 = Headers.put(transaction, TransactionID).
add_transaction(Headers, Headers).


%!  stomp_disconnect(+Connection, +Headers) is det.
%
%   Send a ``DISCONNECT`` frame.
%
%   @see http://stomp.github.io/stomp-specification-1.1.html#DISCONNECT

stomp_disconnect(Connection, Headers) :-
    send_frame(Connection, disconnect, Headers).

%!  send_frame(+Connection, +Command, +Headers) is det.
%!  send_frame(+Connection, +Command, +Headers, +Body) is det.

send_frame(Connection, heartbeat, _Headers) :-
    !,
    connection_property(Connection, stream, Stream),
    put_code(Stream, 0'\n),
    flush_output(Stream).
send_frame(Connection, Command, Headers) :-
    assertion(\+has_body(Command)),
    send_frame(Connection, Command, Headers, '').

send_frame(Connection, Command, Headers, Body) :-
    has_body(Command),
    !,
    connection_property(Connection, stream, Stream),
    default_content_type('text/plain', Headers, Headers1),
    body_bytes(Body, ContentLength),
    Headers2 = Headers1.put('content-length', ContentLength),
    send_command(Stream, Command),
    send_header(Stream, Headers2),
    format(Stream, '~w\u0000\n', [Body]),
    flush_output(Stream).
send_frame(Connection, Command, Headers, _Body) :-
    connection_property(Connection, stream, Stream),
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
    update_connection_property(Connection, received_heartbeat, Now).
process_frame(Connection, Frame) :-
    _{cmd:FrameType, headers:Headers, body:Body} :< Frame,
    !,
    notify(Connection, FrameType, Headers, Body).
process_frame(Connection, Frame) :-
    _{cmd:FrameType, headers:Headers} :< Frame,
    (   FrameType == connected
    ->  start_heartbeat_if_required(Connection, Headers)
    ;   true
    ),
    notify(Connection, FrameType, Headers).

start_heartbeat_if_required(Connection, Headers) :-
    (   connection_property(Connection, 'heart-beat', CHB),
        SHB = Headers.get('heart-beat')
    ->  start_heartbeat(Connection, CHB, SHB)
    ;   true
    ).

start_heartbeat(Connection, CHB, SHB) :-
    extract_heartbeats(CHB, CX, CY),
    extract_heartbeats(SHB, SX, SY),
    calculate_heartbeats(CX-CY, SX-SY, X-Y),
    X-Y \= 0-0, !,
    debug(stompl(heartbeat), 'calculated heartbeats are ~p,~p', [X, Y]),
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
                              _{ heartbeat_thread_id:HeartbeatThreadId,
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
    connection_property(Connection, received_heartbeat, ReceivedHeartbeat),
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
%!  notify(+Connection, +FrameType, +Header) is det.
%!  notify(+Connection, +FrameType, +Header, +Body) is det.

notify(Connection, disconnected) :-
    !,
    call_cleanup(notify(Connection, FrameType, stomp_header{cmd:FrameType}, ""),
                 stomp_teardown(Connection)).
notify(Connection, FrameType) :-
    notify(Connection, FrameType, stomp_header{cmd:FrameType}, "").

notify(Connection, FrameType, Header) :-
    notify(Connection, FrameType, Header, "").

notify(Connection, FrameType, Header, Body) :-
    connection_property(Connection, callback, Callback),
    Error = error(Formal,_),
    (   catch_with_backtrace(
            call(Callback, FrameType, Connection, Header, Body),
            Error, true)
    ->  (   var(Formal)
        ->  true
        ;   print_message(warning, Error)
        )
    ;   true
    ).

update_connection_mapping(Connection, Dict) :-
    dict_pairs(Dict, _, Pairs),
    maplist(update_connection_property(Connection), Pairs).

update_connection_property(Connection, Name-Value) :-
    retractall(connection_property(Connection, Name, _)),
    asserta(connection_property(Connection, Name, Value)).
update_connection_property(Connection, Name, Value) :-
    retractall(connection_property(Connection, Name, _)),
    asserta(connection_property(Connection, Name, Value)).

