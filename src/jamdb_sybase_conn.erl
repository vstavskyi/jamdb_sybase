-module(jamdb_sybase_conn).

%% API
-export([connect/1, connect/2]).
-export([reconnect/1]).
-export([disconnect/1, disconnect/2]).
-export([sql_query/2, sql_query/3]).
-export([prepare/3, unprepare/2]).
-export([execute/3, execute/4]).

-include("jamdb_sybase.hrl").

-opaque state() :: #conn{}.
-type error_type() :: socket | remote | local.
-type empty_result() :: {ok, state()} | {error, error_type(), binary(), state()}.
-type affected_rows() :: {affected_rows, non_neg_integer()}.
-type columns() :: list().  %% TODO
-type metainfo() :: list(). %%TODO
-type rows() :: list().  %% TODO
-type result_set() :: {result_set, columns(), metainfo(), rows()}.
-type return_status() :: non_neg_integer().
-type out_params() :: list().  %% TODO
-type procedure_result() :: {proc_result, return_status(), out_params()}.
-type result() :: affected_rows() | result_set() | procedure_result().
-type query_result() :: {ok, [result()], state()}.
-type options() :: [env()].

-export_type([state/0]).
-export_type([options/0]).

%% API
-spec connect([env()]) -> empty_result().
connect(Opts) ->
    Timeout  = proplists:get_value(timeout, Opts, ?DEF_TIMEOUT),
    connect(Opts, Timeout).

-spec connect([env()], timeout()) -> empty_result().
connect(Opts, Timeout) ->
    Host     = proplists:get_value(host, Opts, ?DEF_HOST),
    Port     = proplists:get_value(port, Opts, ?DEF_PORT),
    Database = proplists:get_value(database, Opts, ?DEF_DATABASE),
    PktSize  = proplists:get_value(packet_size, Opts, ?DEF_PACKET_SIZE),
    SockOpts = [binary, {active, false}, {packet, raw}, 
            {nodelay, true}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
        {ok, Socket} ->
            Conn = #conn{
                socket        = Socket, 
                timeout       = Timeout,
                packet_size   = PktSize,
                env           = Opts
            },
            {ok, [{recbuf, RecBuf}]} = inet:getopts(Socket, [recbuf]),
            inet:setopts(Socket, [{buffer, RecBuf}]),
            case login(Conn, Timeout) of
                {ok, Conn3 = #conn{state = connected}} ->
                    system_query(Conn3, ["use ", Database], Timeout);
                Error ->
                    Error
            end;
        {error, Reason} ->
            {error, socket, Reason}
    end.

-spec disconnect(state()) -> {ok, [env()]}.
disconnect(#conn{timeout=Timeout} = Conn) ->
    disconnect(Conn, Timeout).

-spec disconnect(state(), timeout()) -> {ok, [env()]}.
disconnect(#conn{socket=Socket, env=Env}, 0) ->
    ok = gen_tcp:close(Socket),
    {ok, Env};
disconnect(Conn = #conn{state=connected, socket=Socket, env=Env,
        packet_size=PktSize}, Timeout) ->
    TokenStream = ?ENCODER:encode_tokens([{logout, []}]),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    try send(Socket, DataStream) of
        ok -> handle_empty_resp(Conn, Timeout);
        {error, _Reason} -> ok
    after
        ok = gen_tcp:close(Socket)
    end,
    {ok, Env};
disconnect(#conn{env = Env}, _Timeout) ->
    {ok, Env}.

-spec reconnect(state()) -> empty_result().
reconnect(Conn) ->
    {ok, InitOpts} = disconnect(Conn, 0),
    connect(InitOpts).

-spec sql_query(state(), string()) -> query_result().
sql_query(#conn{timeout=Timeout} = Conn, Query) ->
    sql_query(Conn, Query, Timeout).

-spec sql_query(state(), string(), timeout()) -> query_result().
sql_query(Conn = #conn{state=connected, socket=Socket,
        packet_size=PktSize}, Query, Timeout) ->
    BQuery = to_binary(Query),
    TokenStream = ?ENCODER:encode_tokens([{language, BQuery}]),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    case send(Socket, DataStream) of
        ok              -> handle_query_resp(Conn, Timeout);
        {error, Reason} -> handle_error(socket, Reason, Conn)
    end;
sql_query(Conn, Query, Timeout) ->
    case reconnect(Conn) of
        {ok, Conn2} -> sql_query(Conn2, Query, Timeout);
        Error       -> Error
    end.

prepare(Conn = #conn{state=connected, socket=Socket,
        timeout=Timeout, packet_size=PktSize}, StmtId, Query) ->
    BStmtId = to_binary(StmtId),
    BQuery = to_binary(Query),
    BQuery2 = <<"create proc ", BStmtId/binary, " as ", BQuery/binary>>,
    TokenList = [{dynamic, prepare, [], BStmtId, BQuery2}],
    TokenStream = ?ENCODER:encode_tokens(TokenList),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    case send(Socket, DataStream) of
        ok              -> handle_prepare_resp(Conn, Timeout);
        {error, Reason} -> handle_error(socket, Reason, Conn)
    end;
prepare(Conn, Stmt, Query) ->
    case reconnect(Conn) of
        {ok, Conn2} -> prepare(Conn2, Stmt, Query);
        Error       -> Error
    end.

unprepare(Conn = #conn{state=connected, socket=Socket,
        timeout=Timeout, packet_size=PktSize}, StmtId) ->
    BStmtId = to_binary(StmtId),
    TokenList = [{dynamic, unprepare, [], BStmtId, <<>>}],
    TokenStream = ?ENCODER:encode_tokens(TokenList),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    case send(Socket, DataStream) of
        ok              -> handle_unprepare_resp(Conn, Timeout);
        {error, Reason} -> handle_error(socket, Reason, Conn)
    end;
unprepare(Conn, Stmt) ->
    case reconnect(Conn) of
        {ok, Conn2} -> unprepare(Conn2, Stmt);
        Error       -> Error
    end.

execute(#conn{timeout=Timeout} = Conn, Stmt, Args) ->
    execute(Conn, Stmt, Args, Timeout).

execute(Conn = #conn{state=connected, socket=Socket,
        packet_size=PktSize}, StmtId, Args, Timeout) ->
    BStmtId = to_binary(StmtId),
    TokenList = case Args of
        [] ->
            [{dynamic, execute, [], BStmtId, <<>>}];
        _ ->
            TokenParamsFmt = proplists:get_value(BStmtId, Conn#conn.prepared),
            [
                {dynamic, execute, [?TDS_DYNAMIC_HASARGS], BStmtId, <<>>},
                TokenParamsFmt,
                {params, Args}
            ]
    end,
    TokenStream = ?ENCODER:encode_tokens(TokenList),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    case send(Socket, DataStream) of
        ok              -> handle_execute_resp(Conn, Timeout);
        {error, Reason} -> handle_error(socket, Reason, Conn)
    end;
execute(Conn, Stmt, Args, Timeout) ->
    case reconnect(Conn) of
        {ok, Conn2} -> execute(Conn2, Stmt, Args, Timeout);
        Error       -> Error
    end.


%% internal
to_binary(String) when is_binary(String) ->
    String;
to_binary(String) when is_list(String) ->
    unicode:characters_to_binary(String);
to_binary(String) when is_atom(String) ->
    atom_to_binary(String, utf8).

login(Conn = #conn{env=Env, socket=Socket, packet_size=PktSize}, Timeout) ->
    TokenStream = ?ENCODER:encode_tokens([{login, Env}]),
    DataStream = ?ENCODER:encode_packets(TokenStream, login, PktSize),
    case send(Socket, DataStream) of
        ok ->
            case handle_resp(Conn, Timeout) of
                {ok, TokensBufer, _, Conn2 = #conn{state = auth_negotiate}} ->
                    {AuthParams, _} = take_token_value(params, TokensBufer, []),
                    login(Conn2, AuthParams, Timeout);
                {ok, _, _, Conn2} ->
                    {ok, Conn2};
                Other ->
                    Other
            end;
        {error, Reason} ->
            handle_error(socket, Reason, Conn)
    end.

login(Conn = #conn{env=Env, socket=Socket, packet_size=PktSize}, AuthParams, Timeout) ->
    _Pass    = proplists:get_value(password, Env),
    EncPass = case AuthParams of
        [1,_Key] -> <<>>;  %% TODO public_key_encrypt
        [_Salt]  -> <<>>   %% TODO feal_encrypt
    end,
    Formats = [#format{datatype=?TDS_TYPE_VARBINARY, datatype_group=variable, datatype_max_len=255}],
    Formats2 = [#format{datatype=?TDS_TYPE_VARCHAR, datatype_group=variable, datatype_max_len=255}] ++ Formats,
    TokenList = [{message, 1, 2},{paramsformat, 1, Formats},{params, [EncPass]},
                 {message, 1, 3},{paramsformat, 2, Formats2},{params, [null, EncPass]}],
    TokenStream = ?ENCODER:encode_tokens(TokenList),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    case send(Socket, DataStream) of
        ok              -> handle_empty_resp(Conn, Timeout);
        {error, Reason} -> handle_error(socket, Reason, Conn)
    end.

system_query(Conn = #conn{state=connected, socket=Socket, 
        packet_size=PktSize}, Query, Timeout) ->
    BQuery = to_binary(Query),
    TokenStream = ?ENCODER:encode_tokens([{language, BQuery}]),
    DataStream = ?ENCODER:encode_packets(TokenStream, normal, PktSize),
    case send(Socket, DataStream) of
        ok              -> handle_empty_resp(Conn, Timeout);
        {error, Reason} -> handle_error(socket, Reason, Conn)
    end.

handle_error(remote, Reason, Conn) ->
    {error, remote, Reason, Conn};
handle_error(Type, Reason, Conn) ->
    _ = disconnect(Conn, 0),
    {error, Type, Reason, Conn#conn{state = disconnected}}.

handle_empty_resp(Conn, Timeout) ->
    case handle_resp(Conn, Timeout) of
        {ok, _, _, Conn2} ->
            {ok, Conn2};
        Other ->
            Other
    end.

handle_prepare_resp(Conn = #conn{prepared = Prepared}, Timeout) ->
    case handle_resp(Conn, Timeout) of
        {ok, TokensBufer, _, Conn2} ->
            {TokenDynamic, TokensBufer2} = take_token(dynamic, TokensBufer),
            {ParamsFormat, _TokensBufer3} = take_token(paramsformat, TokensBufer2),
            ParamsFormat2 = ?ENCODER:prepare_input_dataformat(ParamsFormat),
            {dynamic, ack, _Status, Id} = TokenDynamic,
            Conn3 = Conn2#conn{
                prepared = [{Id, ParamsFormat2}|Prepared]
            },
            {ok, Conn3};
        Other ->
            Other
    end.

handle_unprepare_resp(Conn = #conn{prepared = Prepared}, Timeout) ->
    case handle_resp(Conn, Timeout) of
        {ok, TokensBufer, _, Conn2} ->
            {TokenDynamic, _TokensBufer2} = take_token(dynamic, TokensBufer),
            {dynamic, ack, _Status, Id} = TokenDynamic,
            Conn3 = Conn2#conn{
                prepared = lists:keydelete(Id, 1, Prepared)
            },
            {ok, Conn3};
        Other ->
            Other
    end.

handle_query_resp(Conn, Timeout) ->
    case handle_resp(Conn, Timeout) of
        {ok, _TokensBufer, Result, Conn2} ->
            %Result2 = drop_inproc_updates(Result),
            {ok, Result, Conn2};
        Error -> Error
    end.

handle_execute_resp(Conn, Timeout) ->
    case handle_resp(Conn, Timeout) of
        {ok, _TokensBufer, Result, Conn2} ->
            %Result2 = drop_inproc_updates(Result),
            {ok, Result, Conn2};
        Error -> Error
    end.

handle_resp(Conn = #conn{socket=Socket}, Timeout) ->
    case recv(Socket, Timeout) of
        {ok, BinaryData} ->
            decode_stream(BinaryData, [], [], Conn);
        {error, Reason} ->
            handle_error(socket, Reason, Conn)
    end.

decode_stream(Stream, TokensBufer, Results, Conn) ->
    case ?DECODER:decode_token(Stream, TokensBufer) of
        {ok, Token, Stream2} ->
            case element(1, Token) of
            Done when Done == done; Done == doneinproc; Done == doneproc ->
                %io:format("TokensBufer~p~n", [TokensBufer]),
                %io:format("DoneToken ~p~n", [Token]),
                case handle_done_token(Token, TokensBufer, Results) of
                    {next_token, Results2} ->
                        decode_stream(Stream2, [], Results2, Conn);
                    Result ->
                        erlang:append_element(Result, Conn)
                end;
            loginack ->
                Conn2 = handle_loginack_token(Token, Conn),
                decode_stream(Stream2, TokensBufer, Results, Conn2);
            capability ->
                Conn2 = handle_capability_token(Token, Conn),
                decode_stream(Stream2, TokensBufer, Results, Conn2);
            envchange ->
                Conn2 = handle_envchange_token(Token, Conn),
                decode_stream(Stream2, TokensBufer, Results, Conn2);
            _ ->
                decode_stream(Stream2, [Token|TokensBufer], Results, Conn)
            end;
        {error, Reason} ->
            handle_error(local, Reason, Conn)
    end.

handle_loginack_token({loginack, ConnConn, TdsVer, Server}, Conn) ->
    Conn#conn{state = ConnConn, tds_ver = TdsVer, server = Server}.

handle_capability_token({capability, SrvReqCap, SrvRespCap}, Conn) ->
    %ReqCap = ?JAMDB_REQ_CAP -- SrvReqCap,
    %RespCap = ?JAMDB_RESP_CAP -- SrvRespCap,
    %io:format("ReqCap~p~n", [ReqCap]),
    %io:format("RespCap~p~n", [RespCap]),
    Conn#conn{req_capabilities = SrvReqCap, resp_capabilities = SrvRespCap}.

handle_done_token({_, Status, _TrnsctConn, Count}, TokensBufer, Results) ->
    TokensBuferR = lists:reverse(TokensBufer),
    handle_done_status(Status, Count, TokensBuferR, Results).

handle_done_status([?TDS_DONE_MORE|_], _Count, _TokensBufer, Results) ->
    {next_token, Results};
handle_done_status([?TDS_DONE_COUNT|Status], Count, TokensBufer, Results) ->
    {Result, TokensBufer2} = take_result(TokensBufer, Count),
    handle_done_status(Status, Count, TokensBufer2, [Result|Results]);
handle_done_status([?TDS_DONE_PROC|Status], Count, TokensBufer, Results) ->
    %Results2 = drop_inproc_updates(Results),
    {ProcResult, TokensBufer2} = take_procedure_result(TokensBufer),
    handle_done_status(Status, Count, TokensBufer2, [ProcResult|Results]);
handle_done_status([Flag|Status], Count, TokensBufer, Results) 
        when Flag =:= ?TDS_DONE_EVENT; Flag =:= ?TDS_DONE_ATTN; 
                Flag =:= ?TDS_DONE_INXACT ->
    handle_done_status(Status, Count, TokensBufer, Results);
handle_done_status([?TDS_DONE_ERROR|_Status], _Count, TokensBufer, _Results) ->
    {Message, _} = take_token(message, TokensBufer),
    #message{msg_number=MsgNumber, msg_body=MsgBody} = Message,
    {ok, TokensBufer, [{proc_result, MsgNumber, MsgBody}]};
handle_done_status([], Count, TokensBufer, Results) ->
    Results2 = drop_inproc_results(Results),
    case Results2 of
        [] ->
            {ok, TokensBufer, [{affected_rows, Count}]};
        _ ->
            {ok, TokensBufer, lists:reverse(Results2)}
    end.

take_result(TokensBufer, Count) ->
    case take_token(rowformat, TokensBufer) of
        {undefined, _} ->  %% on updates
            {{affected_rows, Count}, TokensBufer};
        {{rowformat, _Amount, RowFormat}, TokensBufer2} ->
            FieldNames = [get_field_name(Fmt) || Fmt <- RowFormat],
            {MetaInfo, TokensBufer3} = take_metainfo(TokensBufer2),
            {TokensList, TokensBufer4} = take_tokens(row, TokensBufer3, Count),
            Rows = [Row || {row, Row} <- TokensList],
            {{result_set, FieldNames, MetaInfo, Rows}, TokensBufer4}
    end.

take_procedure_result(TokensBufer) ->
    {Status, TokensBufer2} = take_token_value(returnstatus, TokensBufer),
    %% Get OutParams from returnvalue token if simple table
    %{Result, TokensBufer2} = take_tokens(returnvalue, TokensBufer, all),
    %{[Value || {returnvalue, Value} <- Result], TokensBufer2}.
    %% Get OutParams from params token if widetable
    {OutParams, TokensBufer3} = take_token_value(params, TokensBufer2, []),
    {{proc_result, Status, OutParams}, TokensBufer3}.

drop_inproc_results(Results) ->
    lists:filter(fun ({proc_result, undefined, []}) -> false; (_) -> true end, Results).
%drop_inproc_updates(Results) ->
    %lists:filter(fun ({affected_rows, _}) -> false; (_) -> true end, Results).

get_field_name(#format{label_name = <<>>, column_name = ColumnName}) ->
    ColumnName;
get_field_name(#format{label_name = LabelName}) ->
    LabelName.

take_metainfo(TokensBufer) ->
    case take_token(orderby, TokensBufer) of
        {undefined, _} ->
            {[], TokensBufer};
        {Token, TokensBufer2} ->
            {[Token], TokensBufer2}
    end.

handle_envchange_token({envchange, EnvChange}, Conn) ->
    lists:foldl(fun set_env/2, Conn, EnvChange).

set_env({packet_size = Key, NewValue, _OldValue}, Conn) ->
    Value = list_to_integer(binary_to_list(NewValue)),
    _ = inet:setopts(Conn#conn.socket, [{buffer, Value}]),
    Env = lists:keystore(Key, 1, Conn#conn.env, {Key, Value}), 
    Conn#conn{env = Env, packet_size = Value};
set_env({Key, NewValue, _OldValue}, Conn) ->
    Env = lists:keystore(Key, 1, Conn#conn.env, {Key, NewValue}),
    Conn#conn{env = Env}.

take_tokens(TokenName, TokensBufer, Count) ->
    take_tokens(TokenName, TokensBufer, Count, []).

take_tokens(TokenName, TokensBufer, Count, Result) when Count > 0 ->
    case take_token(TokenName, TokensBufer) of
        {undefined, _} ->
            {lists:reverse(Result), TokensBufer};
        {TokenTuple, TokensBufer2} ->
            take_tokens(TokenName, TokensBufer2, Count-1, [TokenTuple|Result])
    end;
take_tokens(_TokenName, TokensBufer, _Count, Result) ->
    {lists:reverse(Result), TokensBufer}.

take_token(TokenName, TokensBufer) ->
    case lists:keytake(TokenName, 1, TokensBufer) of
        {value, TokenTuple, TokensBufer2} ->
            {TokenTuple, TokensBufer2};
        false ->
            {undefined, TokensBufer}
    end.

take_token_value(Name, TokensBufer) ->
    take_token_value(Name, TokensBufer, undefined).

take_token_value(Name, TokensBufer, Default) ->
    case take_token(Name, TokensBufer) of
        {undefined, _} ->
            {Default, TokensBufer};
        {{Name, Value}, TokensBufer2} ->
            {Value, TokensBufer2}
    end.

send(Socket, Data) ->
    gen_tcp:send(Socket, Data).

recv(Socket, Timeout) ->
    recv(Socket, Timeout, <<>>, <<>>).

recv(Socket, Timeout, Buffer, ResultData) ->
    case ?DECODER:decode_packet(Buffer) of
        {ok, 0, PacketBody, Buffer2} ->
            ResultData2 = <<ResultData/binary, PacketBody/binary>>,
            recv(Socket, Timeout, Buffer2, ResultData2);
        {ok, 1, PacketBody, _} ->
            {ok, <<ResultData/binary, PacketBody/binary>>};
        {error, incomplete_packet} ->
            case gen_tcp:recv(Socket, 0, Timeout) of
                {ok, NetworkData} ->
                    NewBuffer = <<Buffer/bits, NetworkData/bits>>,
                    recv(Socket, Timeout, NewBuffer, ResultData);
                Error ->
                    Error
            end
    end.
