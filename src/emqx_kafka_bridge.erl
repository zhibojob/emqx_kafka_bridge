%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2016 Huang Rui<vowstar@gmail.com>, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqx_kafka_bridge.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqx_kafka_bridge).

-include_lib("emqx/include/emqx.hrl").

-export([load/1, unload/0]).

%% Hooks functions
-export([on_client_connected/3, on_client_disconnected/4]).
-export([on_message_publish/2]).

-define(KAFKA_CLIENT, brod_client).

%% Called when the plugin application start
load(_Env) ->
    %% Get parameters
    {ok, Kafka} = application:get_env(?MODULE, kafka),
    Broker = proplists:get_value(broker, Kafka),
    Filter = list_to_binary(proplists:get_value(filter, Kafka)),
    Topic = list_to_binary(proplists:get_value(topic, Kafka)),

    {ok, _} = application:ensure_all_started(brod),

    %% Startup kafka
    ClientConfig = [{auto_start_producers, true}, {default_producer_config, [{required_acks, 0}]}],
    ok = brod:start_client([Broker], ?KAFKA_CLIENT, ClientConfig),

    io:format("[emqx_kafka_bridge.kafka_started] ~p~n", [Broker]),

    emqx:hook('client.connected', fun ?MODULE:on_client_connected/3, [{Topic}]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/4, [{Topic}]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [{Topic, Filter}]).

%%-----------client connect start-----------------------------------%%

on_client_connected(#{clientid := ClientId, username := Username, auth_result := AuthResult}, #{connected_at := ConnectedAt}, {Topic}) ->
    % io:format("client ~s connected, connack: ~w~n", [ClientId, ConnAck]),

    case emqx_json:safe_encode([
        {<<"type">>, <<"connected">>},
        {<<"auth_result">>, AuthResult},
        {<<"client_id">>, ClientId},
        {<<"username">>, Username},
        {<<"cluster_node">>, node()},
        {<<"ts">>, ConnectedAt}
    ]) of
        {ok, Json} ->
            kafka_send_message(Topic, ClientId, Json);
        {error, Reason} ->
            io:format("[emqx_kafka_bridge.on_client_connected] safe_encode error reason: ~w~n", [Reason])
    end,
    ok;
on_client_connected(#{}, _ConnInfo, _Env) ->
    ok.

%%-----------client connect end-------------------------------------%%



%%-----------client disconnect start---------------------------------%%
on_client_disconnected(#{clientid := ClientId, username := Username}, Reason, #{disconnected_at := DisconnectedAt}, {Topic}) ->
    % io:format("client ~s disconnected, reason: ~w~n", [ClientId, Reason]),

    case emqx_json:safe_encode([
        {<<"type">>, <<"disconnected">>},
        {<<"client_id">>, ClientId},
        {<<"username">>, Username},
        {<<"reason">>, Reason},
        {<<"cluster_node">>, node()},
        {<<"ts">>, DisconnectedAt}
    ]) of
        {ok, Json} -> 
            kafka_send_message(Topic, ClientId, Json);
        {error, JsonReason} ->
            io:format("[emqx_kafka_bridge.on_client_disconnected] safe_encode error reason: ~w~n", [JsonReason])
    end,
    ok.

%%-----------client disconnect end-----------------------------------%%



%%-----------message publish start--------------------------------------%%

%% transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{topic = Topic}, {KafkaTopic, Filter}) ->
    % io:format("publish ~s~n", [emqx_message:format(Message)]),   

    case emqx_topic:match(Topic, Filter) of
        true ->
            Payload = case is_binary_topic(Topic) of
                true -> base64:encode(Message#message.payload);
                false -> Message#message.payload
            end,

            {FromClientId, FromUsername} = format_from(Message),
            case emqx_json:safe_encode([
                {<<"type">>, <<"published">>},
                {<<"client_id">>, FromClientId},
                {<<"username">>, FromUsername},
                {<<"topic">>, Topic},
                {<<"payload">>, Payload},
                {<<"qos">>, Message#message.qos},
                {<<"cluster_node">>, node()},
                {<<"ts">>, Message#message.timestamp}
            ]) of
                {ok, Json} ->
                    kafka_send_message(KafkaTopic, FromClientId, Json);
                {error, Reason} ->
                    io:format("[emqx_kafka_bridge.on_message_publish] safe_encode error reason: ~w~n", [Reason])
            end;
        _Else ->
            ok
    end,

    {ok, Message}.

format_from(#message{from = ClientId, headers = #{username := Username}}) ->
    {ClientId, Username};
format_from(#message{from = ClientId, headers = _HeadersNoUsername}) ->
    {ClientId, <<"undefined">>}.

%%-----------message publish end--------------------------------------%%

%% ===================================================================
%% ekaf
%% ===================================================================

kafka_send_message(Topic, Key, Message) ->
    % io:format("[emqx_kafka_bridge.kafka_send_message] ~p~n", [Message]).
    PartFun = fun(_Topic, PartCnt, MyKey, _Value) ->
        ChosenPart = erlang:phash2(MyKey, PartCnt),
        % io:format("[select partition] ~p~n", [[PartCnt, ChosenPart]]),
        {ok, ChosenPart}
    end,
    case brod:produce_no_ack(?KAFKA_CLIENT, Topic, PartFun, Key, Message) of
        {error, Reason} ->
            io:format("[emqx_kafka_bridge.kafka_send_message]: ~w~n", [Reason]);
        _Else ->
            ok
    end.


%% Called when the plugin application stop
unload() ->
    brod:stop_client(?KAFKA_CLIENT),
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).

%% 是否为二进制Topic（结尾为：/_b）
is_binary_topic(Topic) ->
    TopicLen = byte_size(Topic),
    TopicLen >= 3 andalso binary:match(Topic, <<"/_b">>, [{scope, {TopicLen - 3, 3}}]) =/= nomatch.






