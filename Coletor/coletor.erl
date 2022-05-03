-module(coletor).
-export([run/0]).

run() -> 
    {ok,Context} = erlzmq:context(),
    {ok,SocketDevices} = erlzmq:socket(Context,[pull,{active,false}]),
    {ok,SocketAgregador} = erlzmq:socket(Context,[push,{active,false}]),
    ok = erlzmq:bind(SocketDevices,"tcp://*:3001"),
    ok = erlzmq:connect(SocketAgregador,"tcp://localhost:3002"),
    Resend = spawn(fun()->resend(SocketAgregador) end),
    spawn(fun()->handle(SocketDevices,Resend) end).

handle(Devices,Agreg) ->
    case erlzmq:recv(Devices) of
        {ok, Data} ->
            io:format("Received: ~p\n",[Data]),
            Agreg ! {ok, Data},
            handle(Devices,Agreg);
        _ -> 
            handle(Devices,Agreg)
    end.

resend(Agreg) ->
    receive
        {ok, Data} -> 
            io:format("Sending: ~p\n",[Data]),
            erlzmq:send(Agreg,Data),
            resend(Agreg);
        _ ->
            resend(Agreg)
    end.