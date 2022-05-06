-module(coletor).
-export([run/0]).

run() -> 
    {ok,Context} = erlzmq:context(),
    {ok,SocketDevices} = erlzmq:socket(Context,[rep,{active,false}]),
    {ok,SocketAgregador} = erlzmq:socket(Context,[push,{active,false}]),
    ok = erlzmq:bind(SocketDevices,"tcp://*:3001"),
    ok = erlzmq:connect(SocketAgregador,"tcp://localhost:3002"),
    Resend = spawn(fun()->resend(SocketAgregador) end),
    spawn(fun()->handle(SocketDevices,Resend) end).

handle(Devices,Agreg) ->
    case erlzmq:recv(Devices) of
        {ok, Data} ->
            io:format("Received: ~p\n",[Data]),
            String = binary_to_list(Data),
            case String of 
                "auth:" ++ Dados ->
                    Split = string:split(Dados,";",all),
                    Id = lists:nth(1,Split),
                    Pass = lists:nth(2,Split),
                    case loginManager:login_and_create(Id,Pass) of
                        ok_register ->
                            erlzmq:send(Devices,list_to_binary("1:Dispositivo Registado com sucesso")),
                            Agreg ! {ok, list_to_binary("registo:"++ Id)};
                        ok ->
                            erlzmq:send(Devices,list_to_binary("1:Login feito com sucesso")),
                            Agreg ! {ok, list_to_binary("login:"++ Id)};
                        _ ->
                            erlzmq:send(Devices,list_to_binary("0:Password errada"))
                    end;
                "tipo:" ++ Dados ->
                    Split = string:split(Dados,";",all),
                    Id = lists:nth(1,Split),
                    Tipo = lists:nth(2,Split),
                    Agreg ! {ok, list_to_binary("tipo:"++Id++";"++Tipo)},
                    erlzmq:send(Devices,list_to_binary("1:Tipo Alterado"));

                "logout:" ++ Id ->
                    loginManager:logout(Id),
                    Agreg ! {ok, list_to_binary("logout:"++Id)},
                    erlzmq:send(Devices,list_to_binary("2:Logging Out"))
            end,
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