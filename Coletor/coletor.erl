-module(coletor).
-export([run/1]).

run(Zona) -> 

    PortaDispositivo = 3001 + Zona*100, 
    PortaAgregador = 3002 + Zona*100, 
    {ok,Context} = erlzmq:context(),
    {ok,SocketAgregador} = erlzmq:socket(Context,[push,{active,false}]),
    ok = erlzmq:connect(SocketAgregador,"tcp://localhost:"++integer_to_list(PortaAgregador)),
    {ok,SocketDevices} = gen_tcp:listen(PortaDispositivo, [binary, {active, once}, {packet, line},
                                                    {reuseaddr, true}]),
    Response = spawn(fun() -> response(SocketAgregador) end),
    spawn(fun() -> acceptor(SocketDevices, Response) end),
    % Usado para poder usar o script para correr o coletor mais facilmente
    ok = gen_tcp:controlling_process(SocketDevices, Response),
    ok.

acceptor(LSock, SocketAgregador) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn(fun() -> acceptor(LSock, SocketAgregador) end),
    handle(Sock, SocketAgregador, ok).

handle(Devices, Agreg, Username) ->
    receive
        {tcp ,_, Data} ->
            inet:setopts(Devices, [{active, once}]),
            String = re:replace(binary_to_list(Data),"\n","",[global,{return,list}]),
            io:format("Received: ~p\n",[String]),
            case String of 
                "auth:" ++ Dados ->
                    Split = string:split(Dados,";",all),
                    Id = lists:nth(1,Split),
                    Pass = lists:nth(2,Split),
                    Tipo = lists:nth(3,Split),
                    case loginManager:login_and_create(Id,Pass,Tipo) of
                        ok_register ->
                            gen_tcp:send(Devices,list_to_binary("1:Dispositivo Registado com sucesso\n")),
                            Agreg ! {ok, list_to_binary("registo:" ++ Id ++ ";" ++ Tipo)},
                            handle(Devices, Agreg, Id);
                        ok ->
                            gen_tcp:send(Devices,list_to_binary("1:Login feito com sucesso\n")),
                            Agreg ! {ok, list_to_binary("login:" ++ Id ++ ";" ++ Tipo)},
                            handle(Devices, Agreg, Id);
                        _ ->
                            gen_tcp:send(Devices,list_to_binary("0:Password ou Tipo errado\n"))
                    end;
                "evento:" ++ Dados ->
                    Split = string:split(Dados,";",all),
                    Tipo = lists:nth(1,Split),
                    Agreg ! {ok, list_to_binary("evento:"++Username++";"++Tipo)},
                    gen_tcp:send(Devices,list_to_binary("1:Evento Alterado\n")),
                    handle(Devices,Agreg,Username);

                "logout" ->
                    loginManager:logout(Username),
                    Agreg ! {ok, list_to_binary("logout:"++Username)},
                    gen_tcp:send(Devices,list_to_binary("2:Logging Out\n"))
            end;
        {tcp_closed, _} -> 
            io:format("Logout ~p\n",[Username]),
            loginManager:logout(Username),
            Agreg ! {ok, list_to_binary("logout:"++Username)};

        {tcp_error,_,_} ->
            io:format("Logout ~p\n",[Username]),
            loginManager:logout(Username),
            Agreg ! {ok, list_to_binary("logout:"++Username)}

        after 5000 ->
            io:format("Away ~p\n",[Username]),
            Agreg ! {ok, list_to_binary("inativo:"++Username)},
            handle(Devices,Agreg,Username)

    end.

response(Agreg) ->
    receive
        {ok, Data} -> 
            io:format("Sending: ~p\n",[Data]),
            erlzmq:send(Agreg,Data),
            response(Agreg);
        _ ->
            response(Agreg)
    end.