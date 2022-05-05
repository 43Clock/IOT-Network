-module(loginManager).
-export([start/0,login_and_create/2,logout/1,online/0]).

start() -> 
    %register(login_manager,spawn(fun() -> loop() end)).
    % #{} é um mapa (?)
    register(?MODULE,spawn(fun() -> loop(#{}) end)).

rpc(Req) ->
    ?MODULE ! {Req,self()},
    receive 
        {Res, ?MODULE} -> 
                Res 
    end.

login_and_create(Username, Password) -> 
    rpc({login,Username,Password}).

logout(Username) -> 
    rpc({logout,Username}).

online() -> 
    rpc({online}).

%server process

loop(Map) ->
    receive
    {{login,Username,Password},From} -> 
        case maps:is_key(Username,Map) of
            false ->
                From ! {ok_register, ?MODULE},
                loop(maps:put(Username,{Password,true}, Map));
            true ->
                case maps:get(Username,Map) of
                    {Password,_} ->
                        From ! {ok,?MODULE},
                        loop(maps:update(Username,{Password,true},Map));
                    _ ->
                        From ! {invalid, ?MODULE},
                        loop(Map)
                end
        end;
    {{logout,Username},From} ->
        case maps:is_key(Username,Map) of
            false ->
                From ! {invalid, ?MODULE},
                loop(Map);
            true ->
                From ! {ok,?MODULE},
                {Password,_} = maps:get(Username,Map),
                loop(maps:update(Username,{Password,false},Map))
        end;

    {{online},From} -> 
        Filtered = maps:filter(fun(_,{_,B})->B end,Map),
        List = maps:keys(Filtered),
        From ! {List,?MODULE},
        loop(Map)
    end.