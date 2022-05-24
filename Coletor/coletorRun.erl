-module(coletorRun).
-export([main/1]).

main(Zona)->
    loginManager:start(),
    coletor:run(Zona).