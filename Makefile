
build: coletor java 

coletor:
	erlc -I dependencies/erlzmq2/include -o Coletor/ Coletor/erlzmq.erl
	erlc -I dependencies/erlzmq2/include -o Coletor/ Coletor/erlzmq_nif.erl
	erlc -o Coletor/ Coletor/coletor.erl Coletor/loginManager.erl

java:
	javac -cp .:dependencies/jar/jeromq-0.5.2.jar Agregador/*.java
	javac -cp ::dependencies/jar/jeromq-0.5.2.jar Dispositivos/*.java

clean:
	-@rm Agregador/*.class
	-@rm Dispositivos/*.class
	-@rm Coletor/*.beam