
build: coletor java 

coletor:
	erlc -I dependencies/erlzmq2/include -o Coletor/ Coletor/erlzmq.erl
	erlc -I dependencies/erlzmq2/include -o Coletor/ Coletor/erlzmq_nif.erl
	erlc -o Coletor/ Coletor/coletor.erl

java:
	javac -cp .:dependencies/jar/jeromq-0.5.2.jar Agregador.java
	javac -cp ::dependencies/jar/jeromq-0.5.2.jar Dispositivo.java

agregador:
	java -cp .:dependencies/jar/jeromq-0.5.2.jar Agregador

dispositivo:
	java -cp .:dependencies/jar/jeromq-0.5.2.jar Dispositivo

clean:
	-@rm *.class
	-@rm Coletor/*.beam