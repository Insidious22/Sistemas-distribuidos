# Entrega TCP - Acelerografo con concurrencia, eleccion y logs globales

Esta carpeta contiene solo el ejercicio del acelerografo TCP.

```text
cliente/    codigo de las estaciones de acelerografo
servidor/   codigo del servidor TCP redundante
```

## Compilar

En cada maquina, entrar a la carpeta que corresponda y compilar:

Servidor:

```powershell
cd servidor
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

Cliente:

```powershell
cd cliente
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

## Levantar servidores

Servidor A, maquina 192.16.108.23:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1 SERV-A 3
```

Servidor B, maquina 192.16.108.24:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1 SERV-B 2
```

Servidor C, maquina 192.16.108.25:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1 SERV-C 1
```

El segundo parametro del servidor es el numero de permisos del semaforo.
Para demostrar exclusion mutua, usar `1`.

## Levantar clientes

Cliente 1:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500,192.16.108.24:7500,192.16.108.25:7500 7500 EC-UNO 1000 0 0
```

Cliente 2:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500,192.16.108.24:7500,192.16.108.25:7500 7500 EC-DOS 1000 0 0
```

Cliente 3:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500,192.16.108.24:7500,192.16.108.25:7500 7500 EC-TRES 1000 0 0
```

Parametros del cliente:

```text
lista de servidores     IP:puerto de A, B y C
7500                    puerto por defecto
EC-UNO                  id del acelerografo
1000                    intervalo entre lecturas en ms
0                       sin limite de repeticiones
0                       sin limite de duracion
```

## Comportamiento esperado

Con A, B y C vivos:

```text
EC-UNO  -> SERV-A
EC-DOS  -> SERV-A
EC-TRES -> SERV-A
```

Si cae SERV-A:

```text
clientes -> SERV-B
```

Si cae SERV-B:

```text
clientes -> SERV-C
```

Si vuelve SERV-B, los clientes migran de C a B.
Si vuelve SERV-A, los clientes migran de B a A.

Si caen los tres servidores, cada cliente sigue generando lecturas y las guarda en cola local.
Cuando vuelve un servidor, sincroniza la cola pendiente en orden por `SEQ`.

## Logs

La evidencia principal queda concentrada en un solo archivo:

```powershell
Get-Content logs\sismo_distribuido.log -Tail 220
```

Buscar clientes especificos:

```powershell
Select-String logs\sismo_distribuido.log -Pattern "EC-UNO|EC-DOS|EC-TRES|EC-CUATRO"
```

Buscar bloqueos y sincronizacion:

```powershell
Select-String logs\sismo_distribuido.log -Pattern "BLOQUEADO|COLA|SINCRONIZADO|PERMISO"
```

Archivos usados:

```text
logs\sismo_distribuido.log  log principal en tablas: cliente, servidor, semaforo, cola, eleccion y metricas
bitacora_sismica.txt        lecturas aceptadas dentro de la region critica
```

El log principal se escribe en bloques tipo tabla ASCII para que se pueda leer como evidencia.

## Prueba principal: clientes permanentes, sismo de 10 segundos

Esta es la prueba recomendada para la evidencia. Los clientes son estaciones persistentes.
Todos generan lecturas. Los mas cercanos al evento tienen mayor prioridad y variacion mas fuerte.
El servidor se levanta con 1 permiso, por eso solo 1 cliente puede comunicarse con el servidor a la vez.
Los demas reciben `ACK_BLOQUEADO`, el servidor cierra esa conexion, el cliente guarda la lectura en cola local, sigue generando nuevas lecturas y luego reconecta para sincronizar la cola cuando el servidor queda libre.
El sismo dura 10 segundos, pero los clientes no se cierran solos: siguen conectados y enviando cada 1 segundo hasta que se detengan manualmente con Ctrl + C.

Levantar un solo servidor activo con 1 permiso y zona critica de 5 segundos:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1 SERV-A 3 5000
```

Parametros nuevos del servidor:

```text
7500  puerto
1     permisos del semaforo, permite 1 cliente comunicandose con el servidor
SERV-A id del servidor
3     prioridad del servidor para eleccion
5000  retardo en ms dentro de la region critica
```

Cliente cercano 1, prioridad alta:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500 7500 EC-UNO 1000 0 0 1 10 10
```

Cliente cercano 2, prioridad alta:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500 7500 EC-DOS 1000 0 0 2 18 10
```

Cliente lejano 3, menor prioridad:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500 7500 EC-TRES 1000 0 0 3 42 10
```

Cliente lejano 4, menor prioridad:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23:7500 7500 EC-CUATRO 1000 0 0 4 55 10
```

Parametros nuevos del cliente:

```text
192.16.108.23:7500  servidor o lista de servidores
7500                 puerto por defecto
EC-UNO               id del acelerografo
1000                 intervalo entre lecturas en ms
0                    repeticiones, 0 significa sin limite por cantidad
0                    duracion total del cliente, 0 significa permanente hasta Ctrl + C
1                    prioridad del cliente, 1 es la mas alta
10                   distancia al evento en metros
10                   duracion del sismo local en segundos; al terminar, el cliente sigue vivo
```

Resultado esperado:

```text
EC-UNO entra primero porque tiene prioridad/distancia favorable.
EC-DOS, EC-TRES y EC-CUATRO intentan enviar, reciben ACK_BLOQUEADO, salen de esa conexion y encolan localmente.
Mientras siguen bloqueados, continuan generando lecturas y crece la cola local.
Cuando el semaforo libera el permiso, reconectan y sincronizan primero la cola pendiente.
Despues de vaciar la cola, vuelven a enviar en tiempo real.
Cuando pasan los 10 segundos de sismo, los clientes siguen conectados y enviando lecturas normales.
```

Log para evidenciarlo:

```powershell
Get-Content logs\sismo_distribuido.log -Tail 220
```

Eventos clave:

```text
CLIENTE_BLOQUEADO_POR_SEMAFORO
COLA_LOCAL_ENCOLADO
COLA_BLOQUEO_ENCOLADO
COLA_DESBLOQUEADA_ENVIO_OK
COLA_LOCAL_SINCRONIZADO
TCP_LECTURA_BLOQUEADA
TCP_CONEXION_RECHAZADA_POR_OCUPACION
TCP_LECTURA_RECIBIDA
PERMISO_OTORGADO
PERMISO_LIBERADO
INTERBLOQUEO_DETECTADO
INTERBLOQUEO_RESUELTO
REINTENTO_SEGURO_OK
```

## Interbloqueo formal

Ademas del semaforo de admision, el servidor simula interbloqueo real durante lecturas con `MOVIMIENTO:SI`.
Para eso usa dos recursos internos:

```text
CANAL_ENVIO
BITACORA_SISMICA
```

Los clientes con prioridad impar intentan tomar:

```text
CANAL_ENVIO -> BITACORA_SISMICA
```

Los clientes con prioridad par intentan tomar:

```text
BITACORA_SISMICA -> CANAL_ENVIO
```

Si dos clientes entran al mismo tiempo, puede pasar:

```text
EC-UNO tiene CANAL_ENVIO y espera BITACORA_SISMICA
EC-DOS tiene BITACORA_SISMICA y espera CANAL_ENVIO
```

Ese ciclo es el interbloqueo. El servidor lo detecta, registra el ciclo, libera un recurso como victima y reintenta en orden seguro.

Buscar evidencia del interbloqueo:

```powershell
Select-String logs\sismo_distribuido.log -Pattern "INTERBLOQUEO|RECURSO_TOMADO|ESPERA_RECURSO|REINTENTO_SEGURO"
```

Secuencia esperada:

```text
RECURSO_TOMADO
ESPERA_RECURSO
INTERBLOQUEO_DETECTADO
INTERBLOQUEO_RESUELTO
REINTENTO_SEGURO_OK
```

## Prueba secundaria: bloqueo por consulta de sismo

Esta prueba usa un solo servidor activo, 2 o 3 clientes que detectan movimiento y un proceso que simula el sismo durante 10 segundos.
El servidor tiene un canal critico de consulta con 1 permiso. Mientras informa al cliente con prioridad, los otros clientes reciben aviso de bloqueo despues de 1 segundo y quedan esperando su turno.

Levantar el servidor:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1 SERV-A 3
```

Disparar el sismo de 10 segundos desde otra terminal:

```powershell
java -cp out socket.conconexion.acelerografo.SimuladorSismoTCP 192.16.108.23 7500 SISMO-001 10000 5000
```

Mientras el sismo esta activo, lanzar los clientes de consulta:

```powershell
java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP 192.16.108.23 7500 EC-UNO 1 5000
```

```powershell
java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP 192.16.108.23 7500 EC-DOS 2 5000
```

```powershell
java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP 192.16.108.23 7500 EC-TRES 3 5000
```

Parametros del cliente de consulta:

```text
192.16.108.23  servidor activo
7500           puerto
EC-UNO         id del cliente/acelerografo
1              prioridad del cliente, 1 es la prioridad mas alta
5000           tiempo en ms que el servidor queda ocupado informando a ese cliente
```

Tambien existe el cliente de alerta persistente, por si se quiere demostrar suscripcion:

```powershell
java -cp out socket.conconexion.acelerografo.ClienteAlertaSismoTCP 192.16.108.23 7500 EC-UNO
```

Parametros del simulador:

```text
192.16.108.23  servidor activo
7500           puerto
SISMO-001      id del evento
10000          duracion del evento en ms
5000           tiempo que el servidor ocupa el canal por cada cliente consultando o suscrito
```

Ver el log de la prueba:

```powershell
Get-Content logs\sismo_distribuido.log -Tail 160
```

Eventos importantes:

```text
SISMO_INICIADO
SISMO_MOVIMIENTO_VARIA
CLIENTE_DETECTA_MOVIMIENTO_Y_CONSULTA
CLIENTE_ENTRA_CONSULTA_SISMO
CLIENTE_CONSULTA_BLOQUEADO
RESPUESTA_CONSULTA_SISMO_ENVIADA
CLIENTE_LIBERA_CONSULTA_SISMO
SISMO_FINALIZADO
```

Resultado esperado:

```text
EC-UNO entra primero porque tiene prioridad 1.
EC-DOS y EC-TRES reciben BLOQUEADO_CONSULTA aproximadamente 1 segundo despues.
EC-DOS entra cuando EC-UNO libera el canal.
EC-TRES entra cuando EC-DOS libera el canal.
El log SISMO_MOVIMIENTO_VARIA muestra que xG, yG, zG e intensidad cambian durante los 10 segundos.
```

Ejemplo de comandos listos para la red de clase:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1 SERV-A 3
java -cp out socket.conconexion.acelerografo.SimuladorSismoTCP 192.16.108.23 7500 SISMO-001 10000 5000
java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP 192.16.108.23 7500 EC-UNO 1 5000
java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP 192.16.108.23 7500 EC-DOS 2 5000
java -cp out socket.conconexion.acelerografo.ClienteConsultaSismoTCP 192.16.108.23 7500 EC-TRES 3 5000
```

Los comandos se ejecutan en terminales separadas. El simulador debe ejecutarse antes o al mismo tiempo que los clientes de consulta para que respondan `MOVIMIENTO:SI`.
