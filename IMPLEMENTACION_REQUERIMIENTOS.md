# Implementacion de requerimientos - Guia de sockets V2

## Estructura creada

Se creo una entrega limpia en `src/` respetando los paquetes pedidos por la guia:

```text
src/socket/sinconexion/simplex/
src/socket/conconexion/unilateral/
src/socket/conconexion/stream/
src/rmi/eco/
src/rmi/sum/
```

Los ZIP de `respaldo_original/no tocar/` se usaron solo como respaldo de consulta.
Los archivos Java originales de la carpeta principal no se modificaron; esta version queda separada para compilar y probar en terminal o importarla como proyecto JavaSE.

La carpeta quedo organizada asi:

```text
LEEME_PRIMERO.md
IMPLEMENTACION_REQUERIMIENTOS.md
src/                 codigo fuente final
out/                 clases .class compiladas
respaldo_original/   material original, duplicados, ZIP/RAR y PDF de apoyo
```

## Forma de trabajo usada

Compilacion general desde PowerShell:

```powershell
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

Ejecucion general:

```powershell
java -cp out paquete.Clase argumentos
```

En un proyecto JavaSE del IDE, use `src/` como carpeta de codigo fuente y ejecute cada clase `main` con los argumentos indicados en cada escenario.

## RMI - Servicio remoto Eco

Archivos:

- `src/rmi/eco/ServicioEco.java`
- `src/rmi/eco/ServicioEcoImpl.java`
- `src/rmi/eco/ServidorEco.java`
- `src/rmi/eco/ClienteEco.java`

Implementacion:

- `ServicioEco` define el contrato remoto con `Remote` y `RemoteException`.
- `ServicioEcoImpl` exporta el objeto remoto con `UnicastRemoteObject`.
- `ServidorEco` crea o reutiliza el registro RMI local, publica el servicio y muestra los nombres registrados.
- `ClienteEco` localiza el registro, busca el servicio y envia mensajes de prueba.

Comandos de prueba:

Terminal 1:

```powershell
java -cp out rmi.eco.ServidorEco 1099 eco
```

Terminal 2:

```powershell
java -cp out rmi.eco.ClienteEco localhost 1099 eco "Hola!" "Adios!"
```

Si se ejecuta sin argumentos, el servidor y el cliente piden los datos por consola y usan valores por defecto.

## RMI - Servicio remoto Sum

Objetivo:

Producir un programa cliente-servidor basado en Java RMI. El servidor ofrece el servicio de sumar 2 numeros enteros y regresa el resultado al cliente.

Archivos:

- `src/rmi/sum/ServicioSum.java`
- `src/rmi/sum/ServicioSumImpl.java`
- `src/rmi/sum/ServidorSum.java`
- `src/rmi/sum/ClienteSum.java`

Implementacion:

- `ServicioSum` define el metodo remoto `sumar(int, int)`.
- `ServicioSumImpl` realiza la suma y devuelve el entero resultante.
- `ServidorSum` crea o reutiliza el registro RMI, publica el servicio con el nombre `sum` y queda activo.
- `ClienteSum` localiza el servicio remoto, envia dos enteros y muestra el resultado.

Comandos de prueba:

Terminal 1:

```powershell
java -cp out rmi.sum.ServidorSum 50000 sum
```

Terminal 2:

```powershell
java -cp out rmi.sum.ClienteSum localhost 50000 sum 8 12
```

Resultado esperado:

```text
Busqueda completa
Servicio de Sum:
8 + 12 = 20
```

## Conceptos base para entender la practica

Un socket es el punto final de comunicacion entre dos procesos. En esta practica se trabajan dos familias:

- UDP con `DatagramSocket` y `DatagramPacket`.
- TCP con `ServerSocket`, `Socket`, `BufferedReader` y `PrintWriter`.

UDP envia mensajes independientes llamados datagramas. Cada datagrama viaja por separado y puede llegar, perderse, duplicarse o llegar en otro orden. Por eso UDP es rapido y simple, pero no garantiza entrega.

TCP crea una conexion logica entre cliente y servidor. Una vez establecida, los bytes se entregan en orden y de forma confiable. Por eso se usa cuando no se puede perder informacion, como reportes financieros o transacciones bancarias.

En las pruebas siempre conviene abrir dos terminales:

- Terminal del servidor o receptor: se ejecuta primero y queda esperando.
- Terminal del cliente o emisor: se ejecuta despues y envia datos.

Si aparece `Address already in use`, significa que ese puerto sigue ocupado. Cierre la terminal anterior con `Ctrl + C` o use otro puerto, por ejemplo `5500`, `5600` o `6000`.

## Escenario 1 - Telemetria ambiental UDP libre

Archivos:

- `src/socket/sinconexion/simplex/Emisor.java`
- `src/socket/sinconexion/simplex/Receptor.java`

Implementacion:

- El emisor genera lecturas simuladas cada 2 segundos por defecto.
- El formato enviado es `TEMP:XX.X;HUM:YY%`.
- El receptor escucha datagramas, imprime IP y puerto efimero del emisor.
- Si el mensaje coincide con el formato de telemetria, calcula promedios acumulados de temperatura y humedad en tiempo real.

Explicacion paso a paso:

1. El receptor se abre primero con un puerto, por ejemplo `5000`.
2. Internamente crea un `DatagramSocket` asociado a ese puerto.
3. El emisor crea su propio `DatagramSocket` sin puerto fijo; el sistema operativo le asigna un puerto efimero.
4. Cada 2 segundos el emisor genera una temperatura y humedad aleatorias.
5. La lectura se convierte a bytes con UTF-8 y se coloca dentro de un `DatagramPacket`.
6. El paquete se envia hacia `localhost:5000`.
7. El receptor recibe el paquete, convierte los bytes otra vez a texto y muestra quien lo envio.
8. Si el texto cumple el patron `TEMP:XX.X;HUM:YY%`, el receptor suma los valores y recalcula el promedio.

Por que funciona asi:

UDP no necesita handshake. El emisor puede mandar aunque el receptor no haya confirmado nada. Si el receptor no esta levantado, esos datagramas simplemente se pierden; el emisor no recibe un error normal de aplicacion porque UDP no verifica que haya alguien escuchando.

Comandos de prueba en dos terminales:

```powershell
java -cp out socket.sinconexion.simplex.Receptor 5000
java -cp out socket.sinconexion.simplex.Emisor localhost 5000 telemetria 2000
```

Prueba realizada: el receptor recibio tres lecturas, mostro `127.0.0.1:<puerto efimero>` y recalculo los promedios de temperatura/humedad.

Salida esperada resumida:

```text
Origen: 127.0.0.1:puerto_efimero
Mensaje completo: TEMP:30.5;HUM:43%
Promedios -> TEMP: 30.50; HUM: 43.00%
```

## Escenario 2 - Alertas y control de truncamiento

Archivos:

- `src/socket/sinconexion/simplex/Emisor.java`
- `src/socket/sinconexion/simplex/Receptor.java`

Implementacion:

- El emisor soporta modo `alerta` para mandar un mensaje largo como `ALERTA_INCENDIO_SECTOR_B`.
- El receptor ya no reserva un buffer fijo de 10 octetos. Usa un buffer de `65507`, que corresponde al maximo payload UDP practico.
- El mensaje se reconstruye con `datagrama.getLength()` para no imprimir basura del arreglo.

Explicacion paso a paso:

1. El problema original era tener `MAX_LON = 10`.
2. Si llega `ALERTA_INCENDIO_SECTOR_B`, el texto ocupa mas de 10 bytes.
3. El `DatagramPacket` solo puede guardar lo que cabe en el arreglo.
4. La parte sobrante del datagrama se descarta; no queda disponible para una segunda lectura.
5. Para evitarlo, el receptor usa un arreglo suficientemente grande antes de llamar a `receive`.
6. Luego usa `getLength()` para construir solo el texto realmente recibido.

Idea clave:

El tamano del buffer debe estar listo antes de recibir. No se puede saber el contenido completo despues si el buffer ya era pequeno, porque el resto del datagrama ya fue descartado por la pila de red.

Comando:

```powershell
java -cp out socket.sinconexion.simplex.Receptor 5000 1
java -cp out socket.sinconexion.simplex.Emisor localhost 5000 alerta ALERTA_INCENDIO_SECTOR_B
```

Prueba realizada: el receptor imprimio el mensaje completo `ALERTA_INCENDIO_SECTOR_B`, sin quedar limitado a 10 caracteres.

Analisis:

Con `MAX_LON = 10`, el sistema operativo entrega solo lo que cabe en el `DatagramPacket`; el resto se descarta silenciosamente. La JVM no lanza una excepcion por truncamiento del payload. Por eso el receptor fue cambiado a un buffer dinamicamente seguro antes de invocar `receive`.

Salida esperada resumida:

```text
Mensaje completo: ALERTA_INCENDIO_SECTOR_B
```

## Escenario 3 - Filtro de seguridad UDP conectado

Archivos:

- `src/socket/conconexion/unilateral/MiSocketDatagrama.java`
- `src/socket/conconexion/unilateral/Receptor.java`
- `src/socket/conconexion/unilateral/Emisor.java`

Implementacion:

- El receptor invoca `miSocket.connect(maquinaEmisora, puertoEmisor)` antes del bucle.
- El socket solo acepta datagramas provenientes de la IP y puerto configurados.
- Los datagramas de un emisor intruso apuntando al mismo puerto se descartan por el filtro del kernel y no llegan a la aplicacion.

Explicacion paso a paso:

1. Aunque UDP se llame "sin conexion", Java permite llamar `connect()` sobre un `DatagramSocket`.
2. Esa llamada no hace un saludo de red como TCP.
3. No se manda un paquete especial ni ocurre un three-way handshake.
4. Lo que cambia es el comportamiento local del socket.
5. El sistema operativo filtra los datagramas y solo deja pasar los que vengan de la IP y puerto conectados.
6. Si otro proceso envia al mismo puerto del receptor, pero desde un puerto distinto, el paquete se descarta antes de llegar al programa Java.

Diferencia importante:

En UDP conectado, "conexion" significa filtro local y destino por defecto. En TCP, "conexion" significa una sesion real entre dos extremos, con control de entrega, orden, retransmision y cierre.

Comandos:

```powershell
java -cp out socket.conconexion.unilateral.Receptor localhost 5001 5000
java -cp out socket.conconexion.unilateral.Emisor localhost 5000 5001 CALIBRAR
java -cp out socket.sinconexion.simplex.Emisor localhost 5000 alerta INTRUSO
```

Nota de ejecucion: para ver el descarte silencioso del intruso, el receptor debe estar conectado a un puerto de emisor legitimo especifico. Un emisor UDP libre que use otro puerto efimero no supera el filtro del socket conectado.

Que observar:

El receptor debe imprimir solo mensajes del emisor legitimo. El mensaje `INTRUSO` no debe aparecer en consola si viene desde un puerto distinto.

## Escenario 4 - Rafaga de comandos con ACK

Archivos:

- `src/socket/conconexion/unilateral/Emisor.java`
- `src/socket/conconexion/unilateral/Receptor.java`

Implementacion:

- El emisor envia exactamente 10 comandos secuenciales (`COMANDO#1` a `COMANDO#10`).
- Luego espera un unico ACK.
- El receptor recibe los 10 datagramas y responde `CALIBRADO_OK`.
- El emisor configura `setSoTimeout(5000)` para no quedarse bloqueado indefinidamente si UDP pierde un datagrama y el ACK nunca llega.

Explicacion paso a paso:

1. El receptor se ejecuta primero y queda esperando 10 comandos.
2. El emisor se conecta logicamente al receptor con UDP conectado.
3. El emisor envia una rafaga de 10 datagramas: `CALIBRAR#1`, `CALIBRAR#2`, etc.
4. Al terminar la rafaga, el emisor llama `recibeMensaje()` para esperar el ACK.
5. El receptor cuenta los 10 comandos recibidos.
6. Cuando completa el lote, responde una sola vez con `CALIBRADO_OK`.
7. El emisor imprime el ACK y finaliza.

Por que se agrego timeout:

`receive()` es bloqueante. Si el ACK nunca llega, el proceso queda esperando indefinidamente. `setSoTimeout(5000)` hace que la espera termine despues de 5 segundos y permita informar el problema.

Comandos:

```powershell
java -cp out socket.conconexion.unilateral.Receptor localhost 5001 5000
java -cp out socket.conconexion.unilateral.Emisor localhost 5000 5001 CALIBRAR
```

Prueba realizada: el emisor mando `CALIBRAR#1` hasta `CALIBRAR#10` y recibio `CALIBRADO_OK`.

Analisis:

Si el datagrama 5 se pierde, el receptor queda esperando completar los 10 mensajes y no envia `CALIBRADO_OK`. El emisor ya envio la rafaga y queda esperando el ACK. Sin timeout ambos procesos pueden quedar bloqueados logicamente. La mitigacion implementada es un timeout de recepcion para detectar la falta de ACK.

Salida esperada resumida:

```text
Comando enviado: CALIBRAR#10
ACK recibido: CALIBRADO_OK
```

## Escenario 5 - Servidor TCP persistente de reportes

Archivos:

- `src/socket/conconexion/stream/AceptadorConexion.java`
- `src/socket/conconexion/stream/SolicitanteConexion.java`
- `src/socket/conconexion/stream/MiSocketStream.java`

Implementacion:

- El servidor se mantiene escuchando con `while (true)`.
- Cada cliente se atiende de forma secuencial y limpia.
- Para cada conexion, el servidor recibe una solicitud y envia un reporte financiero estructurado.
- El socket de datos se cierra despues de atender al cliente, pero el `ServerSocket` sigue vivo.

Explicacion paso a paso:

1. `AceptadorConexion` crea un `ServerSocket` en el puerto indicado.
2. El servidor entra a `while (true)`, por eso no termina despues del primer cliente.
3. Dentro del ciclo llama `accept()`.
4. `accept()` se bloquea hasta que un cliente TCP se conecte.
5. Cuando llega un cliente, `accept()` devuelve un `Socket` de datos exclusivo para esa conexion.
6. Ese `Socket` se envuelve en `MiSocketStream`.
7. El servidor lee la solicitud del cliente.
8. Luego envia un reporte financiero estructurado.
9. Se cierra el socket de datos de ese cliente.
10. El ciclo vuelve arriba y el `ServerSocket` queda listo para otro cliente.

Que significa "secuencial":

El servidor atiende un cliente a la vez. Si un cliente se demora, los nuevos clientes pueden quedar esperando en la cola interna del `ServerSocket`, conocida como backlog. Cuando el servidor termine con el cliente actual y vuelva a `accept()`, tomara la siguiente conexion pendiente.

Por que este cambio cumple el reto:

El codigo original atendia una conexion y terminaba. El requerimiento pedia persistencia de escucha. El `while (true)` mantiene vivo al servidor para que pueda recibir nuevas conexiones sin reiniciarlo.

Comandos:

```powershell
java -cp out socket.conconexion.stream.AceptadorConexion 5000
java -cp out socket.conconexion.stream.SolicitanteConexion localhost 5000
```

Prueba realizada: se ejecutaron dos clientes seguidos contra el mismo servidor. El servidor atendio el primero, volvio a `Preparado para aceptar una conexion`, atendio el segundo y siguio escuchando.

Analisis:

`accept()` bloquea hasta que llega una nueva conexion. Cuando retorna, entrega un socket de datos exclusivo para ese cliente. Como esta version es secuencial, si un cliente se demora, los siguientes quedan esperando en la cola de conexiones del `ServerSocket` hasta que el servidor vuelva a llamar a `accept()`. Si el backlog se llena, nuevas conexiones pueden ser rechazadas o expirar.

Salida esperada resumida:

```text
Servidor de reportes escuchando en puerto 5000
Preparado para aceptar una conexion.
Conexion aceptada.
Solicitud recibida: DESCARGAR_REPORTE_AUDITORIA
Reporte enviado y socket de datos cerrado.
Preparado para aceptar una conexion.
```

## Escenario 6 - Transacciones bancarias TCP stateful

Archivos:

- `src/socket/conconexion/stream/ServidorBanco.java`
- `src/socket/conconexion/stream/ClienteBanco.java`
- `src/socket/conconexion/stream/MiSocketStream.java`

Implementacion:

- El cliente abre una sesion TCP.
- El servidor envia `BIENVENIDO`.
- El cliente envia `TOKEN:<valor>`.
- El servidor valida y responde `AUTH_OK` o `AUTH_RECHAZADO`.
- El cliente envia `RETIRO:<monto>`.
- El servidor responde con `RETIRO_OK|SALDO=<nuevoSaldo>` o `RETIRO_RECHAZADO|SALDO=<saldo>`.

Explicacion paso a paso:

1. `ServidorBanco` abre un `ServerSocket` y espera clientes.
2. `ClienteBanco` se conecta al servidor.
3. El servidor envia `BIENVENIDO`.
4. El cliente manda el token en el formato `TOKEN:TOKEN123`.
5. El servidor revisa si el token existe en su tabla de saldos.
6. Si el token es valido, responde `AUTH_OK`.
7. El cliente manda el retiro en el formato `RETIRO:100.00`.
8. El servidor valida que el monto sea positivo y que haya saldo suficiente.
9. Si todo esta correcto, descuenta el monto y devuelve el nuevo saldo.

Por que este escenario usa TCP:

Una transaccion bancaria tiene varios pasos que dependen del orden. Primero autenticacion, luego solicitud, luego respuesta final. TCP permite mantener una sesion activa y garantiza que los mensajes lleguen en orden.

Comandos:

```powershell
java -cp out socket.conconexion.stream.ServidorBanco 5001
java -cp out socket.conconexion.stream.ClienteBanco localhost 5001 TOKEN123 100.00
```

Prueba realizada: el cliente recibio `BIENVENIDO`, luego `AUTH_OK` y finalmente `RETIRO_OK|SALDO=900.00`.

Analisis de delimitador:

`MiSocketStream.recibeMensaje()` usa `BufferedReader.readLine()`. Ese metodo no termina hasta encontrar un salto de linea o cierre del flujo. Por eso `enviaMensaje()` usa `println()` y luego `flush()`: `println()` agrega el delimitador de linea y `flush()` fuerza la salida del buffer. Si falta cualquiera de los dos, el receptor puede quedar esperando una linea que nunca considera completa.

Explicacion del posible deadlock:

Si el cliente escribe texto pero no manda salto de linea, el servidor queda detenido en `readLine()` esperando el fin de linea. Si ademas el cliente queda esperando una respuesta del servidor, ambos procesos esperan algo del otro. La conexion TCP puede estar activa, pero la aplicacion queda bloqueada por no respetar el protocolo de mensajes por linea.

Salida esperada resumida:

```text
Servidor: BIENVENIDO
Servidor: AUTH_OK
Servidor: RETIRO_OK|SALDO=900.00
```

## Diagnostico incluido en la guia

`UnsupportedClassVersionError`:

Se produce cuando se compila con un JDK moderno y se ejecuta con una JVM antigua. La mitigacion es compilar y ejecutar con el mismo JDK, usando rutas absolutas si el IDE o la consola apuntan a distintas versiones.

`BindException: Address already in use`:

Ocurre cuando otro proceso ya usa el puerto. Se debe cerrar el proceso anterior con `Ctrl + C` o usar otro puerto privado, por ejemplo entre `49152` y `65535`.
