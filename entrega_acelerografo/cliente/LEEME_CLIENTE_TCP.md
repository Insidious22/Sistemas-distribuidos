# Cliente TCP de acelerografo

## Compilar

```powershell
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

## Ejecutar rafaga

Cambie `192.16.108.23` por la IP del servidor y `EC-UCON02` por el ID asignado.

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23 7500 EC-UCON02 1000 0 60
```

## Ejecutar permanente

Para que no se cierre solo:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23 7500 EC-UCON02 1000 0 0
```

Tambien puede ejecutarse sin argumentos; pedira IP, puerto, ID e intervalo, y quedara permanente hasta `Ctrl + C`:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP
```

Parametros:

```text
<ipServidor> <puerto> <idAcel> <intervaloMs> <repeticiones> <duracionSegundos>
```

Ejemplo:

```text
1000 0 60
```

Significa 1 mensaje cada segundo, sin limite por cantidad fija, durante 60 segundos.

Si `repeticiones` es distinto de `0`, el cliente se cierra al completar esa cantidad de mensajes.

El cliente registra metricas en:

```text
logs/cliente_tcp_metricas.log
```
