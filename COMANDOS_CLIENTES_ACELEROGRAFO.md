# Comandos para clientes - Acelerografo

Estos comandos son para los companeros que se conectan al servidor central.

## 1. Entrar al proyecto

```powershell
cd "C:\ruta\donde\descargaron\Sistemas-distribuidos"
```

## 2. Compilar

```powershell
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

## 3. Probar conectividad con el servidor

```powershell
ping 192.16.108.23
```

## 4. Cliente TCP

Cambiar `EC-UCON02` por el ID asignado a cada estacion. El ultimo parametro `0` significa que el cliente queda conectado permanentemente y envia una lectura cada 60 segundos.

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23 7500 EC-UCON02 60000 0
```

## 5. Cliente RMI

Cambiar `EC-UCON02` por el ID asignado a cada estacion.

```powershell
java -cp out rmi.acelerografo.EstacionAcelRMI 192.16.108.23 1099 acelerografo EC-UCON02 60000 1
```

## IDs sugeridos

```text
EC-UCON02
EC-UCON03
EC-UCON04
EC-UCON05
EC-UCON06
```

Si aparece `Connection refused`, el servidor no esta levantado o el Firewall de Windows bloqueo los puertos `7500` o `1099`.

## Logs generados

Al ejecutar los clientes se crea automaticamente la carpeta `logs/`.

Cliente TCP:

```text
logs/cliente_tcp_metricas.log
```

Cliente RMI:

```text
logs/cliente_rmi_metricas.log
```

Cada linea guarda fecha, tipo de evento, ID de acelerografo, servidor y tiempo medido.

En el servidor TCP, cada cliente queda identificado por `idAcel` y el log acumula:

- `lecturas`: cantidad de mensajes recibidos por ese cliente.
- `metricasRtt`: cantidad de mediciones de ida y vuelta reportadas por el cliente.
- `rttPromedioMs`: promedio real medido por el cliente desde envio hasta ACK.
- `rttMinMs` y `rttMaxMs`: minimo y maximo por cliente.
- `procesamientoPromedioMs`: tiempo promedio de parseo/procesamiento local del servidor.
