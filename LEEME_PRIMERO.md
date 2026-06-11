# Guia rapida - Proyecto sistemas distribuidos

Esta carpeta ya quedo ordenada para trabajar como proyecto JavaSE y probar desde terminal.

## Que abrir primero

1. `IMPLEMENTACION_REQUERIMIENTOS.md`
   - Explica cada escenario del PDF.
   - Incluye comandos de compilacion y ejecucion.

2. `COMANDOS_CLIENTES_ACELEROGRAFO.md`
   - Comandos listos para que los companeros se conecten al servidor central.

3. `src/`
   - Codigo fuente final y ordenado segun la guia.
   - Incluye ejemplos de sockets y RMI.

4. `EVENTO5_REMOTO.md`
   - Pasos para ejecutar el evento 5 cuando el servidor y el cliente estan en casas distintas.

5. `out/`
   - Archivos `.class` generados al compilar.

6. `respaldo_original/`
   - Archivos originales, ZIP/RAR, PDF de apoyo y copias extraidas.
   - No es necesario abrirlo para probar la entrega, solo queda como respaldo.

## Compilar

Desde PowerShell, ubicado en esta carpeta:

```powershell
javac --release 8 -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

## Logs de metricas

Los modulos de acelerografo crean automaticamente la carpeta `logs/`.

Servidor TCP: `logs/servidor_tcp_metricas.log`
Resumen TCP: `logs/servidor_tcp_resumen.log`
Cliente TCP: `logs/cliente_tcp_metricas.log`
Concurrencia TCP: `logs/concurrencia_sismica.log`
Servidor RMI: `logs/servidor_rmi_metricas.log`
Cliente RMI: `logs/cliente_rmi_metricas.log`

El servidor TCP tambien escribe la bitacora protegida por exclusion mutua en `bitacora_sismica.txt`.

Para dejar un cliente TCP conectado permanentemente, use `0` como cantidad de envios:

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23 7500 EC-UCON02 60000 0
```

## Probar rapido RMI Eco

Terminal 1:

```powershell
java -cp out rmi.eco.ServidorEco 1099 eco
```

Terminal 2:

```powershell
java -cp out rmi.eco.ClienteEco localhost 1099 eco "Hola!" "Adios!"
```

## Probar rapido RMI Sum

Terminal 1:

```powershell
java -cp out rmi.sum.ServidorSum 50000 sum
```

Terminal 2:

```powershell
java -cp out rmi.sum.ClienteSum localhost 50000 sum 8 12
```

## Levantar servidores de acelerografo

Servidor TCP concurrente, puerto 7500:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500
```

Prueba de semaforo con 2 clientes y solo 1 permiso:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1
```

Servidor RMI, puerto 1099:

```powershell
java -cp out rmi.acelerografo.ServidorAcelRMI 1099 acelerografo 192.16.108.23
```

En laboratorio, informe a los clientes la IP LAN que muestra el servidor RMI. Si necesita forzar una IP especifica:

```powershell
java -cp out rmi.acelerografo.ServidorAcelRMI 1099 acelerografo 192.168.X.X
```

## Probar rapido el escenario 5

Terminal 1:

```powershell
java -cp out socket.conconexion.stream.AceptadorConexion 5000
```

Terminal 2:

```powershell
java -cp out socket.conconexion.stream.SolicitanteConexion localhost 5000
```

Ejecute el cliente varias veces. El servidor debe seguir activo despues de atender cada conexion.

Para detener el servidor use `Ctrl + C`.
