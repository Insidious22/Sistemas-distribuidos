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

Cambiar `EC-UCON02` por el ID asignado a cada estacion.

```powershell
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23 7500 EC-UCON02 60000 1
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
