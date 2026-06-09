# Entrega TCP - Acelerografo

Esta carpeta separa el proyecto TCP en dos partes:

```text
servidor/   codigo para la maquina central
cliente/    codigo para las estaciones de acelerografo
```

Cada parte se compila por separado desde su propia carpeta.

## Servidor

```powershell
cd "C:\Users\Programación\Documents\Sistemas-distribuidos\entrega_acelerografo\servidor"
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500
```

Para demostrar cola de semaforo con solo 2 clientes:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1
```

## Cliente

Cambie `192.16.108.23` por la IP real del servidor y `EC-UCON02` por el ID de la estacion.

```powershell
cd "C:\Users\Programación\Documents\Sistemas-distribuidos\entrega_acelerografo\cliente"
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP 192.16.108.23 7500 EC-UCON02 1000 0 60
```

El ejemplo envia una rafaga de 1 mensaje cada segundo durante 60 segundos.
