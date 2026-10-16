# Sistemas Distribuidos

Prácticas de la asignatura de Sistemas Distribuidos, en Java SE. El hilo conductor
es una **red simulada de acelerógrafos**: varias estaciones que miden movimiento
sísmico en tres ejes y reportan a un servidor central que concentra las lecturas y
reparte alertas.

Sobre ese caso se implementan las distintas formas de comunicación entre procesos:
sockets sin conexión (UDP), sockets con conexión (TCP) y objetos distribuidos (RMI).

## Qué hay dentro

| Carpeta | Contenido |
|---|---|
| `src/socket/sinconexion/` | Emisor y receptor sobre datagramas UDP, el caso base sin conexión |
| `src/socket/conconexion/` | Servidor TCP concurrente, con un hilo despachador por cliente |
| `src/rmi/eco`, `src/rmi/sum` | Servicios RMI mínimos, para aislar el mecanismo de invocación remota |
| `src/rmi/acelerografo/` | El mismo caso de los acelerógrafos, resuelto con objetos distribuidos |
| `src/telemetria/` | Registro de métricas en disco, compartido por los módulos |
| `entrega_acelerografo/` | Cliente y servidor empaquetados por separado, para correr la práctica entre varias máquinas |

## El caso de los acelerógrafos

Cada estación (`EstacionAcelTCP`) genera lecturas triaxiales y las envía al servidor
(`ServidorAcelTCP`), que atiende a varios clientes a la vez. El servidor lleva la
cuenta por cliente (`EstadisticasCliente`), protege el acceso concurrente al estado
compartido (`AdministradorRecursosSismicos`) y, cuando una lectura supera el umbral,
difunde la alerta al resto (`GestorAlertasSismo`).

El sismo se puede disparar a mano con `ActivadorSismoClienteTCP` o dejar que lo
genere `SimuladorSismoTCP`.

## Escenarios de la guía

Los seis escenarios de la práctica —y los comandos para reproducir cada uno— están
documentados en [`IMPLEMENTACION_REQUERIMIENTOS.md`](IMPLEMENTACION_REQUERIMIENTOS.md):

1. Telemetría ambiental sobre UDP sin conexión
2. Alertas y control de truncamiento de mensajes
3. Filtro de seguridad con UDP conectado
4. Ráfaga de comandos con confirmación (ACK)
5. Servidor TCP persistente de reportes
6. Transacciones bancarias TCP con estado

## Cómo correrlo

Requiere únicamente un JDK (se compila con `--release 8`).

```bash
javac --release 8 -encoding UTF-8 -d out $(find src -name '*.java')
```

En PowerShell:

```powershell
javac --release 8 -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

Luego se levanta el servidor y se conectan las estaciones:

```bash
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP
java -cp out socket.conconexion.acelerografo.EstacionAcelTCP
```

Los módulos escriben sus métricas en `logs/` al ejecutarse.

## Documentación

- [`LEEME_PRIMERO.md`](LEEME_PRIMERO.md) — por dónde empezar
- [`IMPLEMENTACION_REQUERIMIENTOS.md`](IMPLEMENTACION_REQUERIMIENTOS.md) — escenario por escenario
- [`COMANDOS_CLIENTES_ACELEROGRAFO.md`](COMANDOS_CLIENTES_ACELEROGRAFO.md) — conectar estaciones desde otras máquinas
- [`EVENTO5_REMOTO.md`](EVENTO5_REMOTO.md) — correr cliente y servidor en redes distintas
