# Evento 5 - servidor TCP desde casas distintas

Este escenario usa TCP. Tu computadora actua como servidor y tu companero ejecuta el cliente.

## Compilar

Desde esta carpeta del proyecto:

```powershell
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

## Prueba local primero

Terminal 1, en tu PC:

```powershell
java -cp out socket.conconexion.stream.AceptadorConexion 5000
```

Terminal 2, en tu PC:

```powershell
java -cp out socket.conconexion.stream.SolicitanteConexion localhost 5000
```

Si aparece el reporte, el codigo del evento 5 esta funcionando.

## Para que tu companero se conecte desde su casa

El cliente no debe usar `localhost`, porque `localhost` siempre significa "mi propia computadora". Tu companero debe usar una direccion que llegue a tu PC.

Opcion A, redireccion de puerto en tu router:

1. Ejecuta el servidor en tu PC:

```powershell
java -cp out socket.conconexion.stream.AceptadorConexion 5000
```

2. En Windows, permite Java en el firewall o crea una regla de entrada para TCP puerto `5000`.
3. En tu router, redirige TCP `5000` hacia la IP local de tu PC, por ejemplo `192.168.1.25:5000`.
4. Comparte con tu companero tu IP publica y el puerto `5000`.
5. Tu companero ejecuta:

```powershell
java -cp out socket.conconexion.stream.SolicitanteConexion TU_IP_PUBLICA 5000
```

Opcion B, VPN privada si no puedes abrir puertos:

Usen una VPN tipo Tailscale, ZeroTier o Radmin VPN. Cuando ambos esten en la misma red virtual, tu companero usa la IP virtual de tu PC:

```powershell
java -cp out socket.conconexion.stream.SolicitanteConexion TU_IP_VIRTUAL 5000
```

Esta opcion suele evitar problemas de CGNAT, routers bloqueados o proveedores que no permiten exponer puertos.

## Senales de que algo falla fuera del codigo

- `Connection refused`: la IP responde, pero no hay servidor escuchando en ese puerto o el firewall lo bloquea.
- `Connection timed out`: la conexion no llega a tu PC; suele ser router, NAT, IP incorrecta o puerto no abierto.
- En el servidor no aparece `Conexion aceptada`: el problema esta antes de Java, normalmente red/firewall/router.
- En el servidor aparece `Conexion aceptada` pero el cliente no recibe reporte: revisa que el cliente envie la solicitud con salto de linea. Este proyecto usa `println()` y `flush()` para eso.
