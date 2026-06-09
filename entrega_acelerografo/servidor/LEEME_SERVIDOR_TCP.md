# Servidor TCP de acelerografos

## Compilar

```powershell
javac -encoding UTF-8 -d out (Get-ChildItem -Recurse src -Filter *.java).FullName
```

## Levantar servidor

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500
```

Para probar exclusion con 2 clientes donde solo uno entra y el otro espera, levante el servidor con 1 permiso:

```powershell
java -cp out socket.conconexion.acelerografo.ServidorAcelTCP 7500 1
```

El servidor acepta varios clientes permanentes al mismo tiempo y registra metricas en:

```text
logs/servidor_tcp_metricas.log
logs/servidor_tcp_resumen.log
logs/concurrencia_sismica.log
```

Use `servidor_tcp_resumen.log` para leer rapidamente los promedios por cliente.

La bitacora unificada protegida por exclusion mutua queda en:

```text
bitacora_sismica.txt
```

Para detenerlo use `Ctrl + C`.
