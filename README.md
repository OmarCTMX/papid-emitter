# papid-emitter

Servicio que procesa los eventos de personal (asignaciones y lecturas RFID)
y publica el estado actualizado en NATS.

Es el **único publicador** del estado del personal. Los dashboards y Node-RED
solo leen lo que este servicio publica. No tiene UI ni servidor web — corre
en segundo plano como un proceso ligero.

Un emitter atiende **una máquina física** (por ejemplo `a2i`), que puede tener
varias **sub-máquinas** que comparten el mismo personal (`a2i-1-r`, `a2i-2-r`,
`a2i-3-r`). Para otra máquina física (`envasadora-1`) se corre otro emitter.

---

## Qué hace

```
Escucha de NATS:
  papid.admin.signed          → asignación de personal a una (sub)máquina
  papid.admin.signed.forced   → asignación forzada (todos en verde)
  papid.admin.unsigned        → desasignación (quita esa sub-máquina)
  papid.personal.login        → tarjeta RFID puesta en el lector
  papid.personal.logout       → tarjeta RFID retirada del lector

Publica en NATS:
  papid.emitter.dashboard     → estado de la máquina; lo leen TODOS los dashboards
  papid.emitter.sync          → igual que arriba, con el campo "maquinas"
  papid.emitter.<submaquina>  → una copia por sub-máquina conocida
  papid.personalui.sync       → estado genérico (Node-RED / monitor)
  papid.personalui.full       → estado completo con is_active / is_frozen
  papid.personal.denied       → alguien pasó su tarjeta y NO estaba asignado
  papid.personal.cleared      → logout de alguien que no estaba activo
```

---

## Cómo funciona

1. **Filtra** los eventos que le corresponden según su `MACHINE_CODE`
   (ver [Configuración](#configuración-env)). Lo que no es suyo lo ignora.
2. **Fusiona** el personal en un solo estado. Si llega un `signed` de
   `a2i-2-r`, no borra lo que vino de `a2i-1-r`: agrega la sub-máquina al
   campo `maquinas` de cada persona. Una persona existe **una sola vez** y
   tiene **un solo estado**, aunque esté asignada a varias sub-máquinas.
3. En el login/logout busca la persona por su `tag`. Como el estado es único,
   al poner su tarjeta se pone verde en **todas** sus sub-máquinas a la vez.
4. Cuando algo cambia, espera **200 ms** (debounce) por si llegan más eventos
   en ráfaga, y luego publica el estado final una sola vez.

El debounce evita que los lectores físicos se saturen cuando llegan varios
eventos seguidos (ej. tres tarjetas puestas casi al mismo tiempo).

### Estados de una persona

| Estado      | Significado                                          |
|-------------|------------------------------------------------------|
| `Asignado`  | Está asignada pero nunca ha puesto su tarjeta        |
| `Activo`    | Tiene la tarjeta puesta ahora (verde)                |
| `Inactivo`  | Ya se registró antes, pero retiró la tarjeta         |

---

## Configuración (.env)

```dotenv
# Máquina física de este emitter. Es el machine_code que publica y define
# qué eventos acepta.
MACHINE_CODE=a2i

# DEF_MAQUINAS (opcional): lista EXACTA de sub-máquinas a aceptar.
# Déjalo comentado para usar el modo prefijo (recomendado).
#DEF_MAQUINAS=a2i-1-r,a2i-2-r

# Credenciales del servidor NATS
NATS_URL=nats://192.168.90.156:4222
NATS_USER=papid
NATS_PASS=papid2024
```

`MACHINE_CODE` es **obligatorio**. Sin él el emitter no sabe qué aceptar ni
con qué nombre publicar.

### Los dos modos de filtrado

**Modo prefijo** (sin `DEF_MAQUINAS`) — el recomendado:

```dotenv
MACHINE_CODE=a2i
```

Acepta `a2i` y **cualquier** sub-máquina que empiece con `a2i-`
(`a2i-1-r`, `a2i-2-r`, `a2i-3-r`, ...) sin tener que enumerarlas. Las
descubre solas conforme llegan los `signed`.

El guion importa: `a2i2-r` **no** se acepta, porque no empieza con `a2i-`.
Así `envasadora-1` nunca se confunde con `envasadora-10`.

**Modo lista exacta** (con `DEF_MAQUINAS`) — para casos específicos:

```dotenv
MACHINE_CODE=a2i
DEF_MAQUINAS=a2i-1-r,a2i-2-r     # solo estas dos comparten personal
#DEF_MAQUINAS=a2i-1-r            # una sola máquina, aislada
```

Solo acepta esos codes exactos e ignora el resto. Además, la lista viaja en
el mensaje (campo `def_maquinas`) para que los dashboards que no estén en
ella **no** pinten nada.

### Máquina sin sub-máquinas

```dotenv
MACHINE_CODE=envasadora-1
```

Funciona igual, solo que nunca aparecerán sub-máquinas.

---

## Cómo lo leen los dashboards

Todos los dashboards escuchan el **mismo** topic, `papid.emitter.dashboard`,
y cada uno se queda con lo que le toca comparando su propio `MACHINE_CODE`:

```
Emitter (MACHINE_CODE=a2i)
   │
   └── papid.emitter.dashboard   {"machine_code":"a2i", ...}
              │
              ├──► Dashboard a2i-1-r   (a2i-1-r pertenece a a2i → pinta)
              ├──► Dashboard a2i-2-r   (a2i-2-r pertenece a a2i → pinta)
              └──► Dashboard envasadora-1  (no pertenece → ignora)
```

Por eso agregar un dashboard nuevo **no requiere tocar nada**: se suscribe,
ve que su code pertenece a la máquina y empieza a pintar.

### Ejemplo de mensaje

```json
{
  "machine_code": "a2i",
  "personnel": [
    {"employee_id":"V000","nombre":"Omar Ramirez","rol":"Revolturero",
     "status":"Asignado","tipo_asignacion_id":1,"maquinas":["a2i-1-r"]},
    {"employee_id":"V0912","nombre":"Michel Davalos","rol":"Revolturero",
     "status":"Activo","tipo_asignacion_id":1,"usb_port":"3-3",
     "maquinas":["a2i-1-r","a2i-2-r"]}
  ]
}
```

El campo `maquinas` indica en qué sub-máquinas está asignada cada persona.
Solo viaja en `papid.emitter.dashboard` y `papid.emitter.sync`; los demás
topics conservan su formato original.

> **Nota sobre `usb_port`:** se llena en el login y **no** se borra en el
> logout. Al leerlo, tómalo en cuenta solo cuando el `status` sea `Activo`;
> si no, apuntarás a un lector que ya está libre.

---

## Cómo correrlo

### Requisitos

- Go 1.26 o superior
- Acceso al servidor NATS

### Primera vez (descargar dependencias)

```bash
cd papid-emitter
go mod tidy
```

### Correr en modo desarrollo

```bash
go run .
```

Importante: córrelo **desde la carpeta** `papid-emitter`, porque el `.env` se
lee del directorio actual.

Al arrancar imprime la configuración que tomó, útil para verificar:

```
[emitter] MACHINE_CODE: a2i
[emitter] Modo prefijo: acepta "a2i" y "a2i"-*
[emitter] Conectado a NATS en nats://192.168.90.156:4222
[emitter] Suscrito a papid.admin.signed
...
```

### Compilar un binario para producción

```bash
go build -o emitter .
./emitter
```

### Detener

`Ctrl+C`. Antes de salir publica el último estado pendiente y espera a que
los mensajes salgan de verdad (flush).

---

## Probar sin lector físico

Con el CLI de [NATS](https://github.com/nats-io/natscli):

```bash
# Ver lo que publica el emitter
nats sub papid.emitter.dashboard -s nats://192.168.90.156:4222 --user papid --password papid2024

# Asignar personal a a2i-1-r
echo '{"machine_id":"4","personnel":[
  {"employee_id":"V000","name":"Omar Ramirez","tipo_asignacion_id":1,"tipo_asignacion":"Revolturero","nfc_tag":"0B:59:22:1A"},
  {"employee_id":"V0912","name":"Michel Davalos","tipo_asignacion_id":1,"tipo_asignacion":"Revolturero","nfc_tag":"0D:9B:BE:3C"}
],"is_frozen":true,"machine_code":"a2i-1-r"}' | \
  nats pub papid.admin.signed -s nats://192.168.90.156:4222 --user papid --password papid2024

# Poner la tarjeta de Omar en el lector 3-1
echo '{"tag":"0B:59:22:1A","usb_port":"3-1"}' | \
  nats pub papid.personal.login -s nats://192.168.90.156:4222 --user papid --password papid2024

# Retirarla
echo '{"tag":"0B:59:22:1A","usb_port":"3-1"}' | \
  nats pub papid.personal.logout -s nats://192.168.90.156:4222 --user papid --password papid2024

# Desasignar la sub-máquina
echo '{"machine_code":"a2i-1-r"}' | \
  nats pub papid.admin.unsigned -s nats://192.168.90.156:4222 --user papid --password papid2024
```

El lector manda el identificador en el campo **`tag`** (no `id` ni `rfc_tag`).

---

## Ajustes

### Debounce

En `main.go`:

```go
const debounceDelay = 200 * time.Millisecond
```

- `200 * time.Millisecond` → reactivo (valor actual)
- `1 * time.Second` → más conservador para ráfagas largas

### Tope de sub-máquinas

En `main.go`:

```go
const maxSedes = 64
```

Limita cuántas sub-máquinas distintas se recuerdan. Evita que un
`machine_code` mal formado haga crecer la memoria sin control en un servicio
que corre semanas. Las sub-máquinas desasignadas se purgan solas después de
recibir su último estado.

---

## Notas de operación

Está pensado para correr indefinidamente:

- **Reconexión infinita a NATS.** Si el broker se cae, reintenta para siempre
  (con los valores por defecto de la librería se habría rendido a los ~2
  minutos y el proceso habría quedado vivo pero mudo). Las desconexiones y
  reconexiones se registran en el log.
- **Un solo worker de publicación**, así nunca se publican dos estados en
  paralelo ni llegan en orden invertido.
- **Errores visibles.** Si una suscripción falla, el proceso aborta con un
  mensaje claro en vez de arrancar ciego a ese subject. Los errores al
  publicar se registran.

---

## Estructura

```
papid-emitter/
├── main.go                      # Programa principal: config, NATS, publicación
├── .env                         # Configuración (MACHINE_CODE, NATS)
├── go.mod                       # Dependencias
└── internal/
    ├── model/model.go           # Estructuras de los mensajes
    └── multistore/multistore.go # Estado del personal (fusión + login/logout)
```

---

## Relación con los demás servicios

```
        papid.admin.signed / login / logout / unsigned
                          │
                          ▼
                 ┌─────────────────┐
                 │  papid-emitter  │   (este servicio)
                 └────────┬────────┘
                          │ publica
        ┌─────────────────┼──────────────────┐
        ▼                 ▼                  ▼
papid.emitter.dashboard   papid.personalui.*   papid.personal.denied
papid.emitter.sync        (Node-RED: colores   /cleared
papid.emitter.<submaq>     y cambio pantalla)  (Node-RED: alerta)
        │
        ▼
  Dashboards (solo pintan, no publican nada)
```

El emitter es la **fuente única de verdad**. Si no corre, los dashboards no
se actualizan y Node-RED no recibe cambios de estado.
