# papid-emitter

Servicio que procesa los eventos de personal (asignaciones y lecturas RFID)
y publica el estado actualizado de cada máquina en NATS.

Es el **único publicador** del estado del personal. Los dashboards y Node-RED
solo leen lo que este servicio publica. No tiene UI ni servidor web — corre
en segundo plano como un proceso ligero.

---

## Qué hace

```
Escucha de NATS:
  papid.admin.signed          → asignación de personal a una máquina
  papid.admin.signed.forced   → asignación forzada (todos en verde)
  papid.admin.unsigned        → desasignación (vacía la máquina)
  papid.personal.login        → tarjeta RFID puesta en el sensor
  papid.personal.logout       → tarjeta RFID retirada del sensor

Publica en NATS:
  papid.emitter.<machine>     → estado específico de una máquina (lo escucha su dashboard)
  papid.personalui.sync       → estado genérico (lo escucha Node-RED y el monitor)
  papid.personalui.full       → estado completo con is_active/is_frozen
  papid.personal.denied       → alguien intentó registrarse y NO estaba asignado
  papid.personal.cleared      → logout de alguien que no estaba activo
```

---

## Cómo funciona

1. Escucha **todos** los eventos de personal de **todas** las máquinas (no filtra).
2. Mantiene un store en memoria por máquina (sabe quién está asignado, quién tiene tarjeta puesta, etc.).
3. Cuando algo cambia en una máquina, espera **1 segundo** (debounce) por si llegan más cambios en ráfaga.
4. Pasado el segundo, publica el estado final en el subject de esa máquina.

El debounce evita que los lectores físicos se saturen cuando llegan varios
eventos rápidos (ej. 3 tarjetas puestas casi al mismo tiempo).

---

## Configuración (.env)

```dotenv
# Credenciales del servidor NATS
NATS_URL=nats://192.168.90.156:4222
NATS_USER=papid
NATS_PASS=papid2024
```

No necesita `MACHINE_CODE` porque procesa todas las máquinas.

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

### Compilar un binario para producción

```bash
go build -o emitter .
./emitter
```

### Detener

`Ctrl+C` (el proceso se apaga limpiamente).

---

## Ajustar el debounce

En `main.go`, la línea:

```go
const debounceDelay = 1 * time.Second
```

Cambia el valor según lo que necesites:
- `500 * time.Millisecond` → medio segundo (más reactivo)
- `2 * time.Second` → 2 segundos (más conservador para ráfagas largas)

---

## Estructura

```
papid-emitter/
├── main.go                      # Programa principal
├── .env                         # Configuración NATS
├── go.mod                       # Dependencias
└── internal/
    ├── model/model.go           # Estructuras de datos
    └── multistore/multistore.go # Store multi-máquina (estado de todas)
```

---

## Relación con los demás servicios

```
                    ┌─────────────────┐
                    │  papid-emitter  │  (este servicio)
                    └────────┬────────┘
                             │ publica
                ┌────────────┼────────────┐
                ▼            ▼            ▼
  papid.emitter.a2i-1-r   sync/full   denied/cleared
        │                    │              │
        ▼                    ▼              ▼
  Dashboard a2i-1-r    Node-RED       Node-RED
  (solo pinta)         (colores)      (alerta lector)
```

El emitter es la **fuente única de verdad**. Si no corre, los dashboards no
se actualizan y Node-RED no recibe cambios de estado.
