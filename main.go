// Emitter de Personal — servicio único que procesa TODAS las máquinas y
// publica el estado de cada una en su subject específico:
//   papid.emitter.<machine_code>
//
// Escucha:  papid.admin.signed / unsigned / forced, papid.personal.login / logout
// Publica:  papid.emitter.<machine_code>, papid.personalui.full, papid.personal.denied/cleared
//
// NO tiene UI, ni HTTP, ni SSE.
// Es un servicio ligero que corre en segundo plano.
//
// Uso:  go run .
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"

	"papid-emitter/internal/model"
	"papid-emitter/internal/multistore"
)

// Subjects de NATS.
const (
	// Entrada
	subjAsignar   = "papid.admin.signed"
	subjDesaignar = "papid.admin.unsigned"
	subjLogin     = "papid.personal.login"
	subjLogout    = "papid.personal.logout"
	subjForced    = "papid.admin.signed.forced"

	// Salida
	topicEmitterPrefix = "papid.emitter." // + machine_code (por sub-máquina, para el compañero)
	topicEmitterSync   = "papid.emitter.sync"      // unión + campo maquinas (para el compañero)
	topicDashboard     = "papid.emitter.dashboard" // unión + maquinas, topic global que leen TODOS los dashboards
	topicFull          = "papid.personalui.full"
	topicSync          = "papid.personalui.sync"
	topicDenied        = "papid.personal.denied"
	topicCleared       = "papid.personal.cleared"
)

func main() {
	if err := godotenv.Overload(); err != nil {
		log.Println("[emitter] No se encontró .env, se usan variables del entorno")
	}

	// MACHINE_CODE: el código de ESTA máquina física (lo que publica en
	//               papid.emitter.dashboard como machine_code).
	// DEF_MAQUINAS (opcional): lista EXACTA de sub-máquinas. Si se define, solo
	//               se aceptan esos codes y el mismo mensaje se publica solo a
	//               esos. Si NO se define, se acepta por PREFIJO: el propio
	//               MACHINE_CODE y cualquier "MACHINE_CODE-*".
	//
	// Ejemplos .env:
	//   MACHINE_CODE=a2i                       -> acepta a2i, a2i-1-r, a2i-2-r, ...
	//   MACHINE_CODE=a2i  DEF_MAQUINAS=a2i-1-r,a2i-2-r -> solo esas dos
	//   MACHINE_CODE=a2i  DEF_MAQUINAS=a2i-1-r         -> máquina individual
	machineCode := os.Getenv("MACHINE_CODE")
	defMaquinas := leerDefMaquinas()

	log.Printf("[emitter] MACHINE_CODE: %s", machineCode)
	if len(defMaquinas) > 0 {
		log.Printf("[emitter] DEF_MAQUINAS (lista exacta): %v", defMaquinas)
	} else {
		log.Printf("[emitter] Modo prefijo: acepta %q y %q-*", machineCode, machineCode)
	}

	// pertenece decide si un machine_code entrante es de esta máquina.
	pertenece := func(code string) bool {
		if len(defMaquinas) > 0 {
			for _, c := range defMaquinas {
				if c == code {
					return true
				}
			}
			return false
		}
		// Modo prefijo: el propio code o cualquier sub-máquina "machineCode-...".
		return code == machineCode || strings.HasPrefix(code, machineCode+"-")
	}

	// Sedes vistas: sub-máquinas de las que hemos recibido asignaciones.
	// Se usan para el fan-out por sub-máquina cuando no hay DEF_MAQUINAS.
	// maxSedes acota el mapa: sin límite, un machine_code mal formado lo haría
	// crecer para siempre y multiplicaría los publishes en cada evento.
	const maxSedes = 64
	var sedesMu sync.Mutex
	sedesVistas := make(map[string]bool)
	sedesPorRetirar := make(map[string]bool) // desasignadas: se quitan tras publicar

	recordarSede := func(code string) {
		sedesMu.Lock()
		defer sedesMu.Unlock()
		delete(sedesPorRetirar, code) // volvió a usarse
		if sedesVistas[code] {
			return
		}
		if len(sedesVistas) >= maxSedes {
			log.Printf("[emitter] Límite de %d sub-máquinas alcanzado, se ignora %q", maxSedes, code)
			return
		}
		sedesVistas[code] = true
	}

	// marcarSedeRetirada agenda el borrado de una sub-máquina desasignada.
	// Se borra DESPUÉS de publicar, para que reciba el último estado (vacío).
	marcarSedeRetirada := func(code string) {
		sedesMu.Lock()
		if sedesVistas[code] {
			sedesPorRetirar[code] = true
		}
		sedesMu.Unlock()
	}

	// destinosFanout devuelve los codes a los que se publica papid.emitter.<code>.
	destinosFanout := func() []string {
		if len(defMaquinas) > 0 {
			return defMaquinas
		}
		sedesMu.Lock()
		defer sedesMu.Unlock()
		out := make([]string, 0, len(sedesVistas))
		for c := range sedesVistas {
			out = append(out, c)
		}
		sort.Strings(out)
		return out
	}

	// purgarSedesRetiradas quita del fan-out las sub-máquinas desasignadas
	// (ya recibieron su último mensaje con el estado vacío).
	purgarSedesRetiradas := func() {
		sedesMu.Lock()
		for c := range sedesPorRetirar {
			delete(sedesVistas, c)
			delete(sedesPorRetirar, c)
			log.Printf("[emitter] Sub-máquina %q retirada del fan-out", c)
		}
		sedesMu.Unlock()
	}

	// Conexión a NATS.
	nc := conectar()
	defer nc.Drain()

	// Store de la máquina física (una sola, la del emitter).
	ms := multistore.New(machineCode)

	// Ventana para agrupar ráfagas de eventos en una sola publicación.
	const debounceDelay = 200 * time.Millisecond //AQUI ESTA EL DELAY, pueden ser Millisecond o Second

	// pub publica y registra el error (antes se descartaba: si el buffer de
	// reconexión se desbordaba, se perdía estado en silencio).
	pub := func(subject string, v any) {
		data, err := json.Marshal(v)
		if err != nil {
			log.Printf("[emitter] Error serializando para %s: %v", subject, err)
			return
		}
		if err := nc.Publish(subject, data); err != nil {
			log.Printf("[emitter] Error publicando en %s: %v", subject, err)
		}
	}

	publicar := func() {
		// Unión del personal (sin el campo maquinas).
		base := ms.BuildSync()

		// papid.emitter.<sub-máquina> para cada sede conocida (compañero).
		// Cada mensaje lleva su propio machine_code, con el mismo personal.
		sedes := destinosFanout()
		for _, gc := range sedes {
			base.MachineCode = gc
			pub(topicEmitterPrefix+gc, base)
		}

		// papid.personalui.sync / full (node-red) con machine_code = MACHINE_CODE.
		base.MachineCode = machineCode
		pub(topicSync, base)
		pub(topicFull, ms.BuildFull())

		// papid.emitter.sync (compañero): objeto único con unión + maquinas.
		esync := ms.BuildEmitterSync()
		pub(topicEmitterSync, esync)

		// papid.emitter.dashboard (dashboards): mismo objeto con machine_code =
		// MACHINE_CODE. Si hay DEF_MAQUINAS, se adjunta la lista para que los
		// dashboards filtren por code EXACTO; si no, filtran por prefijo.
		dash := esync
		if len(defMaquinas) > 0 {
			dash.DefMaquinas = defMaquinas
		}
		pub(topicDashboard, dash)

		// Las sub-máquinas desasignadas ya recibieron su estado final: fuera.
		purgarSedesRetiradas()

		log.Printf("[emitter] Publicado (sync, full, %d sub-máquinas, %s y %s)", len(sedes), topicEmitterSync, topicDashboard)
	}

	// Señal de "hay cambios por publicar". Capacidad 1 = las ráfagas se
	// colapsan en una sola señal.
	cambios := make(chan struct{}, 1)
	ms.SetOnChange(func() {
		select {
		case cambios <- struct{}{}:
		default: // ya hay una publicación pendiente, se agrupa con esta
		}
	})

	// Worker ÚNICO: garantiza que nunca haya dos publicar() en paralelo (con
	// timers concurrentes dos snapshots podían publicarse en orden invertido y
	// dejar a los dashboards con el estado viejo).
	go func() {
		for range cambios {
			time.Sleep(debounceDelay) // agrupa la ráfaga
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[emitter] Panic al publicar (recuperado): %v", r)
					}
				}()
				publicar()
			}()
		}
	}()

	// parseAsignacion decodifica un mensaje de asignación y verifica que sea
	// de MI GRUPO. Conserva el machine_code original (la sub-máquina) para
	// poder fusionar y saber en qué sub-máquinas está cada persona.
	parseAsignacion := func(data []byte) (model.MensajeAsignacion, bool) {
		var msg model.MensajeAsignacion
		if json.Unmarshal(data, &msg) != nil || msg.MachineCode == "" || !pertenece(msg.MachineCode) {
			return msg, false
		}
		recordarSede(msg.MachineCode) // recordar esta sub-máquina para el fan-out
		return msg, true
	}

	// leerEvento decodifica un evento de login/logout del lector.
	leerEvento := func(data []byte) (model.EventoRFID, bool) {
		var ev model.EventoRFID
		if json.Unmarshal(data, &ev) != nil {
			return ev, false
		}
		return ev, true
	}

	// publicarEvento publica un aviso (denied/cleared) con el puerto y tag.
	publicarEvento := func(subject string, ev model.EventoRFID) {
		data := fmt.Sprintf(`{"usb_port":"%s","tag":"%s"}`, ev.UsbPort, ev.Tag)
		nc.Publish(subject, []byte(data))
		log.Printf("[emitter] Publicado en %s: %s", subject, data)
	}

	// suscribir registra un handler y aborta el arranque si falla (antes el
	// error se ignoraba y el emitter quedaba ciego a ese subject en silencio).
	suscribir := func(subject string, h nats.MsgHandler) {
		if _, err := nc.Subscribe(subject, h); err != nil {
			log.Fatalf("[emitter] No se pudo suscribir a %s: %v", subject, err)
		}
		log.Printf("[emitter] Suscrito a %s", subject)
	}

	// --- Suscripciones (solo procesa mensajes de MI GRUPO) ---
	// El machine_code del mensaje es la sub-máquina (a2i-1-r, a2i-2-r, ...).
	suscribir(subjAsignar, func(m *nats.Msg) {
		if msg, ok := parseAsignacion(m.Data); ok {
			ms.Asignar(msg.MachineCode, msg)
		}
	})

	suscribir(subjForced, func(m *nats.Msg) {
		if msg, ok := parseAsignacion(m.Data); ok {
			ms.AsignarForzado(msg.MachineCode, msg)
		}
	})

	suscribir(subjDesaignar, func(m *nats.Msg) {
		if msg, ok := parseAsignacion(m.Data); ok {
			ms.Desasignar(msg.MachineCode) // quita esa sub-máquina de todos
			marcarSedeRetirada(msg.MachineCode)
		}
	})

	suscribir(subjLogin, func(m *nats.Msg) {
		if ev, ok := leerEvento(m.Data); ok && !ms.RegistrarPorID(ev.Tag, ev.UsbPort) {
			publicarEvento(topicDenied, ev)
		}
	})

	suscribir(subjLogout, func(m *nats.Msg) {
		if ev, ok := leerEvento(m.Data); ok && !ms.RetirarPorID(ev.Tag) {
			publicarEvento(topicCleared, ev)
		}
	})

	log.Println("[emitter] Emitter MULTI-MÁQUINA corriendo")
	log.Println("[emitter] Publica en: papid.emitter.<machine>, papid.personalui.sync/full, papid.personal.denied/cleared")
	log.Println("[emitter] Presiona Ctrl+C para detener")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[emitter] Apagando...")

	// Publica el último estado pendiente y espera a que salga de verdad.
	select {
	case <-cambios:
		publicar()
	default:
	}
	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		log.Printf("[emitter] Flush incompleto al apagar: %v", err)
	}
}

func conectar() *nats.Conn {
	opts := []nats.Option{
		nats.Name("papid-emitter-multi"),
		// Servicio 24/7: reintentar INDEFINIDAMENTE. El emitter es la única
		// fuente de verdad de los dashboards; con el default (60 intentos) una
		// caída del broker lo dejaba como proceso zombi (vivo pero mudo).
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2 * time.Second),
		nats.ReconnectJitter(100*time.Millisecond, time.Second),
		nats.Timeout(10 * time.Second),
		nats.RetryOnFailedConnect(true),
		// Buffer amplio para no perder publicaciones mientras reconecta.
		nats.ReconnectBufSize(16 * 1024 * 1024),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("[emitter] NATS desconectado: %v (reintentando...)", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("[emitter] NATS reconectado a %s", c.ConnectedUrl())
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			log.Println("[emitter] NATS: conexión cerrada definitivamente")
		}),
		nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, err error) {
			subject := ""
			if s != nil {
				subject = s.Subject
			}
			log.Printf("[emitter] NATS error (subject %q): %v", subject, err)
		}),
	}
	if u := os.Getenv("NATS_USER"); u != "" {
		opts = append(opts, nats.UserInfo(u, os.Getenv("NATS_PASS")))
	}
	nc, err := nats.Connect(getenv("NATS_URL", "nats://localhost:4222"), opts...)
	if err != nil {
		log.Fatalf("[emitter] No se pudo conectar a NATS: %v", err)
	}
	log.Printf("[emitter] Conectado a NATS en %s", nc.ConnectedUrl())
	return nc
}

func getenv(clave, def string) string {
	if v := os.Getenv(clave); v != "" {
		return v
	}
	return def
}

// leerDefMaquinas lee la variable DEF_MAQUINAS del .env (opcional) y devuelve
// la lista de machine_codes. Si no está definida, devuelve nil (modo prefijo).
// Formato del .env: DEF_MAQUINAS=a2i-1-r,a2i-2-r,a2i-3-r
func leerDefMaquinas() []string {
	raw := os.Getenv("DEF_MAQUINAS")
	if raw == "" {
		return nil
	}
	partes := strings.Split(raw, ",")
	grupo := make([]string, 0, len(partes))
	for _, p := range partes {
		p = strings.TrimSpace(p)
		if p != "" {
			grupo = append(grupo, p)
		}
	}
	return grupo
}
