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
	topicEmitterPrefix = "papid.emitter." // + machine_code
	topicFull          = "papid.personalui.full"
	topicSync          = "papid.personalui.sync"
	topicDenied        = "papid.personal.denied"
	topicCleared       = "papid.personal.cleared"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("[emitter] No se encontró .env, se usan variables del entorno")
	}

	// Conexión a NATS.
	nc := conectar()
	defer nc.Drain()

	// Store multi-máquina.
	ms := multistore.New()

	// Evita publicar múltiples veces en ráfaga cuando llegan varios eventos
	// de la misma máquina
	const debounceDelay = 200 * time.Millisecond //AQUI ESTA EL DELAY, pueden ser Millisecond o Second
	var debounceMu sync.Mutex
	debounceTimers := make(map[string]*time.Timer)

	publicarParaMaquina := func(machineCode string) {
		sync := ms.BuildSync(machineCode)
		data, _ := json.Marshal(sync)
		subject := topicEmitterPrefix + machineCode
		nc.Publish(subject, data)
		log.Printf("[emitter] Publicado en %s", subject)

		nc.Publish(topicSync, data)
		fullData, _ := json.Marshal(ms.BuildFull(machineCode))
		nc.Publish(topicFull, fullData)
	}

	// Cuando cambia una máquina, programamos publicación con debounce.
	ms.SetOnChange(func(machineCode string) {
		debounceMu.Lock()
		if timer, ok := debounceTimers[machineCode]; ok {
			timer.Stop()
		}
		debounceTimers[machineCode] = time.AfterFunc(debounceDelay, func() {
			publicarParaMaquina(machineCode)
		})
		debounceMu.Unlock()
	})

	// --- Suscripciones ---
	nc.Subscribe(subjAsignar, func(m *nats.Msg) {
		var msg model.MensajeAsignacion
		if json.Unmarshal(m.Data, &msg) != nil || msg.MachineCode == "" {
			return
		}
		ms.Asignar(msg)
	})

	nc.Subscribe(subjForced, func(m *nats.Msg) {
		var msg model.MensajeAsignacion
		if json.Unmarshal(m.Data, &msg) != nil || msg.MachineCode == "" {
			return
		}
		ms.AsignarForzado(msg)
	})

	nc.Subscribe(subjDesaignar, func(m *nats.Msg) {
		var msg model.MensajeAsignacion
		if json.Unmarshal(m.Data, &msg) != nil || msg.MachineCode == "" {
			return
		}
		ms.Desasignar(msg.MachineCode)
	})

	nc.Subscribe(subjLogin, func(m *nats.Msg) {
		var ev model.EventoRFID
		if json.Unmarshal(m.Data, &ev) != nil {
			return
		}
		_, esValido := ms.RegistrarPorID(ev.Tag, ev.UsbPort)
		if !esValido {
			deniedData := fmt.Sprintf(`{"usb_port":"%s","tag":"%s"}`, ev.UsbPort, ev.Tag)
			nc.Publish(topicDenied, []byte(deniedData))
			log.Printf("[emitter] Publicado en %s: %s", topicDenied, deniedData)
		}
	})

	nc.Subscribe(subjLogout, func(m *nats.Msg) {
		var ev model.EventoRFID
		if json.Unmarshal(m.Data, &ev) != nil {
			return
		}
		_, cambiado := ms.RetirarPorID(ev.Tag)
		if !cambiado {
			clearedData := fmt.Sprintf(`{"usb_port":"%s","tag":"%s"}`, ev.UsbPort, ev.Tag)
			nc.Publish(topicCleared, []byte(clearedData))
			log.Printf("[emitter] Publicado en %s: %s", topicCleared, clearedData)
		}
	})

	log.Println("[emitter] Emitter MULTI-MÁQUINA corriendo")
	log.Println("[emitter] Publica en: papid.emitter.<machine>, papid.personalui.sync/full, papid.personal.denied/cleared")
	log.Println("[emitter] Presiona Ctrl+C para detener")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Println("[emitter] Apagando...")
}

func conectar() *nats.Conn {
	opts := []nats.Option{nats.Name("papid-emitter-multi")}
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
