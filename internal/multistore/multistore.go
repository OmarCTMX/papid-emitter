// Package multistore mantiene el estado ÚNICO del personal de una máquina
// física (la del emitter). Una persona aparece UNA sola vez, con un solo
// estado (verde/naranja), pero puede pertenecer a varias sub-máquinas
// (ej. a2i-1-r, a2i-2-r). El campo Maquinas guarda ese conjunto.
//
// Los mensajes de asignación (signed/unsigned) llegan por sub-máquina y se
// FUSIONAN aquí: no se sobreescriben. Así se puede saber en qué sub-máquinas
// está asignada cada persona.
package multistore

import (
	"log"
	"sort"
	"sync"
	"time"

	"papid-emitter/internal/model"
)

// Trabajador es el estado de una persona en la máquina.
type Trabajador struct {
	ID                  string
	Nombre              string
	Rol                 string
	Estado              string // empty | pending | active
	EmployeeID          string
	TipoAsignacionID    int
	YaRegistrado        bool
	FechaHoraInicioReal string
	UsbPort             string
	Maquinas            map[string]bool // sub-máquinas donde está asignado
}

// Store guarda el estado único del personal de la máquina del emitter.
type Store struct {
	mu           sync.RWMutex
	machineCode  string // máquina física (ej. a2i)
	machineID    string
	isFrozen     bool
	orderDetails model.OrderDetails
	personal     [model.TotalSlots]Trabajador
	onChange     func()
}

// New crea un store con los espacios vacíos.
func New(machineCode string) *Store {
	s := &Store{machineCode: machineCode}
	for i := range s.personal {
		s.personal[i] = vacante()
	}
	return s
}

func (s *Store) SetOnChange(fn func()) { s.onChange = fn }

func (s *Store) notificar() {
	if s.onChange != nil {
		s.onChange()
	}
}

// Asignar fusiona el personal de la sub-máquina subCode en el estado único.
func (s *Store) Asignar(subCode string, msg model.MensajeAsignacion) {
	s.aplicarAsignacion(subCode, msg, false)
}

// AsignarForzado hace lo mismo pero pone en verde a los entrantes.
func (s *Store) AsignarForzado(subCode string, msg model.MensajeAsignacion) {
	s.aplicarAsignacion(subCode, msg, true)
}

func (s *Store) aplicarAsignacion(subCode string, msg model.MensajeAsignacion, forzar bool) {
	s.mu.Lock()
	s.machineID = msg.MachineID
	s.isFrozen = msg.IsFrozen
	s.orderDetails = msg.OrderDetails

	// employee_ids que vienen en este signed de subCode.
	entrantes := make(map[string]bool)
	for _, p := range msg.Personnel {
		entrantes[p.EmployeeID] = true
	}

	// 1. Quitar subCode a quienes ya NO vienen en este signed de subCode.
	//    Si a alguien no le queda ninguna máquina, se vacía su espacio.
	for i := range s.personal {
		t := &s.personal[i]
		if t.Estado == model.EstadoEmpty {
			continue
		}
		if t.Maquinas[subCode] && !entrantes[t.EmployeeID] {
			delete(t.Maquinas, subCode)
			if len(t.Maquinas) == 0 {
				s.personal[i] = vacante()
			}
		}
	}

	// 2. Agregar / actualizar los entrantes.
	for _, p := range msg.Personnel {
		idx := s.buscarPorEmployee(p.EmployeeID)
		if idx == -1 {
			idx = s.slotLibre()
			if idx == -1 {
				log.Printf("[emitter] Sin slot libre para %s (máquina llena)", p.Name)
				continue
			}
			s.personal[idx] = Trabajador{
				Estado:   model.EstadoPending,
				Maquinas: map[string]bool{},
			}
		}
		t := &s.personal[idx]
		if t.Maquinas == nil {
			t.Maquinas = map[string]bool{}
		}
		t.Maquinas[subCode] = true
		// Datos base (se refrescan siempre).
		t.ID = p.NfcTag
		t.Nombre = p.Name
		t.Rol = valorODefault(p.TipoAsignacion, "—")
		t.EmployeeID = p.EmployeeID
		t.TipoAsignacionID = p.TipoAsignacionID
		if forzar {
			t.Estado = model.EstadoActive
			t.YaRegistrado = true
		}
	}
	s.mu.Unlock()
	log.Printf("[emitter] Asignación fusionada (sub-máquina %s)", subCode)
	s.notificar()
}

// Desasignar quita la sub-máquina subCode de todos. Quien quede sin máquinas
// se vacía.
func (s *Store) Desasignar(subCode string) {
	s.mu.Lock()
	for i := range s.personal {
		t := &s.personal[i]
		if t.Estado == model.EstadoEmpty {
			continue
		}
		if t.Maquinas[subCode] {
			delete(t.Maquinas, subCode)
			if len(t.Maquinas) == 0 {
				s.personal[i] = vacante()
			}
		}
	}
	s.mu.Unlock()
	log.Printf("[emitter] Desasignada sub-máquina %s", subCode)
	s.notificar()
}

// RegistrarPorID pone en verde (active) al trabajador con ese tag.
// Devuelve true si es válido (se registró o ya estaba activo).
func (s *Store) RegistrarPorID(tag, usbPort string) bool {
	s.mu.Lock()
	cambiado := false
	yaActivo := false
	for i := range s.personal {
		t := &s.personal[i]
		if t.ID != tag {
			continue
		}
		if t.Estado == model.EstadoPending {
			t.Estado = model.EstadoActive
			t.YaRegistrado = true
			t.UsbPort = usbPort
			if t.FechaHoraInicioReal == "" {
				t.FechaHoraInicioReal = time.Now().Format(time.RFC3339)
			}
			cambiado = true
			log.Printf("[emitter] Registrado: %s", t.Nombre)
			break
		}
		if t.Estado == model.EstadoActive {
			yaActivo = true
		}
	}
	s.mu.Unlock()

	if cambiado {
		s.notificar()
		return true
	}
	return yaActivo
}

// RetirarPorID regresa a pending (naranja) al trabajador activo con ese tag.
func (s *Store) RetirarPorID(tag string) bool {
	s.mu.Lock()
	cambiado := false
	for i := range s.personal {
		t := &s.personal[i]
		if t.ID == tag && t.Estado == model.EstadoActive {
			t.Estado = model.EstadoPending
			cambiado = true
			log.Printf("[emitter] Tarjeta retirada: %s", t.Nombre)
			break
		}
	}
	s.mu.Unlock()

	if cambiado {
		s.notificar()
	}
	return cambiado
}

// BuildSync arma el sync clásico (sin el campo maquinas).
func (s *Store) BuildSync() model.MensajeSync {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.MensajeSync{MachineCode: s.machineCode, Personnel: s.personnel(false)}
}

// BuildEmitterSync arma el sync para papid.emitter.sync (con maquinas).
func (s *Store) BuildEmitterSync() model.MensajeSync {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return model.MensajeSync{MachineCode: s.machineCode, Personnel: s.personnel(true)}
}

// BuildFull arma el mensaje completo (papid.personalui.full).
func (s *Store) BuildFull() model.MensajeFull {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hayAsignados := false
	todosYaReg := true
	for _, t := range s.personal {
		if t.Estado == model.EstadoEmpty {
			continue
		}
		hayAsignados = true
		if !t.YaRegistrado {
			todosYaReg = false
		}
	}
	return model.MensajeFull{
		IsActive:     hayAsignados && todosYaReg,
		IsFrozen:     s.isFrozen,
		MachineCode:  s.machineCode,
		MachineID:    s.machineID,
		OrderDetails: s.orderDetails,
		Personnel:    s.personnel(false),
	}
}

// personnel construye la lista de personal (omitiendo los espacios vacíos).
// Debe llamarse con el lock tomado.
func (s *Store) personnel(conMaquinas bool) []model.PersonaSync {
	out := make([]model.PersonaSync, 0, model.TotalSlots)
	for _, t := range s.personal {
		if t.Estado == model.EstadoEmpty {
			continue
		}
		p := model.PersonaSync{
			EmployeeID:          t.EmployeeID,
			Nombre:              t.Nombre,
			Rol:                 t.Rol,
			FechaHoraInicioReal: t.FechaHoraInicioReal,
			Status:              statusDe(t),
			TipoAsignacionID:    t.TipoAsignacionID,
			UsbPort:             t.UsbPort,
		}
		if conMaquinas {
			p.Maquinas = maquinasOrdenadas(t.Maquinas)
		}
		out = append(out, p)
	}
	return out
}

func (s *Store) buscarPorEmployee(empID string) int {
	for i := range s.personal {
		if s.personal[i].Estado != model.EstadoEmpty && s.personal[i].EmployeeID == empID {
			return i
		}
	}
	return -1
}

func (s *Store) slotLibre() int {
	for i := range s.personal {
		if s.personal[i].Estado == model.EstadoEmpty {
			return i
		}
	}
	return -1
}

func statusDe(t Trabajador) string {
	switch {
	case t.Estado == model.EstadoActive:
		return "Activo"
	case t.YaRegistrado:
		return "Inactivo"
	default:
		return "Asignado"
	}
}

func maquinasOrdenadas(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func vacante() Trabajador {
	return Trabajador{Estado: model.EstadoEmpty}
}

func valorODefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
