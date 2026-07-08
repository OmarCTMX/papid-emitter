// Package multistore mantiene el estado del personal de MÚLTIPLES máquinas.
// Un solo emitter puede procesar todas las máquinas y publicar al subject
// correcto de cada una (papid.emitter.<machine_code>).
package multistore

import (
	"log"
	"sync"
	"time"

	"papid-emitter/internal/model"
)

// Trabajador es el estado de una persona en una máquina.
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
}

// Maquina es el estado de una máquina con su personal.
type Maquina struct {
	MachineCode  string
	MachineID    string
	IsFrozen     bool
	OrderDetails model.OrderDetails
	Personal     [model.TotalSlots]Trabajador
}

// MultiStore guarda el estado de todas las máquinas.
type MultiStore struct {
	mu       sync.RWMutex
	maquinas map[string]*Maquina // clave: machine_code

	// Se llama cada vez que cambia una máquina, pasando su machine_code.
	onChange func(machineCode string)
}

func New() *MultiStore {
	return &MultiStore{maquinas: make(map[string]*Maquina)}
}

func (ms *MultiStore) SetOnChange(fn func(machineCode string)) {
	ms.onChange = fn
}

func (ms *MultiStore) notificar(machineCode string) {
	if ms.onChange != nil {
		ms.onChange(machineCode)
	}
}

// getMaquina devuelve la máquina o la crea si no existe. Debe llamarse con lock.
func (ms *MultiStore) getMaquina(code string) *Maquina {
	mq, ok := ms.maquinas[code]
	if !ok {
		mq = &Maquina{MachineCode: code}
		for i := range mq.Personal {
			mq.Personal[i] = vacante()
		}
		ms.maquinas[code] = mq
	}
	return mq
}

// Asignar actualiza el personal de una máquina.
func (ms *MultiStore) Asignar(msg model.MensajeAsignacion) {
	ms.mu.Lock()
	mq := ms.getMaquina(msg.MachineCode)
	mq.MachineID = msg.MachineID
	mq.IsFrozen = msg.IsFrozen
	mq.OrderDetails = msg.OrderDetails

	for i := 0; i < model.TotalSlots; i++ {
		if i < len(msg.Personnel) {
			p := msg.Personnel[i]
			estado := model.EstadoPending
			yaReg := false
			usbPort := ""
			// Conservar estado previo si la persona ya estaba.
			prev := mq.Personal[i]
			if prev.ID == p.NfcTag {
				if prev.Estado == model.EstadoActive {
					estado = model.EstadoActive
				}
				yaReg = prev.YaRegistrado
				usbPort = prev.UsbPort
			} else {
				for _, existing := range mq.Personal {
					if existing.ID == p.NfcTag {
						if existing.Estado == model.EstadoActive {
							estado = model.EstadoActive
						}
						yaReg = existing.YaRegistrado
						usbPort = existing.UsbPort
						break
					}
				}
			}
			mq.Personal[i] = Trabajador{
				ID: p.NfcTag, Nombre: p.Name, Rol: valorODefault(p.TipoAsignacion, "—"),
				Estado: estado, EmployeeID: p.EmployeeID,
				TipoAsignacionID: p.TipoAsignacionID, YaRegistrado: yaReg, UsbPort: usbPort,
			}
		} else {
			mq.Personal[i] = vacante()
		}
	}
	ms.mu.Unlock()
	log.Printf("[emitter] Asignación actualizada para %s", msg.MachineCode)
	ms.notificar(msg.MachineCode)
}

// AsignarForzado pone a todos en verde.
func (ms *MultiStore) AsignarForzado(msg model.MensajeAsignacion) {
	ms.mu.Lock()
	mq := ms.getMaquina(msg.MachineCode)
	mq.MachineID = msg.MachineID
	mq.IsFrozen = msg.IsFrozen
	mq.OrderDetails = msg.OrderDetails

	for i := 0; i < model.TotalSlots; i++ {
		if i < len(msg.Personnel) {
			p := msg.Personnel[i]
			mq.Personal[i] = Trabajador{
				ID: p.NfcTag, Nombre: p.Name, Rol: valorODefault(p.TipoAsignacion, "—"),
				Estado: model.EstadoActive, EmployeeID: p.EmployeeID,
				TipoAsignacionID: p.TipoAsignacionID, YaRegistrado: true,
			}
		} else {
			mq.Personal[i] = vacante()
		}
	}
	ms.mu.Unlock()
	log.Printf("[emitter] Asignación FORZADA para %s", msg.MachineCode)
	ms.notificar(msg.MachineCode)
}

// Desasignar vacía una máquina.
func (ms *MultiStore) Desasignar(machineCode string) {
	ms.mu.Lock()
	if mq, ok := ms.maquinas[machineCode]; ok {
		for i := range mq.Personal {
			mq.Personal[i] = vacante()
		}
	}
	ms.mu.Unlock()
	log.Printf("[emitter] Desasignado: %s", machineCode)
	ms.notificar(machineCode)
}

// RegistrarPorID busca el tag en TODAS las máquinas y pone en verde.
// Devuelve (machine_code afectada, esValido, usbPort del evento).
func (ms *MultiStore) RegistrarPorID(tag string, usbPort string) (string, bool) {
	ms.mu.Lock()
	var afectada string
	for code, mq := range ms.maquinas {
		for i := range mq.Personal {
			if mq.Personal[i].ID == tag && mq.Personal[i].Estado == model.EstadoPending {
				mq.Personal[i].Estado = model.EstadoActive
				mq.Personal[i].YaRegistrado = true
				mq.Personal[i].UsbPort = usbPort
				if mq.Personal[i].FechaHoraInicioReal == "" {
					mq.Personal[i].FechaHoraInicioReal = time.Now().Format(time.RFC3339)
				}
				afectada = code
				log.Printf("[emitter] Registrado: %s en %s", mq.Personal[i].Nombre, code)
				break
			}
		}
		if afectada != "" {
			break
		}
	}
	ms.mu.Unlock()

	if afectada != "" {
		ms.notificar(afectada)
		return afectada, true
	}

	// Verificar si ya estaba activo (no es error).
	ms.mu.RLock()
	for _, mq := range ms.maquinas {
		for _, t := range mq.Personal {
			if t.ID == tag && t.Estado == model.EstadoActive {
				ms.mu.RUnlock()
				return "", true // ya activo, válido pero sin cambio
			}
		}
	}
	ms.mu.RUnlock()
	return "", false // no encontrado en ninguna máquina
}

// RetirarPorID busca el tag y vuelve a pending.
func (ms *MultiStore) RetirarPorID(tag string) (string, bool) {
	ms.mu.Lock()
	var afectada string
	for code, mq := range ms.maquinas {
		for i := range mq.Personal {
			if mq.Personal[i].ID == tag && mq.Personal[i].Estado == model.EstadoActive {
				mq.Personal[i].Estado = model.EstadoPending
				afectada = code
				log.Printf("[emitter] Tarjeta retirada: %s en %s", mq.Personal[i].Nombre, code)
				break
			}
		}
		if afectada != "" {
			break
		}
	}
	ms.mu.Unlock()

	if afectada != "" {
		ms.notificar(afectada)
		return afectada, true
	}
	return "", false
}

// BuildSync arma el mensaje sync para una máquina específica.
func (ms *MultiStore) BuildSync(machineCode string) model.MensajeSync {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	mq, ok := ms.maquinas[machineCode]
	if !ok {
		return model.MensajeSync{MachineCode: machineCode}
	}

	personnel := make([]model.PersonaSync, 0, model.TotalSlots)
	for _, t := range mq.Personal {
		if t.Estado == model.EstadoEmpty {
			continue
		}
		personnel = append(personnel, model.PersonaSync{
			EmployeeID:          t.EmployeeID,
			Nombre:              t.Nombre,
			Rol:                 t.Rol,
			FechaHoraInicioReal: t.FechaHoraInicioReal,
			Status:              statusDe(t),
			TipoAsignacionID:    t.TipoAsignacionID,
			UsbPort:             t.UsbPort,
		})
	}
	return model.MensajeSync{MachineCode: machineCode, Personnel: personnel}
}

// BuildFull arma el mensaje full para una máquina específica.
func (ms *MultiStore) BuildFull(machineCode string) model.MensajeFull {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	mq, ok := ms.maquinas[machineCode]
	if !ok {
		return model.MensajeFull{MachineCode: machineCode}
	}

	hayAsignados := false
	todosYaReg := true
	personnel := make([]model.PersonaSync, 0, model.TotalSlots)
	for _, t := range mq.Personal {
		if t.Estado == model.EstadoEmpty {
			continue
		}
		hayAsignados = true
		if !t.YaRegistrado {
			todosYaReg = false
		}
		personnel = append(personnel, model.PersonaSync{
			EmployeeID:          t.EmployeeID,
			Nombre:              t.Nombre,
			Rol:                 t.Rol,
			FechaHoraInicioReal: t.FechaHoraInicioReal,
			Status:              statusDe(t),
			TipoAsignacionID:    t.TipoAsignacionID,
			UsbPort:             t.UsbPort,
		})
	}

	return model.MensajeFull{
		IsActive:     hayAsignados && todosYaReg,
		IsFrozen:     mq.IsFrozen,
		MachineCode:  mq.MachineCode,
		MachineID:    mq.MachineID,
		OrderDetails: mq.OrderDetails,
		Personnel:    personnel,
	}
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

func vacante() Trabajador {
	return Trabajador{Estado: model.EstadoEmpty}
}

func valorODefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
