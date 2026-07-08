// Package model define las estructuras de datos del emitter.
package model

// Estados posibles de cada espacio.
const (
	EstadoEmpty   = "empty"
	EstadoPending = "pending"
	EstadoActive  = "active"
)

// TotalSlots es la cantidad fija de espacios por máquina.
const TotalSlots = 3

// Trabajador representa a una persona en un espacio.
type Trabajador struct {
	ID               string
	Nombre           string
	Rol              string
	Estado           string
	EmployeeID       string
	TipoAsignacionID int
	YaRegistrado     bool
	FechaHoraInicioReal string
	UsbPort          string
}

// Vacante devuelve un espacio vacío.
func Vacante() Trabajador {
	return Trabajador{ID: "", Nombre: "No asignado", Rol: "—", Estado: EstadoEmpty}
}

// EventoRFID es el mensaje de login/logout del lector.
type EventoRFID struct {
	Tag     string `json:"tag"`
	UsbPort string `json:"usb_port"`
}

// OrderDetails son los datos de la orden.
type OrderDetails struct {
	LotesActuales string `json:"lotes_actuales"`
	LotesTotales  string `json:"lotes_totales"`
}

// MensajeAsignacion es el mensaje de papid.admin.signed / unsigned.
type MensajeAsignacion struct {
	MachineCode  string            `json:"machine_code"`
	MachineID    string            `json:"machine_id"`
	IsActive     bool              `json:"is_active"`
	IsFrozen     bool              `json:"is_frozen"`
	OrderDetails OrderDetails      `json:"order_details"`
	Personnel    []PersonaAsignada `json:"personnel"`
}

// PersonaAsignada es cada elemento del array "personnel" recibido.
type PersonaAsignada struct {
	Name             string `json:"name"`
	TipoAsignacion   string `json:"tipo_asignacion"`
	TipoAsignacionID int    `json:"tipo_asignacion_id"`
	NfcTag           string `json:"nfc_tag"`
	Status           string `json:"status"`
	EmployeeID       string `json:"employee_id"`
}

// MensajeSync es lo que publicamos en papid.personalui.sync.
type MensajeSync struct {
	MachineCode string        `json:"machine_code"`
	Personnel   []PersonaSync `json:"personnel"`
}

// MensajeFull es lo que publicamos en papid.personalui.full.
type MensajeFull struct {
	IsActive     bool          `json:"is_active"`
	IsFrozen     bool          `json:"is_frozen"`
	MachineCode  string        `json:"machine_code"`
	MachineID    string        `json:"machine_id"`
	OrderDetails OrderDetails  `json:"order_details"`
	Personnel    []PersonaSync `json:"personnel"`
}

// PersonaSync es cada elemento del "personnel" que publicamos.
type PersonaSync struct {
	EmployeeID          string `json:"employee_id"`
	Nombre              string `json:"nombre,omitempty"`
	Rol                 string `json:"rol,omitempty"`
	FechaHoraInicioReal string `json:"fecha_hora_inicio_real,omitempty"`
	Status              string `json:"status"`
	TipoAsignacionID    int    `json:"tipo_asignacion_id"`
	UsbPort             string `json:"usb_port,omitempty"`
}
