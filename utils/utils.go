package utils

import "reflect"

// RecoverErrorToString processes a recover response for error reporting.
func RecoverErrorToString(r reflect.Value) string {
	switch r.Kind() {
	case reflect.String:
		return r.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return string(rune(r.Int())) // https://github.com/prometheus/prometheus/commit/ed6ce7ac98b3592ea004fc4de661dbfdc998ea56
	default:
		return "Unknown error"
	}
}
