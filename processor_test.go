package main

import (
	"testing"
)

func TestParseLogLine(t *testing.T) {
	line := "2024-01-15 10:30:00,192.168.1.100,GET,/api/users,200,150"

	expectedIP := "192.168.1.100"
	expectedStatus := 200

	logEntry, err := parseLogLine(line)

	if err != nil {
		t.Fatalf("Функция вернула ошибку для валидной строки: %v", err)
	}

	if logEntry.IP != expectedIP {
		t.Errorf("Ожидался IP %s, получили %s", expectedIP, logEntry.IP)
	}

	if logEntry.StatusCode != expectedStatus {
		t.Errorf("Ожидался статус %d, получили %d", expectedStatus, logEntry.StatusCode)
	}
}

func TestParseLogLine_Error(t *testing.T) {
	line := "ошибка,данных,нет"

	_, err := parseLogLine(line)

	if err == nil {
		t.Error("Ожидалась ошибка для некорректной строки, но её нет")
	}
}
