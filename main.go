package main

import (
	"context"
	"fmt"
	"log"
)

// main запускает обработку логов, собирает статистику и печатает отчёты.
// В качестве источника используется файл testdata/logs.csv.
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filename := "testdata/logs.csv"
	const numWorkers int = 5

	inputChan, err := readLogs(filename)
	if err != nil {
		log.Fatalf("Ошибка при чтении файла: %v", err)
	}

	processedChan := processLogs(ctx, inputChan, numWorkers)

	unfilteredChan, filteredChan := tee(processedChan, 100)

	stats := calculateStats(unfilteredChan)

	errorStats := calculateStats(filterLogs(filteredChan, 400))

	fmt.Printf("\n--- ОБЩИЙ ОТЧЕТ ---\n")
	fmt.Printf("Всего запросов: %d\n", stats.TotalRequests)
	fmt.Printf("Количество ошибок: %d\n", stats.ErrorCount)
	fmt.Printf("Среднее время ответа: %.2f мс\n\n", stats.AverageRespTime)

	fmt.Printf("\n--- ОТЧЕТ ПО ОШИБКАМ ---\n")
	fmt.Printf("Всего запросов: %d\n", errorStats.TotalRequests)
	fmt.Printf("Количество ошибок: %d\n", errorStats.ErrorCount)
	fmt.Printf("Среднее время ответа: %.2f мс\n\n", errorStats.AverageRespTime)

	printTopIPs(stats.RequestsByIP, 5)
}
