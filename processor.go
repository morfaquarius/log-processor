package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func parseLogLine(line string) (LogEntry, error) {
	parts := strings.Split(line, ",")

	if len(parts) < 6 {
		return LogEntry{}, fmt.Errorf("Неверный формат лога: недостаточно полей")
	}

	status, err := strconv.Atoi(parts[4])
	if err != nil {
		return LogEntry{}, fmt.Errorf("Неверный статус код: %w", err)
	}

	responseTime, err := strconv.Atoi(parts[5])
	if err != nil {
		return LogEntry{}, fmt.Errorf("Неверное время отклика: %w", err)
	}

	return LogEntry{
		Timestamp:    parts[0],
		IP:           parts[1],
		Method:       parts[2],
		URL:          parts[3],
		StatusCode:   status,
		ResponseTime: responseTime,
	}, nil
}

func readLogs(filename string) (<-chan LogEntry, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("невозможно открыть файл: %w", err)
	}

	logs := make(chan LogEntry)

	go func() {
		defer file.Close()
		defer close(logs)

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()

			logEntry, err := parseLogLine(line)

			if err != nil {
				log.Printf("Ошибка при парсинге строки: %v", err)
				continue
			}
			logs <- logEntry
		}
		if err := scanner.Err(); err != nil {
			log.Printf("Ошибка при чтении: %v", err)
		}
	}()

	return logs, nil
}

func processLogs(ctx context.Context, input <-chan LogEntry, numWorkers int) <-chan LogEntry {
	output := make(chan LogEntry)
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	worker := func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case logEntry, ok := <-input:
				if !ok {
					return
				}
				logEntry.URL = strings.ToLower(logEntry.URL)

				select {
				case output <- logEntry:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	for i := 0; i <= numWorkers; i++ {
		go worker()
	}

	go func() {
		defer wg.Wait()
		defer close(output)
	}()

	return output
}

func tee(input <-chan LogEntry, bufferSize int) (<-chan LogEntry, <-chan LogEntry) {
	output1 := make(chan LogEntry, bufferSize)
	output2 := make(chan LogEntry, bufferSize)
	go func() {
		defer close(output1)
		defer close(output2)
		for v := range input {
			output1 <- v
			output2 <- v
		}
	}()
	return output1, output2
}

func calculateStats(input <-chan LogEntry) Statistics {
	stats := Statistics{
		RequestsByIP: make(map[string]int),
	}
	var totalTime int
	for logEntry := range input {
		stats.TotalRequests++
		if logEntry.StatusCode >= 400 {
			stats.ErrorCount++
		}
		stats.RequestsByIP[logEntry.IP]++
		totalTime += logEntry.ResponseTime
	}
	if stats.TotalRequests > 0 {
		stats.AverageRespTime = float64(totalTime) / float64(stats.TotalRequests)
	}
	return stats
}

func filterLogs(input <-chan LogEntry, minStatus int) <-chan LogEntry {
	errorLogs := make(chan LogEntry)
	go func() {
		defer close(errorLogs)
		for logEntry := range input {
			if logEntry.StatusCode >= minStatus {
				errorLogs <- logEntry
			}
		}
	}()
	return errorLogs
}

func printTopIPs(requestsByIP map[string]int, n int) {
	type ipStats struct {
		ip    string
		count int
	}

	var stats []ipStats

	for ip, count := range requestsByIP {
		stats = append(stats, ipStats{ip, count})
	}

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].count > stats[j].count
	})

	fmt.Printf("\nТоп %d IP адресов:\n", n)
	for i := 0; i < n && i < len(stats); i++ {
		fmt.Printf("%s: %d запр.\n", stats[i].ip, stats[i].count)
	}
}
