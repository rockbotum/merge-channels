package main

import (
	"fmt"
	"sync"
	"time"
)

func MergeChannels[T any](channels ...<-chan T) <-chan T {
	out := make(chan T)

	var wg sync.WaitGroup
	wg.Add(len(channels))

	for _, ch := range channels {
		ch := ch
		go func(c <-chan T) {
			defer wg.Done()
			for v := range c {
				out <- v
			}
		}(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func main() {
	// Создаём тестовые каналы
	ch1 := make(chan string)
	ch2 := make(chan string)
	ch3 := make(chan string)

	// Отправляем данные в первом канале
	go func() {
		messages := []string{"A", "B", "C"}
		for _, msg := range messages {
			ch1 <- msg
			time.Sleep(100 * time.Millisecond)
		}
		close(ch1)
	}()

	// Отправляем данные во втором канале
	go func() {
		messages := []string{"D", "E", "F"}
		for _, msg := range messages {
			ch2 <- msg
			time.Sleep(150 * time.Millisecond)
		}
		close(ch2)
	}()

	// Отправляем данные в третьем канале
	go func() {
		messages := []string{"G", "H", "I"}
		for _, msg := range messages {
			ch3 <- msg
			time.Sleep(80 * time.Millisecond)
		}
		close(ch3)
	}()

	// Объединяем каналы
	merged := MergeChannels(ch1, ch2, ch3)

	// Выводим результат
	for val := range merged {
		fmt.Println(val)
	}
}
