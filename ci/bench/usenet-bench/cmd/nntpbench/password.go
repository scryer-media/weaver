package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
)

func resolvePassword(value, file string) (string, error) {
	if value != "" && file != "" {
		return "", errors.New("--password and --password-file cannot be combined")
	}
	if file == "" {
		return value, nil
	}
	contents, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("read NNTP password file: %w", err)
	}
	password := strings.TrimRight(string(contents), "\r\n")
	if password == "" {
		return "", errors.New("NNTP password file is empty")
	}
	return password, nil
}
