package credentials

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"golang.org/x/term"
)

func PromptForCredentials(remoteType string) (map[string]string, error) {
	prompts := GetCredentialPrompts(remoteType)
	if len(prompts) == 0 {
		return nil, nil
	}

	result := make(map[string]string)
	reader := bufio.NewReader(os.Stdin)

	for _, p := range prompts {
		var value string
		var err error

		if p.Secret {
			value, err = readSecret(p.Label)
		} else {
			value, err = readLine(reader, p.Label)
		}

		if err != nil {
			return nil, err
		}

		if value != "" {
			result[p.Key] = value
		}
	}

	return result, nil
}

func readLine(reader *bufio.Reader, label string) (string, error) {
	fmt.Printf("%s: ", label)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func readSecret(label string) (string, error) {
	fmt.Printf("%s: ", label)

	fd := int(os.Stdin.Fd())
	if term.IsTerminal(fd) {
		password, err := term.ReadPassword(fd)
		fmt.Println()
		if err != nil {
			return "", err
		}
		return string(password), nil
	}

	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

func ConfirmSaveCredentials() (bool, error) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print("Save credentials to config? [Y/n]: ")
	line, err := reader.ReadString('\n')
	if err != nil {
		return false, err
	}
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "" || line == "y" || line == "yes", nil
}
