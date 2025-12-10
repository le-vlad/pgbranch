package cli

import (
	"fmt"

	"github.com/fatih/color"
	"github.com/le-vlad/pgbranch/internal/credentials"
	"github.com/spf13/cobra"
)

func newKeysCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "keys",
		Short: "Manage encryption keys for credentials",
	}

	cmd.AddCommand(newKeysGenerateCmd())
	cmd.AddCommand(newKeysStatusCmd())

	return cmd
}

func newKeysGenerateCmd() *cobra.Command {
	var force bool

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate a new encryption key",
		Long: `Generate a new encryption key for encrypting remote credentials.

The key is stored in ~/.pgbranch_key and is used to encrypt credentials
stored in project config files.

Warning: Regenerating the key will make existing encrypted credentials
unreadable. Use --force to regenerate anyway.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			keyPath, err := credentials.GetKeyPath()
			if err != nil {
				return err
			}

			if credentials.KeyExists() && !force {
				fmt.Printf("Encryption key already exists at: %s\n", keyPath)
				fmt.Println("Use --force to regenerate (will invalidate existing encrypted credentials)")
				return nil
			}

			key, err := credentials.GenerateKey()
			if err != nil {
				return fmt.Errorf("failed to generate key: %w", err)
			}

			if err := credentials.SaveKey(key); err != nil {
				return fmt.Errorf("failed to save key: %w", err)
			}

			green := color.New(color.FgGreen).SprintFunc()
			if force && credentials.KeyExists() {
				fmt.Printf("%s Regenerated encryption key at: %s\n", green("✓"), keyPath)
				yellow := color.New(color.FgYellow).SprintFunc()
				fmt.Printf("%s Existing encrypted credentials will need to be re-entered\n", yellow("!"))
			} else {
				fmt.Printf("%s Generated encryption key at: %s\n", green("✓"), keyPath)
			}

			return nil
		},
	}

	cmd.Flags().BoolVarP(&force, "force", "f", false, "Regenerate key even if one exists")

	return cmd
}

func newKeysStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show encryption key status",
		RunE: func(cmd *cobra.Command, args []string) error {
			keyPath, err := credentials.GetKeyPath()
			if err != nil {
				return err
			}

			if credentials.KeyExists() {
				green := color.New(color.FgGreen).SprintFunc()
				fmt.Printf("%s Encryption key exists\n", green("✓"))
				fmt.Printf("  Location: %s\n", keyPath)
			} else {
				yellow := color.New(color.FgYellow).SprintFunc()
				fmt.Printf("%s No encryption key found\n", yellow("!"))
				fmt.Printf("  Expected location: %s\n", keyPath)
				fmt.Println("\nRun 'pgbranch keys generate' to create one")
			}

			return nil
		},
	}

	return cmd
}
