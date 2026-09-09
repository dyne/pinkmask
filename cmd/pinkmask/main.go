package main

import (
	"fmt"
	"os"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/copy"
	"github.com/dyne/pinkmask/internal/inspect"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/pb"
	"github.com/dyne/pinkmask/internal/plan"
	"github.com/dyne/pinkmask/internal/transform"
	"github.com/spf13/cobra"
)

var version = "dev"

type globalOptions struct {
	Verbose  bool
	Salt     string
	Seed     int64
	FK       string
	Triggers string
	Jobs     int
	TempDir  string
	Plugins  []string
}

func main() {
	rootOpts := &globalOptions{}
	root := &cobra.Command{
		Use:     "pinkmask",
		Short:   "Deterministic SQLite anonymization and subsetting",
		Version: version,
	}

	root.PersistentFlags().BoolVar(&rootOpts.Verbose, "verbose", false, "enable debug logging")
	root.PersistentFlags().StringVar(&rootOpts.Salt, "salt", "", "salt for deterministic hashing")
	root.PersistentFlags().Int64Var(&rootOpts.Seed, "seed", 0, "seed for deterministic generation")
	root.PersistentFlags().StringVar(&rootOpts.FK, "fk", "on", "foreign key enforcement (on|off)")
	root.PersistentFlags().StringVar(&rootOpts.Triggers, "triggers", "on", "trigger creation (on|off)")
	root.PersistentFlags().IntVar(&rootOpts.Jobs, "jobs", 4, "parallelism")
	root.PersistentFlags().StringVar(&rootOpts.TempDir, "tempdir", "", "temporary directory")
	root.PersistentFlags().StringSliceVar(&rootOpts.Plugins, "plugin", nil, "plugin .so path (repeatable)")

	root.AddCommand(copyCmd(rootOpts, false))
	root.AddCommand(copyCmd(rootOpts, true))
	root.AddCommand(inspectCmd(rootOpts))
	root.AddCommand(planCmd(rootOpts))
	root.AddCommand(pbCmd(rootOpts))
	root.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version)
		},
	})

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func copyCmd(rootOpts *globalOptions, sample bool) *cobra.Command {
	var inPath string
	var outPath string
	var cfgPath string
	var sourceEmail, sourcePassword string
	var destEmail, destPassword string
	cmdName := "copy"
	cmdShort := "Copy a SQLite database with masking"
	if sample {
		cmdName = "sample"
		cmdShort = "Subset and mask a SQLite database"
	}
	envStr := func(k, fallback string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return fallback
	}
	cmd := &cobra.Command{
		Use:   cmdName,
		Short: cmdShort,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := transform.LoadPlugins(rootOpts.Plugins); err != nil {
				return err
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			level := log.LevelInfo
			if rootOpts.Verbose {
				level = log.LevelDebug
			}
			logger := log.New(level, cmd.OutOrStdout())
			opts := copy.Options{
				InPath: inPath, OutPath: outPath,
				SourceEmail:    envStr("PB_SOURCE_EMAIL", sourceEmail),
				SourcePassword: envStr("PB_SOURCE_PASSWORD", sourcePassword),
				DestEmail:      envStr("PB_DEST_EMAIL", destEmail),
				DestPassword:   envStr("PB_DEST_PASSWORD", destPassword),
				Config:         cfg, Salt: rootOpts.Salt, Seed: rootOpts.Seed,
				FKMode: rootOpts.FK, Triggers: rootOpts.Triggers, Jobs: rootOpts.Jobs,
				TempDir: rootOpts.TempDir, Subset: sample, Logger: logger,
			}
			return copy.Run(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input SQLite file or pb:<url> endpoint")
	cmd.Flags().StringVar(&outPath, "out", "", "output SQLite file or pb:<url> endpoint")
	cmd.Flags().StringVar(&sourceEmail, "source-email", "", "source PocketBase superuser email")
	cmd.Flags().StringVar(&sourcePassword, "source-password", "", "source PocketBase superuser password")
	cmd.Flags().StringVar(&destEmail, "dest-email", "", "destination PocketBase superuser email")
	cmd.Flags().StringVar(&destPassword, "dest-password", "", "destination PocketBase superuser password")
	cmd.Flags().StringVar(&cfgPath, "config", "", "mask configuration file")
	_ = cmd.MarkFlagRequired("in")
	_ = cmd.MarkFlagRequired("out")
	return cmd
}

func inspectCmd(rootOpts *globalOptions) *cobra.Command {
	var inPath string
	var draftPath string
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Inspect schema and detect PII candidates",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := transform.LoadPlugins(rootOpts.Plugins); err != nil {
				return err
			}
			level := log.LevelInfo
			if rootOpts.Verbose {
				level = log.LevelDebug
			}
			logger := log.New(level, cmd.OutOrStdout())
			return inspect.Run(cmd.Context(), inPath, draftPath, logger)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input SQLite file")
	cmd.Flags().StringVar(&draftPath, "draft-config", "", "write a draft mask config to a file ('-' for stdout)")
	_ = cmd.MarkFlagRequired("in")
	return cmd
}

func planCmd(rootOpts *globalOptions) *cobra.Command {
	var inPath string
	var cfgPath string
	cmd := &cobra.Command{
		Use:   "plan",
		Short: "Show transformation plan",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := transform.LoadPlugins(rootOpts.Plugins); err != nil {
				return err
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			level := log.LevelInfo
			if rootOpts.Verbose {
				level = log.LevelDebug
			}
			logger := log.New(level, cmd.OutOrStdout())
			return plan.Run(cmd.Context(), inPath, cfg, logger)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input SQLite file")
	cmd.Flags().StringVar(&cfgPath, "config", "", "mask configuration file")
	_ = cmd.MarkFlagRequired("in")
	return cmd
}

func pbCmd(rootOpts *globalOptions) *cobra.Command {
	var sourceURL, sourceEmail, sourcePassword string
	var destURL, destEmail, destPassword string
	var cfgPath string
	envStr := func(k, fallback string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return fallback
	}
	cmd := &cobra.Command{
		Use:     "pb",
		Aliases: []string{"pocketbase"},
		Short:   "Copy records between PocketBase instances with masking",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := transform.LoadPlugins(rootOpts.Plugins); err != nil {
				return err
			}
			cfg, err := config.Load(cfgPath)
			if err != nil {
				return err
			}
			level := log.LevelInfo
			if rootOpts.Verbose {
				level = log.LevelDebug
			}
			logger := log.New(level, cmd.OutOrStdout())
			return pb.Run(cmd.Context(), pb.Options{
				SourceURL:      sourceURL,
				SourceEmail:    envStr("PB_SOURCE_EMAIL", sourceEmail),
				SourcePassword: envStr("PB_SOURCE_PASSWORD", sourcePassword),
				DestURL:        destURL,
				DestEmail:      envStr("PB_DEST_EMAIL", destEmail),
				DestPassword:   envStr("PB_DEST_PASSWORD", destPassword),
				Config:         cfg,
				Salt:           rootOpts.Salt,
				Seed:           rootOpts.Seed,
				Logger:         logger,
			})
		},
	}
	cmd.Flags().StringVar(&sourceURL, "source-url", "", "source PocketBase URL")
	cmd.Flags().StringVar(&sourceEmail, "source-email", "", "source superuser email")
	cmd.Flags().StringVar(&sourcePassword, "source-password", "", "source superuser password")
	cmd.Flags().StringVar(&destURL, "dest-url", "", "destination PocketBase URL")
	cmd.Flags().StringVar(&destEmail, "dest-email", "", "destination superuser email")
	cmd.Flags().StringVar(&destPassword, "dest-password", "", "destination superuser password")
	cmd.Flags().StringVar(&cfgPath, "config", "", "mask configuration file")
	_ = cmd.MarkFlagRequired("source-url")
	_ = cmd.MarkFlagRequired("dest-url")
	return cmd
}
