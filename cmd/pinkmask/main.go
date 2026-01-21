package main

import (
	"fmt"
	"os"

	"github.com/dyne/pinkmask/internal/config"
	"github.com/dyne/pinkmask/internal/copy"
	"github.com/dyne/pinkmask/internal/inspect"
	"github.com/dyne/pinkmask/internal/log"
	"github.com/dyne/pinkmask/internal/plan"
	"github.com/dyne/pinkmask/internal/transform"
	"github.com/spf13/cobra"
)

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
		Use:   "pinkmask",
		Short: "Deterministic SQLite anonymization and subsetting",
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

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func copyCmd(rootOpts *globalOptions, sample bool) *cobra.Command {
	var inPath string
	var outPath string
	var cfgPath string
	cmdName := "copy"
	cmdShort := "Copy a SQLite database with masking"
	if sample {
		cmdName = "sample"
		cmdShort = "Subset and mask a SQLite database"
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
				InPath:   inPath,
				OutPath:  outPath,
				Config:   cfg,
				Salt:     rootOpts.Salt,
				Seed:     rootOpts.Seed,
				FKMode:   rootOpts.FK,
				Triggers: rootOpts.Triggers,
				Jobs:     rootOpts.Jobs,
				TempDir:  rootOpts.TempDir,
				Subset:   sample,
				Logger:   logger,
			}
			return copy.Run(cmd.Context(), opts)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input SQLite file")
	cmd.Flags().StringVar(&outPath, "out", "", "output SQLite file")
	cmd.Flags().StringVar(&cfgPath, "config", "", "mask configuration file")
	_ = cmd.MarkFlagRequired("in")
	_ = cmd.MarkFlagRequired("out")
	return cmd
}

func inspectCmd(rootOpts *globalOptions) *cobra.Command {
	var inPath string
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
			return inspect.Run(cmd.Context(), inPath, logger)
		},
	}
	cmd.Flags().StringVar(&inPath, "in", "", "input SQLite file")
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
