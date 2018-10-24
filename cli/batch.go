package cli

import (
	"errors"
	//"os"
	//"strings"
	//"time"

	"github.com/stelligent/mu/common"
	"github.com/stelligent/mu/workflows"
	"github.com/urfave/cli"
)

func newBatchCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:    BatchCmd,
		Aliases: []string{},
		Usage:   BatchUsage,
		Subcommands: []cli.Command{
			//*newBatchShowCommand(ctx),
			//*newBatchPushCommand(ctx),
			*newBatchDeployCommand(ctx),
			//*newBatchUndeployCommand(ctx),
			//*newBatchLogsCommand(ctx),
			//*newBatchExecuteCommand(ctx),
			//*newBatchRestartCommand(ctx),
		},
	}

	return cmd
}

/*
func newBatchShowCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:      ShowCmd,
		Usage:     SvcShowUsage,
		ArgsUsage: SvcShowUsage,
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  "watch, w",
				Usage: "watch results",
			},
			cli.BoolFlag{
				Name:  "tasks, t",
				Usage: "show task details",
			},
		},
		Action: func(c *cli.Context) error {
			service := c.Args().First()
			watch := c.Bool("watch")
			tasks := c.Bool("tasks")
			workflow := workflows.NewServiceViewer(ctx, service, ctx.DockerOut, tasks)
			for true {
				if watch {
					print("\033[H\033[2J")
				}

				err := workflow()
				if err != nil {
					return err
				} else if watch {
					time.Sleep(10 * time.Second)
				} else {
					break
				}
			}
			return nil
		},
	}

	return cmd
}
*/

/*
func newBatchPushCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:  PushCmd,
		Usage: BatchPushCmdUsage,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  TagFlagName,
				Usage: SvcPushTagFlagUsage,
			},
			cli.StringFlag{
				Name:  ProviderFlagName,
				Usage: SvcPushProviderFlagUsage,
			},
			cli.StringFlag{
				Name:  KmsKeyFlagName,
				Usage: SvcPushKmsKeyFlagUsage,
			},
		},
		Action: func(c *cli.Context) error {
			tag := c.String(Tag)
			provider := c.String(Provider)
			kmsKey := c.String(KmsKey)
			workflow := workflows.NewServicePusher(ctx, tag, provider, kmsKey, ctx.DockerOut)
			return workflow()
		},
	}

	return cmd
}
*/

func newBatchDeployCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:      DeployCmd,
		Usage:     BatchDeployCmdUsage,
		ArgsUsage: EnvArgUsage,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  TagFlagName,
				Usage: BatchDeployTagFlagUsage,
			},
		},
		Action: func(c *cli.Context) error {
			environmentName := c.Args().First()
			if len(environmentName) == Zero {
				cli.ShowCommandHelp(c, DeployCmd)
				return errors.New(NoEnvValidation)
			}
			tag := c.String(Tag)
			workflow := workflows.NewBatchDeployer(ctx, environmentName, tag)
			return workflow()
		},
	}

	return cmd
}

/*

func newBatchUndeployCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:      UndeployCmd,
		Usage:     SvcUndeployCmdUsage,
		ArgsUsage: SvcUndeployArgsUsage,
		Action: func(c *cli.Context) error {
			environmentName := c.Args().First()
			if len(environmentName) == Zero {
				cli.ShowCommandHelp(c, UndeployCmd)
				return errors.New(NoEnvValidation)
			}
			serviceName := c.Args().Get(SvcUndeploySvcFlagIndex)
			workflow := workflows.NewServiceUndeployer(ctx, serviceName, environmentName)
			return workflow()
		},
	}

	return cmd
}

func newBatchRestartCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:      RestartCmd,
		Usage:     RestartUsage,
		ArgsUsage: EnvArgUsage,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  ServiceFlag,
				Usage: SvcRestartServiceFlagUsage,
			},
			cli.IntFlag{
				Name:  BatchFlag,
				Usage: SvcRestartBatchFlagUsage,
				Value: 1,
			},
		},
		Action: func(c *cli.Context) error {
			environmentName := c.Args().First()
			if len(environmentName) == Zero {
				cli.ShowCommandHelp(c, RestartCmd)
				return errors.New(NoEnvValidation)
			}

			serviceName := c.String(SvcCmd)
			batchSize := c.Int(BatchSize)

			workflow := workflows.NewServiceRestarter(ctx, environmentName, serviceName, batchSize)
			return workflow()
		},
	}
	return cmd
}

func newBatchLogsCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:  LogsCmd,
		Usage: SvcLogUsage,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  ServiceFlag,
				Usage: SvcLogServiceFlagUsage,
			},
			cli.BoolFlag{
				Name:  FollowFlag,
				Usage: FollowUsage,
			},
			cli.DurationFlag{
				Name:  SearchDurationFlag,
				Usage: SearchDurationUsage,
				Value: DefaultLogDurationValue,
			},
		},
		ArgsUsage: SvcLogArgUsage,
		Action: func(c *cli.Context) error {
			environmentName := c.Args().First()
			if len(environmentName) == Zero {
				cli.ShowCommandHelp(c, LogsCmd)
				return errors.New(NoEnvValidation)
			}
			serviceName := c.String(SvcCmd)

			workflow := workflows.NewServiceLogViewer(ctx, c.Duration(SearchDuration), c.Bool(Follow), environmentName, serviceName, os.Stdout, strings.Join(c.Args().Tail(), Space))
			return workflow()
		},
	}

	return cmd
}



func validateExecuteArguments(ctx *cli.Context) error {
	environmentName := ctx.Args().First()
	argLength := len(ctx.Args())

	if argLength == Zero || len(strings.TrimSpace(environmentName)) == Zero {
		cli.ShowCommandHelp(ctx, ExeCmd)
		return errors.New(NoEnvValidation)
	}
	if argLength == ExeArgsCmdIndex {
		cli.ShowCommandHelp(ctx, ExeCmd)
		return errors.New(NoCmdValidation)
	}
	if len(strings.TrimSpace(ctx.Args().Get(ExeArgsCmdIndex))) == Zero {
		cli.ShowCommandHelp(ctx, ExeCmd)
		return errors.New(EmptyCmdValidation)
	}
	return nil
}



func newBatchExecuteCommand(ctx *common.Context) *cli.Command {
	cmd := &cli.Command{
		Name:      ExeCmd,
		Usage:     ExeUsage,
		ArgsUsage: ExeArgs,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  ServiceFlag,
				Usage: SvcExeServiceFlagUsage,
			},
			cli.StringFlag{
				Name:   TaskFlag,
				Usage:  SvcExeTaskFlagUsage,
				Hidden: TaskFlagVisible,
			},
			cli.StringFlag{
				Name:   ClusterFlag,
				Usage:  SvcExeClusterFlagUsage,
				Hidden: ClusterFlagVisible,
			},
		},
		Action: func(c *cli.Context) error {
			task, err := newTask(c)
			if err != nil {
				return err
			}

			workflow := workflows.NewServiceExecutor(ctx, *task)
			return workflow()
		},
	}
	return cmd
}
*/

/*
func newTask(c *cli.Context) (*common.Task, error) {
	err := validateExecuteArguments(c)
	if err != nil {
		return nil, err
	}
	environmentName := c.Args().First()
	command := c.Args()[ExeArgsCmdIndex:]
	return &common.Task{
		Environment:    environmentName,
		Command:        command,
		Service:        c.String(SvcCmd),
		TaskDefinition: c.String(TaskFlagName),
		Cluster:        c.String(ClusterFlagName),
	}, nil
}
*/
