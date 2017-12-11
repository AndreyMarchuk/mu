package workflows

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/stelligent/mu/common"
	"io"
)

type purgeWorkflow struct {
	repoName string
}

// NewPurge create a new workflow for purging mu resources
func NewPurge(ctx *common.Context, writer io.Writer) Executor {
	workflow := new(purgeWorkflow)

	return newPipelineExecutor(
		workflow.purgeWorker(ctx, ctx.StackManager, writer),
	)
}

//
//
// main.go: main
// cli/app.go: NewApp
// cli/environments.go: newEnvironmentsCommand
// cli/environments.go: newEnvironmentsTerminateCommand
// workflows/environment_terminate.go: NewEnvironmentTerminate

// main.go: main
// cli/app.go: NewApp
// cli/services.go: newServicesCommand
// cli/services.go: newServicesUndeployCommand
// workflows/service_undeploy.go: newServiceUndeployer

//Workflow sequence
//
//for region in region-list (default to current, maybe implement a --region-list or --all-regions switch)
//  for namespace in namespaces (default to specified namespace)
//    for environment in all-environments (i.e. acceptance/production)
//      for service in services (all services in environment)
//         invoke 'svc undeploy'
//         invoke `env term`
//remove ECS repo
//invoke `pipeline term`
//remove s3 bucket containing environment name
//remove RDS databases
//
//other artifacts to remove:
//* common IAM roles
//* cloudwatch buckets
//* cloudwatch dashboards
//* (should be covered by CFN stack removal)
//* ECS scheduled tasks
//* SES
//* SNS
//* SQS
//* ELB
//* EC2 subnet
//* EC2 VPC Gateway attachment
//* security groups
//* EC2 Network ACL
//* EC2 Routetable
//* CF stacks

func removeStacksByStatus(stacks []*common.Stack, statuses []string) []*common.Stack {
	var ret []*common.Stack
	for _, stack := range stacks {
		found := false
		for _, status := range statuses {
			if stack.Status == status {
				found = true
			}
		}
		if !found {
			ret = append(ret, stack)
		}
	}
	return ret
}

func filterStacksByType(stacks []*common.Stack, stackType common.StackType) []*common.Stack {
	var ret []*common.Stack
	for _, stack := range stacks {
		if stack.Tags["type"] == string(stackType) {
			ret = append(ret, stack)
		}
	}
	return ret
}

func (workflow *purgeWorkflow) purgeWorker(ctx *common.Context, stackLister common.StackLister, writer io.Writer) Executor {
	return func() error {

		// TODO establish outer loop for regions
		// TODO establish outer loop for multiple namespaces
		// purgeMap := make(map[string][]*common.Stack)

		// gather all the stackNames for each type (in parallel)
		stacks, err := stackLister.ListStacks(common.StackTypeAll)
		if err != nil {
			log.Warning("couldn't list stacks (all)")
		}
		stacks = removeStacksByStatus(stacks, []string{cloudformation.StackStatusRollbackComplete})

		table := CreateTableSection(writer, PurgeHeader)
		stackCount := 0
		for _, stack := range stacks {
			stackType, ok := stack.Tags["type"]
			if ok {
				table.Append([]string{
					Bold(stackType),
					stack.Name,
					fmt.Sprintf(KeyValueFormat, colorizeStackStatus(stack.Status), stack.StatusReason),
					stack.StatusReason,
					stack.LastUpdateTime.Local().Format(LastUpdateTime),
				})
				stackCount++
			}
		}
		table.Render()

		// create a grand master list of all the things we're going to delete
		var executors []Executor

		svcWorkflow := new(serviceWorkflow)

		// add the services we're going to terminate

		for _, stack := range filterStacksByType(stacks, common.StackTypeService) {
			// TODO - scheduled tasks are attached to service, so must be deleted first.
			// common.StackTypeSchedule
			executors = append(executors, svcWorkflow.serviceInput(ctx, stack.Tags["service"]))
			executors = append(executors, svcWorkflow.serviceUndeployer(ctx.Config.Namespace, stack.Tags["environment"], ctx.StackManager, ctx.StackManager))
		}

		// Add the terminator jobs to the master list for each environment
		envWorkflow := new(environmentWorkflow)
		for _, stack := range filterStacksByType(stacks, common.StackTypeEnv) {
			// Add the terminator jobs to the master list for each environment
			envName := stack.Tags["environment"]

			executors = append(executors, envWorkflow.environmentServiceTerminator(envName, ctx.StackManager, ctx.StackManager, ctx.StackManager, ctx.RolesetManager))
			executors = append(executors, envWorkflow.environmentDbTerminator(envName, ctx.StackManager, ctx.StackManager, ctx.StackManager))
			executors = append(executors, envWorkflow.environmentEcsTerminator(ctx.Config.Namespace, envName, ctx.StackManager, ctx.StackManager))
			executors = append(executors, envWorkflow.environmentConsulTerminator(ctx.Config.Namespace, envName, ctx.StackManager, ctx.StackManager))
			executors = append(executors, envWorkflow.environmentRolesetTerminator(ctx.RolesetManager, envName))
			executors = append(executors, envWorkflow.environmentElbTerminator(ctx.Config.Namespace, envName, ctx.StackManager, ctx.StackManager))
			executors = append(executors, envWorkflow.environmentVpcTerminator(ctx.Config.Namespace, envName, ctx.StackManager, ctx.StackManager))
		}

		// add the pipelines to terminate
		codePipelineWorkflow := new(pipelineWorkflow)
		for _, codePipeline := range filterStacksByType(stacks, common.StackTypePipeline) {
			// log.Infof("%s %v", codePipeline.Name, codePipeline.Tags)
			executors = append(executors, codePipelineWorkflow.serviceFinder(codePipeline.Tags["service"], ctx))
			executors = append(executors, codePipelineWorkflow.pipelineTerminator(ctx.Config.Namespace, ctx.StackManager, ctx.StackManager))
			executors = append(executors, codePipelineWorkflow.pipelineRolesetTerminator(ctx.RolesetManager))
		}

		// add the ecs repos to terminate

		for _, codePipeline := range filterStacksByType(stacks, common.StackTypePipeline) {
			// log.Infof("%s %v", codePipeline.Name, codePipeline.Tags)
			executors = append(executors, codePipelineWorkflow.serviceFinder(codePipeline.Tags["service"], ctx))
			executors = append(executors, codePipelineWorkflow.pipelineTerminator(ctx.Config.Namespace, ctx.StackManager, ctx.StackManager))
			executors = append(executors, codePipelineWorkflow.pipelineRolesetTerminator(ctx.RolesetManager))
		}

		// QUESTION: do we want to delete stacks of type CodeCommit?  (currently, my example is github)

		// common.StackTypeLoadBalancer
		// common.StackTypeDatabase - databaseWorkflow
		// common.StackTypeBucket
		// common.StackTypeVpc

		// logsWorkflow (for cloudwatch workflows)

		// common.StackTypeRepo
		// delete repo by AWS CLI remove, key is in Tags["repo"]



		log.Infof("total of %d stacks of %d types to purge", stackCount, len(executors))

		// newPipelineExecutorNoStop is just like newPipelineExecutor, except that it doesn't stop on error
		executor := newPipelineExecutorNoStop(executors...)

		// run everything we've collected
		executor()
		return nil
	}
}
