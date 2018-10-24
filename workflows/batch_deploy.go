package workflows

import (
	//"encoding/base64"
	//"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/stelligent/mu/common"
)

// NewBatchDeployer create a new workflow for deploying (registering) a batch job in an environment
func NewBatchDeployer(ctx *common.Context, environmentName string, tag string) Executor {

	workflow := new(batchWorkflow)
	workflow.codeRevision = ctx.Config.Repo.Revision
	workflow.repoName = ctx.Config.Repo.Slug

	stackParams := make(map[string]string)

	return newPipelineExecutor(
		workflow.batchLoader(ctx, tag, ""),
		workflow.batchEnvironmentLoader(ctx.Config.Namespace, environmentName, ctx.StackManager),
		workflow.batchApplyParams(ctx.Config.Namespace, &ctx.Config.Batch, stackParams, environmentName, ctx.StackManager, ctx.ParamManager, ctx.RolesetManager),
		workflow.batchRolesetUpserter(ctx.RolesetManager, ctx.RolesetManager, environmentName),
		workflow.batchRepoUpserter(ctx.Config.Namespace, &ctx.Config.Batch, ctx.StackManager, ctx.StackManager),
		workflow.batchDeployer(ctx.Config.Namespace, &ctx.Config.Batch, stackParams, environmentName, ctx.StackManager, ctx.StackManager),
	)
}

func (workflow *batchWorkflow) batchEnvironmentLoader(namespace string, environmentName string, stackWaiter common.StackWaiter) Executor {
	return func() error {

		envStackName := common.CreateStackName(namespace, common.StackTypeEnv, environmentName)
		workflow.envStack = stackWaiter.AwaitFinalStatus(envStackName)

		if workflow.envStack == nil {
			return fmt.Errorf("Unable to find stack '%s' for environment '%s'", envStackName, environmentName)
		}

		return nil
	}
}

func (workflow *batchWorkflow) batchRolesetUpserter(rolesetUpserter common.RolesetUpserter, rolesetGetter common.RolesetGetter, environmentName string) Executor {
	return func() error {
		err := rolesetUpserter.UpsertCommonRoleset()
		if err != nil {
			return err
		}

		commonRoleset, err := rolesetGetter.GetCommonRoleset()
		if err != nil {
			return err
		}

		workflow.cloudFormationRoleArn = commonRoleset["CloudFormationRoleArn"]

		err = rolesetUpserter.UpsertBatchRoleset(environmentName, workflow.serviceName)
		if err != nil {
			return err
		}

		serviceRoleset, err := rolesetGetter.GetBatchRoleset(environmentName, workflow.serviceName)
		if err != nil {
			return err
		}
		//workflow.ecsEventsRoleArn = serviceRoleset["EcsEventsRoleArn"]
		workflow.batchJobRoleArn = serviceRoleset["BatchJobRoleArn"]

		return nil
	}
}

func (workflow *batchWorkflow) batchApplyParams(namespace string, service *common.Batch,
	params map[string]string, environmentName string, stackWaiter common.StackWaiter,
	paramGetter common.ParamGetter, rolesetGetter common.RolesetGetter) Executor {
	return func() error {

		params["ServiceName"] = workflow.serviceName
		params["Namespace"] = namespace
		params["EnvironmentName"] = environmentName

		params["BatchJobRoleArn"] = workflow.batchJobRoleArn
		params["ServiceMemory"] = strconv.Itoa(service.Memory)
		params["ServiceVCpu"] = strconv.Itoa(service.VCPU)
		params["ImageUrl"] = workflow.serviceImage

		/*
				params["EcsCluster"] = fmt.Sprintf("%s-EcsCluster", workflow.envStack.Name)
				params["LaunchType"] = fmt.Sprintf("%s-LaunchType", workflow.envStack.Name)
				params["ServiceSubnetIds"] = fmt.Sprintf("%s-InstanceSubnetIds", workflow.envStack.Name)
				params["ServiceSecurityGroup"] = fmt.Sprintf("%s-InstanceSecurityGroup", workflow.envStack.Name)
				params["ElbSecurityGroup"] = fmt.Sprintf("%s-InstanceSecurityGroup", workflow.lbStack.Name)
				params["ServiceDiscoveryId"] = fmt.Sprintf("%s-ServiceDiscoveryId", workflow.lbStack.Name)
				params["ServiceDiscoveryName"] = fmt.Sprintf("%s-ServiceDiscoveryName", workflow.lbStack.Name)
				common.NewMapElementIfNotEmpty(params, "ServiceDiscoveryTTL", service.DiscoveryTTL)

				params["ImageUrl"] = workflow.serviceImage

				cpu := common.CPUMemorySupport[0]
				if service.CPU != 0 {
					params["ServiceCpu"] = strconv.Itoa(service.CPU)
					cpu = matchRequestedCPU(service.CPU, cpu)
				}

				memory := cpu.Memory[0]
				if service.Memory != 0 {
					params["ServiceMemory"] = strconv.Itoa(service.Memory)
					memory = matchRequestedMemory(service.Memory, cpu, memory)
				}

				if workflow.isFargateProvider()() {
					params["TaskCpu"] = strconv.Itoa(cpu.CPU)
					params["TaskMemory"] = strconv.Itoa(memory)
				}

				if len(service.Links) > 0 {
					params["Links"] = strings.Join(service.Links, ",")
				}

				params["AssignPublicIp"] = strconv.FormatBool(service.AssignPublicIP)

				// force 'awsvpc' network mode for ecs-fargate
				if strings.EqualFold(string(workflow.envStack.Tags["provider"]), string(common.EnvProviderEcsFargate)) {
					params["TaskNetworkMode"] = common.NetworkModeAwsVpc
				} else if service.NetworkMode != "" {
					params["TaskNetworkMode"] = string(service.NetworkMode)
				}

				serviceRoleset, err := rolesetGetter.GetServiceRoleset(workflow.envStack.Tags["environment"], workflow.serviceName)
				if err != nil {
					return err
				}

				params["EcsServiceRoleArn"] = serviceRoleset["EcsServiceRoleArn"]
				params["EcsTaskRoleArn"] = serviceRoleset["EcsTaskRoleArn"]
				params["ApplicationAutoScalingRoleArn"] = serviceRoleset["ApplicationAutoScalingRoleArn"]
				params["ServiceName"] = workflow.serviceName

				params["MinimumHealthyPercent"], params["MaximumPercent"] = getMinMaxPercentForStrategy(service.DeploymentStrategy)


			params["VpcId"] = fmt.Sprintf("%s-VpcId", workflow.envStack.Name)

			svcStackName := common.CreateStackName(namespace, common.StackTypeBatch, workflow.serviceName, environmentName)
			svcStack := stackWaiter.AwaitFinalStatus(svcStackName)

			if svcStack != nil && svcStack.Status != "ROLLBACK_COMPLETE" {

			}

			params["ServiceName"] = workflow.serviceName
		*/

		return nil
	}
}

func (workflow *batchWorkflow) batchDeployer(namespace string, service *common.Batch, stackParams map[string]string, environmentName string, stackUpserter common.StackUpserter, stackWaiter common.StackWaiter) Executor {
	return func() error {
		log.Noticef("Deploying service '%s' to '%s' from '%s'", workflow.serviceName, environmentName, workflow.serviceImage)

		svcStackName := common.CreateStackName(namespace, common.StackTypeBatch, workflow.serviceName, environmentName)

		resolveBatchEnvironment(service, environmentName)

		tags := createTagMap(&ServiceTags{
			Service:     workflow.serviceName,
			Environment: environmentName,
			Type:        common.StackTypeService,
			Provider:    workflow.envStack.Outputs["provider"],
			Revision:    workflow.codeRevision,
			Repo:        workflow.repoName,
		})

		err := stackUpserter.UpsertStack(svcStackName, common.TemplateBatchJob, service, stackParams, tags, "", workflow.cloudFormationRoleArn)
		if err != nil {
			return err
		}
		log.Debugf("Waiting for stack '%s' to complete", svcStackName)
		stack := stackWaiter.AwaitFinalStatus(svcStackName)
		if stack == nil {
			return fmt.Errorf("Unable to create stack %s", svcStackName)
		}
		if strings.HasSuffix(stack.Status, "ROLLBACK_COMPLETE") || !strings.HasSuffix(stack.Status, "_COMPLETE") {
			return fmt.Errorf("Ended in failed status %s %s", stack.Status, stack.StatusReason)
		}
		workflow.batchJobDefinitionArn = stack.Outputs["BatchJobDefinitionArn"]

		return nil
	}
}

func resolveBatchEnvironment(service *common.Batch, environment string) {
	for key, value := range service.Environment {
		switch value.(type) {
		case map[interface{}]interface{}:
			found := false
			for env, v := range value.(map[interface{}]interface{}) {
				if env.(string) == environment {
					service.Environment[key] = v.(string)
					found = true
				}
			}
			if found != true {
				service.Environment[key] = ""
			}
		case string:
			// do nothing
		default:
			log.Warningf("Unable to resolve environment '%s': %v", key, value)
		}

	}
}
