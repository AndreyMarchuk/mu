package workflows

import (
	"encoding/base64"
	"fmt"
	//"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/stelligent/mu/common"
)

type batchWorkflow struct {
	envStack *common.Stack
	//artifactProvider              common.ArtifactProvider
	serviceName           string
	serviceTag            string
	serviceImage          string
	registryAuth          string
	codeRevision          string
	repoName              string
	appName               string
	appRevisionBucket     string
	appRevisionKey        string
	cloudFormationRoleArn string
	batchJobDefinitionArn string
	batchJobRoleArn       string
	ecsEventsRoleArn      string
}

// Find a batch in config, by name and set the reference
func (workflow *batchWorkflow) batchLoader(ctx *common.Context, tag string, provider string) Executor {
	return func() error {
		err := workflow.batchInput(ctx, "")()
		if err != nil {
			return err
		}

		// Tag
		if tag != "" {
			workflow.serviceTag = tag
		} else if ctx.Config.Repo.Revision != "" {
			workflow.serviceTag = ctx.Config.Repo.Revision
		} else {
			workflow.serviceTag = "latest"
		}
		workflow.appRevisionKey = fmt.Sprintf("%s/%s.zip", workflow.serviceName, workflow.serviceTag)

		workflow.codeRevision = ctx.Config.Repo.Revision
		workflow.repoName = ctx.Config.Repo.Slug

		/*
			if provider == "" {
				dockerfile := ctx.Config.Batch.Dockerfile
				if dockerfile == "" {
					dockerfile = "Dockerfile"
				}

				dockerfilePath := fmt.Sprintf("%s/%s", ctx.Config.Basedir, dockerfile)
				log.Debugf("Determining repo provider by checking for existence of '%s'", dockerfilePath)

				if _, err := os.Stat(dockerfilePath); !os.IsNotExist(err) {
					log.Infof("Dockerfile found, assuming ECR pipeline")
					workflow.artifactProvider = common.ArtifactProviderEcr
				} else {
					log.Infof("No Dockerfile found, assuming CodeDeploy pipeline")
					workflow.artifactProvider = common.ArtifactProviderS3
				}
			} else {
				workflow.artifactProvider = common.ArtifactProvider(provider)
			}
		*/

		log.Debugf("Working with service:'%s' tag:'%s'", workflow.serviceName, workflow.serviceTag)
		return nil
	}
}

func (workflow *batchWorkflow) batchInput(ctx *common.Context, serviceName string) Executor {
	return func() error {
		// Repo Name
		if serviceName != "" {
			workflow.serviceName = serviceName
		} else if ctx.Config.Batch.Name != "" {
			workflow.serviceName = ctx.Config.Batch.Name
		} else if ctx.Config.Repo.Name != "" {
			workflow.serviceName = ctx.Config.Repo.Name
		} else {
			return errors.New("Service name must be provided")
		}
		return nil
	}
}

func (workflow *batchWorkflow) batchRepoUpserter(namespace string, service *common.Batch, stackUpserter common.StackUpserter, stackWaiter common.StackWaiter) Executor {
	return func() error {
		if service.ImageRepository != "" {
			log.Noticef("Using repo '%s' for batch '%s'", service.ImageRepository, workflow.serviceName)
			workflow.serviceImage = service.ImageRepository
			return nil
		}

		log.Noticef("Upsert repo for batch '%s'", workflow.serviceName)

		ecrStackName := common.CreateStackName(namespace, common.StackTypeRepo, workflow.serviceName)

		stackParams := make(map[string]string)
		stackParams["RepoName"] = fmt.Sprintf("%s-%s", namespace, workflow.serviceName)

		tags := createTagMap(&ServiceTags{
			Service:  workflow.serviceName,
			Type:     string(common.StackTypeRepo),
			Provider: "",
			Revision: workflow.codeRevision,
			Repo:     workflow.repoName,
		})

		err := stackUpserter.UpsertStack(ecrStackName, common.TemplateRepo, nil, stackParams, tags, "", "")
		if err != nil {
			return err
		}

		log.Debugf("Waiting for stack '%s' to complete", ecrStackName)
		stack := stackWaiter.AwaitFinalStatus(ecrStackName)
		if stack == nil {
			return fmt.Errorf("Unable to create stack %s", ecrStackName)
		}
		if strings.HasSuffix(stack.Status, "ROLLBACK_COMPLETE") || !strings.HasSuffix(stack.Status, "_COMPLETE") {
			return fmt.Errorf("Ended in failed status %s %s", stack.Status, stack.StatusReason)
		}
		workflow.serviceImage = fmt.Sprintf("%s:%s", stack.Outputs["RepoUrl"], workflow.serviceTag)
		return nil
	}
}

func (workflow *batchWorkflow) batchRegistryAuthenticator(authenticator common.RepositoryAuthenticator) Executor {
	return func() error {
		log.Debugf("Authenticating to registry '%s'", workflow.serviceImage)
		registryAuth, err := authenticator.AuthenticateRepository(workflow.serviceImage)
		if err != nil {
			return err
		}

		data, err := base64.StdEncoding.DecodeString(registryAuth)
		if err != nil {
			return err
		}

		authParts := strings.Split(string(data), ":")

		workflow.registryAuth = base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("{\"username\":\"%s\", \"password\":\"%s\"}", authParts[0], authParts[1])))
		return nil
	}
}
