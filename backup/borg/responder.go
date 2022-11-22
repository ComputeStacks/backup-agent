package borg

import (
	"encoding/json"
	"strings"

	"github.com/getsentry/sentry-go"
)

func readRepoResponse(response string) (RepositoryResponse, *LogMessage) {
	var logMsg LogMessage
	var repoResponse RepositoryResponse

	for _, item := range strings.Split(response, "\n{") {
		if item[0:1] != "{" {
			item = "{" + item
		}
		var marshalErr error
		if strings.Contains(item, "msgid") {
			marshalErr = json.Unmarshal([]byte(item), &logMsg)
		} else {
			marshalErr = json.Unmarshal([]byte(item), &repoResponse)
			if marshalErr != nil {
				logMsg.Message = item
			}
		}
		if marshalErr != nil {
			borgLogger().Debug("readRepoResponse", "item", item)
			borgLogger().Debug("readRepoResponse", "error", marshalErr.Error())
			sentry.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetExtra("logMsg", item)
			})
			sentry.CaptureException(marshalErr)
			return repoResponse, &logMsg
		}
		if logMsg != (LogMessage{}) {
			if logMsg.Type != "question_env_answer" && logMsg.Type != "question_prompt" {
				borgLogger().Debug("readRepoResponse", "LogMessage", logMsg.ToYaml())
				return repoResponse, &logMsg
			}
		}
	}
	return repoResponse, nil
}

func readRepoContentResponse(response string) (RepositoryContentResponse, *LogMessage) {
	var logMsg LogMessage
	var repoResponse RepositoryContentResponse

	for _, item := range strings.Split(response, "\n{") {
		if item[0:1] != "{" {
			item = "{" + item
		}
		var marshalErr error
		if strings.Contains(item, "msgid") {
			marshalErr = json.Unmarshal([]byte(item), &logMsg)
		} else {
			marshalErr = json.Unmarshal([]byte(item), &repoResponse)
			if marshalErr != nil {
				logMsg.Message = item
			}
		}
		if marshalErr != nil {
			borgLogger().Debug("readRepoResponse", "item", item)
			borgLogger().Debug("readRepoResponse", "error", marshalErr.Error())
			sentry.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetExtra("logMsg", item)
			})
			sentry.CaptureException(marshalErr)
			return repoResponse, &logMsg
		}
		if logMsg != (LogMessage{}) {
			if logMsg.Type != "question_env_answer" && logMsg.Type != "question_prompt" {
				borgLogger().Debug("readRepoResponse", "LogMessage", logMsg.ToYaml())
				return repoResponse, &logMsg
			}
		}
	}
	return repoResponse, nil
}

func readArchiveRestoreResponse(response string) *LogMessage {
	if response == "" {
		return nil
	}
	var logMsg LogMessage
	for _, item := range strings.Split(response, "\n{") {
		if item[0:1] != "{" {
			item = "{" + item
		}
		var marshalErr error
		marshalErr = json.Unmarshal([]byte(item), &logMsg)
		if marshalErr != nil {
			borgLogger().Debug("readRepoResponse", "item", item)
			borgLogger().Debug("readRepoResponse", "error", marshalErr.Error())
			sentry.ConfigureScope(func(scope *sentry.Scope) {
				scope.SetExtra("logMsg", item)
			})
			sentry.CaptureException(marshalErr)
			return &logMsg
		}
		if logMsg != (LogMessage{}) {
			if logMsg.Type != "question_env_answer" && logMsg.Type != "question_prompt" {
				borgLogger().Debug("readArchiveRestoreResponse", "LogMessage", logMsg.ToYaml())
				return &logMsg
			}
		}
	}
	return nil
}
